"""Bookings with transactional capacity and idempotency in one owned database."""
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from uuid import uuid4

import httpx
from fastapi import FastAPI, Header, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import CheckConstraint, Integer, String, create_engine, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker


class Base(DeclarativeBase):
    pass


class Inventory(Base):
    __tablename__ = "inventory"
    __table_args__ = (CheckConstraint("remaining >= 0 AND remaining <= capacity"),)
    event_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    capacity: Mapped[int] = mapped_column(Integer)
    remaining: Mapped[int] = mapped_column(Integer)


class Booking(Base):
    __tablename__ = "bookings"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    event_id: Mapped[str] = mapped_column(String(36), index=True)
    student_id: Mapped[str] = mapped_column(String(80))
    idempotency_key: Mapped[str] = mapped_column(String(100), unique=True)
    status: Mapped[str] = mapped_column(String(16), default="CONFIRMED")


class BookingInput(BaseModel):
    event_id: str = Field(min_length=1, max_length=36)
    student_id: str = Field(min_length=1, max_length=80, pattern=r"^[A-Za-z0-9_-]+$")


class BookingOutput(BaseModel):
    model_config = {"from_attributes": True}
    id: str
    event_id: str
    student_id: str
    status: str


def create_app(database_url: str | None = None, event_url: str | None = None, event_transport=None):
    url = database_url or os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is required")
    engine = create_engine(url, pool_pre_ping=True)
    sessions = sessionmaker(engine, expire_on_commit=False)
    base_url = event_url or os.environ.get("EVENT_SERVICE_URL", "http://event-service:8000")

    @asynccontextmanager
    async def lifespan(app):
        Base.metadata.create_all(engine)
        yield
        engine.dispose()

    app = FastAPI(title="Campus Booking Service", version="1.0.0", lifespan=lifespan)

    def replay(existing, body):
        if existing.event_id != body.event_id or existing.student_id != body.student_id:
            raise HTTPException(409, "Idempotency key already used for another request")
        return existing

    @app.get("/health/live")
    def live():
        return {"status": "ok", "service": "booking-service"}

    @app.get("/health/ready")
    def ready():
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        except Exception:
            raise HTTPException(503, "Database unavailable") from None
        return {"status": "ready"}

    @app.post("/bookings", response_model=BookingOutput, status_code=201)
    def book(body: BookingInput, idempotency_key: str = Header(min_length=1, max_length=100)):
        with sessions() as session:
            existing = session.scalar(select(Booking).where(Booking.idempotency_key == idempotency_key))
            if existing:
                return replay(existing, body)

        # Remote reads happen outside the database transaction; bounded timeout,
        # no implicit retries of remote writes and no distributed seat counter.
        try:
            with httpx.Client(timeout=3.0, transport=event_transport) as client:
                response = client.get(f"{base_url}/events/{body.event_id}")
            if response.status_code == 404:
                raise HTTPException(404, "Event not found")
            response.raise_for_status()
            event = response.json()
            capacity = event["capacity"]
            starts_at = datetime.fromisoformat(event["starts_at"].replace("Z", "+00:00"))
            if type(capacity) is not int or capacity <= 0 or starts_at.tzinfo is None:
                raise ValueError("Invalid event contract")
        except (httpx.HTTPError, ValueError, KeyError, TypeError, AttributeError):
            raise HTTPException(503, "Event service unavailable or invalid response") from None
        if starts_at <= datetime.now(timezone.utc):
            raise HTTPException(409, "Event has already started")

        try:
            with sessions.begin() as session:
                insert = sqlite_insert if engine.dialect.name == "sqlite" else pg_insert
                session.execute(insert(Inventory).values(event_id=body.event_id, capacity=capacity, remaining=capacity)
                                .on_conflict_do_nothing(index_elements=["event_id"]))
                # The database serializes competing conditional updates across replicas.
                taken = session.execute(update(Inventory).where(Inventory.event_id == body.event_id,
                                        Inventory.remaining > 0).values(remaining=Inventory.remaining - 1))
                if taken.rowcount != 1:
                    # A retry can race with the original request taking the last seat.
                    existing = session.scalar(select(Booking).where(Booking.idempotency_key == idempotency_key))
                    if existing:
                        return replay(existing, body)
                    raise HTTPException(409, "Event is full")
                booking = Booking(id=str(uuid4()), idempotency_key=idempotency_key, **body.model_dump())
                session.add(booking)
                session.flush()
            return booking
        except IntegrityError:
            # Unique-key collision rolls back the entire decrement before replay.
            with sessions() as session:
                existing = session.scalar(select(Booking).where(Booking.idempotency_key == idempotency_key))
                if existing:
                    return replay(existing, body)
            raise HTTPException(409, "Booking conflict") from None

    @app.get("/bookings", response_model=list[BookingOutput])
    def list_bookings(student_id: str | None = None, limit: int = Query(50, ge=1, le=100), offset: int = Query(0, ge=0)):
        with sessions() as session:
            query = select(Booking).order_by(Booking.id)
            if student_id:
                query = query.where(Booking.student_id == student_id)
            return session.scalars(query.offset(offset).limit(limit)).all()

    @app.get("/bookings/{booking_id}", response_model=BookingOutput)
    def get_booking(booking_id: str):
        with sessions() as session:
            booking = session.get(Booking, booking_id)
            if booking is None:
                raise HTTPException(404, "Booking not found")
            return booking

    @app.delete("/bookings/{booking_id}", response_model=BookingOutput)
    def cancel(booking_id: str):
        with sessions.begin() as session:
            changed = session.execute(update(Booking).where(Booking.id == booking_id, Booking.status == "CONFIRMED")
                                      .values(status="CANCELLED").returning(Booking.event_id)).scalar_one_or_none()
            if changed:
                session.execute(update(Inventory).where(Inventory.event_id == changed).values(remaining=Inventory.remaining + 1))
            booking = session.get(Booking, booking_id)
            if booking is None:
                raise HTTPException(404, "Booking not found")
        return booking

    return app
