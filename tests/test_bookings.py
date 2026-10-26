from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import httpx
from fastapi.testclient import TestClient

from app.main import create_app


def client_for(tmp_path, capacity=2, code=200, handler=None):
    def event_response(request):
        return httpx.Response(code, json={"id": "event-1", "capacity": capacity,
                              "starts_at": (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()})
    return TestClient(create_app(f"sqlite:///{tmp_path / 'bookings.db'}", event_url="http://events",
                                event_transport=httpx.MockTransport(handler or event_response)))


def book(client, key, student="student1"):
    return client.post("/bookings", json={"event_id": "event-1", "student_id": student},
                       headers={"Idempotency-Key": key})


def test_book_replay_cancel_and_capacity(tmp_path):
    with client_for(tmp_path, capacity=1) as client:
        first = book(client, "one")
        assert first.status_code == 201
        assert book(client, "one").json() == first.json()
        assert book(client, "one", "different").status_code == 409
        assert book(client, "two").status_code == 409
        bid = first.json()["id"]
        assert client.get(f"/bookings/{bid}").status_code == 200
        assert client.delete(f"/bookings/{bid}").json()["status"] == "CANCELLED"
        assert client.delete(f"/bookings/{bid}").status_code == 200
        assert book(client, "two").status_code == 201
        assert book(client, "three").status_code == 409
        # Original idempotency key still refers to its cancelled booking.
        assert book(client, "one").json()["status"] == "CANCELLED"
        assert client.get("/bookings?student_id=absent").json() == []
        assert client.get("/bookings/missing").status_code == 404
        assert client.delete("/bookings/missing").status_code == 404


def test_upstream_errors(tmp_path):
    with client_for(tmp_path, code=404) as client:
        assert book(client, "a").status_code == 404
    with client_for(tmp_path, code=500) as client:
        assert book(client, "a").status_code == 503
        assert client.get("/health/live").status_code == 200
    def timeout(request):
        raise httpx.ReadTimeout("test", request=request)
    with client_for(tmp_path, handler=timeout) as client:
        assert book(client, "a").status_code == 503
    with client_for(tmp_path, handler=lambda request: httpx.Response(200, json={"capacity": "bad"})) as client:
        assert book(client, "a").status_code == 503


def test_concurrent_capacity(tmp_path):
    with client_for(tmp_path, capacity=3) as client:
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda i: book(client, f"key-{i}", f"student{i}"), range(12)))
        assert sum(r.status_code == 201 for r in results) == 3
        assert sum(r.status_code == 409 for r in results) == 9


def test_concurrent_retries_and_cancellation(tmp_path):
    with client_for(tmp_path, capacity=2) as client:
        with ThreadPoolExecutor(max_workers=6) as pool:
            results = list(pool.map(lambda _: book(client, "same"), range(8)))
        assert all(r.status_code == 201 for r in results)
        assert len({r.json()["id"] for r in results}) == 1
        bid = results[0].json()["id"]
        with ThreadPoolExecutor(max_workers=6) as pool:
            results = list(pool.map(lambda _: client.delete(f"/bookings/{bid}"), range(8)))
        assert all(r.json()["status"] == "CANCELLED" for r in results)
        assert book(client, "new1").status_code == 201
        assert book(client, "new2").status_code == 201
        assert book(client, "new3").status_code == 409


def test_input_validation(tmp_path):
    with client_for(tmp_path) as client:
        assert client.post("/bookings", json={"event_id": "event-1", "student_id": "x"}).status_code == 422
        assert book(client, "a", " ").status_code == 422
        assert client.get("/health/ready").status_code == 200
