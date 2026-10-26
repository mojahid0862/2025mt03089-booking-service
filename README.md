# booking-service
Coursework by MOJAHID UL HAQUE, 2025mt03089. One independently runnable service.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
python -m pytest
export DATABASE_URL='sqlite:///./local.db'
export EVENT_SERVICE_URL=http://127.0.0.1:8001
uvicorn app.main:create_app --factory --host 127.0.0.1 --port 8002
```

On PowerShell use `.venv\Scripts\Activate.ps1` and `$env:DATABASE_URL="sqlite:///./local.db"`.
SQLite is for isolated tests/development. Deployment uses this service's own PostgreSQL database.
API docs: `/docs`; schema: `/openapi.json`; health: `/health/live` and `/health/ready`.
Build: `docker build -t booking-service:1.0.0 .`.
Runtime has no imports from the other repository. REST is the only cross-service interface.
Start the service before its tests only for live API testing; pytest starts an isolated in-process app.
This classroom API has no authentication. Bind to loopback; do not expose publicly.
