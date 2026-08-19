from fastapi.testclient import TestClient

from oploom.api.main import app


def make_client(monkeypatch):
    # Force NullRecorder + demo mode: tests must not touch Supabase or the API.
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("OPLOOM_DEMO", "1")
    return TestClient(app)


def test_health(monkeypatch):
    with make_client(monkeypatch) as client:
        r = client.get("/health")
        assert r.status_code == 200
        body = r.json()
        assert body["ok"] is True
        assert "invoice_intake" in body["pipelines"]


def test_submit_event_completes(monkeypatch):
    with make_client(monkeypatch) as client:
        r = client.post("/events/invoice_intake", json={
            "vendor": "Acme", "invoice_number": "INV-1", "amount": 300,
            "currency": "USD", "due_date": "2026-08-01",
        })
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "completed"
        assert body["route"] == "schedule_payment"


def test_submit_event_gated(monkeypatch):
    with make_client(monkeypatch) as client:
        r = client.post("/events/invoice_intake", json={
            "vendor": "Acme", "invoice_number": "INV-2", "amount": 12000,
            "currency": "USD", "due_date": "2026-08-01",
        })
        assert r.json()["status"] == "pending_approval"


def test_unknown_pipeline_404(monkeypatch):
    with make_client(monkeypatch) as client:
        r = client.post("/events/nope", json={})
        assert r.status_code == 404