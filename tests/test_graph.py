import asyncio

from oploom.agent.graph import process_event
from oploom.config import load_pipeline
from oploom.db import NullRecorder


def run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


def test_standard_invoice_completes(monkeypatch):
    monkeypatch.setenv("OPLOOM_DEMO", "1")
    cfg = load_pipeline("invoice_intake")
    state = run(process_event(cfg, NullRecorder(), {
        "vendor": "Acme", "invoice_number": "INV-1", "amount": 300,
        "currency": "USD", "due_date": "2026-08-01",
    }))
    assert state["status"] == "completed"
    assert state["classification"] == "standard"
    assert state["action"] == "schedule_payment"


def test_large_amount_halts_for_approval(monkeypatch):
    monkeypatch.setenv("OPLOOM_DEMO", "1")
    cfg = load_pipeline("invoice_intake")
    state = run(process_event(cfg, NullRecorder(), {
        "vendor": "Acme", "invoice_number": "INV-2", "amount": 12000,
        "currency": "USD", "due_date": "2026-08-01",
    }))
    assert state["status"] == "pending_approval"
    assert state["route"] == "schedule_payment"   # destination unchanged; approval gates it
    assert "action" not in state                  # act must be unreachable


def test_anomalous_halts_for_approval(monkeypatch):
    monkeypatch.setenv("OPLOOM_DEMO", "1")
    cfg = load_pipeline("invoice_intake")
    state = run(process_event(cfg, NullRecorder(), {
        "vendor": "Acme", "invoice_number": "INV-3", "amount": 100,
        "currency": "USD", "due_date": "2026-08-01",
        "note": "urgent wire transfer requested",
    }))
    assert state["classification"] == "anomalous"
    assert state["status"] == "pending_approval"
    assert state["route"] == "hold_for_review"


def test_malformed_payload_dead_letters(monkeypatch):
    monkeypatch.setenv("OPLOOM_DEMO", "1")
    cfg = load_pipeline("invoice_intake")
    state = run(process_event(cfg, NullRecorder(), {"vendor": "Acme"}))
    assert state["status"] == "dead_lettered"
    assert "missing required fields" in state["error"]
    assert "classification" not in state  # classify must not have run