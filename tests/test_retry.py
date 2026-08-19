import asyncio

import pytest

import oploom.agent.graph as graph_mod
from oploom.agent.graph import PermanentError, process_event
from oploom.config import load_pipeline
from oploom.db import NullRecorder


def run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


class SpyRecorder(NullRecorder):
    """NullRecorder that also captures what was recorded."""

    def __init__(self):
        self.stage_attempts: list[tuple[str, int]] = []
        self.dlq: list[dict] = []
        self.event_updates: list[dict] = []

    async def stage_started(self, event_id, stage, attempt):
        self.stage_attempts.append((stage, attempt))
        return await super().stage_started(event_id, stage, attempt)

    async def dead_letter(self, event_id, stage, attempts, error, payload):
        self.dlq.append({
            "stage": stage, "attempts": attempts,
            "error": error, "payload": payload,
        })

    async def update_event(self, event_id, **fields):
        self.event_updates.append(fields)


VALID = {
    "vendor": "Acme", "invoice_number": "INV-1", "amount": 300,
    "currency": "USD", "due_date": "2026-08-01",
}


@pytest.fixture
def cfg():
    c = load_pipeline("invoice_intake")
    for s in c.stages:
        s.retry.backoff_seconds = 0   # keep tests instant
    return c


def test_transient_failure_retries_then_succeeds(cfg, monkeypatch):
    monkeypatch.setenv("OPLOOM_DEMO", "1")
    calls = {"n": 0}
    real = graph_mod._demo_classify

    def flaky(cfg_, payload):
        calls["n"] += 1
        if calls["n"] < 3:
            raise TimeoutError("simulated transient failure")
        return real(cfg_, payload)

    monkeypatch.setattr(graph_mod, "_demo_classify", flaky)
    spy = SpyRecorder()
    state = run(process_event(cfg, spy, VALID))

    assert state["status"] == "completed"
    classify_attempts = [a for s, a in spy.stage_attempts if s == "classify"]
    assert classify_attempts == [1, 2, 3]
    assert spy.dlq == []


def test_exhaustion_dead_letters_with_payload(cfg, monkeypatch):
    monkeypatch.setenv("OPLOOM_DEMO", "1")

    def always_fails(cfg_, payload):
        raise TimeoutError("permanently flaky")

    monkeypatch.setattr(graph_mod, "_demo_classify", always_fails)
    spy = SpyRecorder()
    state = run(process_event(cfg, spy, VALID))

    assert state["status"] == "dead_lettered"
    classify_attempts = [a for s, a in spy.stage_attempts if s == "classify"]
    assert classify_attempts == [1, 2, 3]        # max_attempts from YAML
    assert len(spy.dlq) == 1
    assert spy.dlq[0]["stage"] == "classify"
    assert spy.dlq[0]["attempts"] == 3
    assert spy.dlq[0]["payload"] == VALID        # snapshot enables replay


def test_permanent_failure_skips_retries(cfg, monkeypatch):
    monkeypatch.setenv("OPLOOM_DEMO", "1")
    spy = SpyRecorder()
    state = run(process_event(cfg, spy, {"vendor": "Acme"}))

    assert state["status"] == "dead_lettered"
    intake_attempts = [a for s, a in spy.stage_attempts if s == "intake"]
    assert intake_attempts == [1]                # no retry on PermanentError
    assert len(spy.dlq) == 1
    assert "missing required fields" in spy.dlq[0]["error"]