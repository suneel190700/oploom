"""The agent core: a LangGraph state machine driven entirely by pipeline YAML.

Stages: intake -> classify -> route -> act. Every stage attempt is a
stage_runs row; failures retry per the YAML policy, then dead-letter.

Modes:
  OPLOOM_DEMO=1 (default)  deterministic classifier, no API key needed
  OPLOOM_DEMO=0            Claude via structured output, model from YAML
"""

from __future__ import annotations

import asyncio
import os
from typing import Any, TypedDict

from langgraph.graph import END, StateGraph

from ..config import PipelineConfig
from ..db import Recorder
from ..logging import get_logger


class PermanentError(Exception):
    """Failure that retrying cannot fix (bad payload, unknown label...)."""


class EventState(TypedDict, total=False):
    event_id: str
    pipeline: str
    payload: dict[str, Any]
    classification: str
    route: str
    action: str
    status: str
    error: str


def demo_mode() -> bool:
    return os.environ.get("OPLOOM_DEMO", "1") != "0"


def _demo_classify(cfg: PipelineConfig, payload: dict) -> str:
    text = " ".join(str(v) for v in payload.values()).lower()
    keyword_map = {
        "invoice_intake": [
            ("duplicate", "duplicate_suspect"),
            ("urgent wire", "anomalous"),
        ],
    }
    for needle, label in keyword_map.get(cfg.pipeline, []):
        if needle in text:
            return label
    for fallback in ("standard", "cold", "p3_question"):
        if fallback in cfg.classification.labels:
            return fallback
    return cfg.classification.labels[0]


def _llm_classify(cfg: PipelineConfig, payload: dict) -> str:
    from langchain_anthropic import ChatAnthropic
    from pydantic import BaseModel, Field

    labels = cfg.classification.labels

    class Classification(BaseModel):
        label: str = Field(description=f"one of: {labels}")

    llm = ChatAnthropic(model=cfg.classification.model, max_tokens=256)
    result = llm.with_structured_output(Classification).invoke(
        f"{cfg.classification.instructions}\n\nEvent fields:\n{payload}"
    )
    if result.label not in labels:
        raise PermanentError(f"model returned unknown label {result.label!r}")
    return result.label


def build_graph(cfg: PipelineConfig, recorder: Recorder):
    log = get_logger(pipeline=cfg.pipeline)

    async def _dead_letter(
        state: EventState, stage: str, attempts: int, error: str
    ) -> EventState:
        await recorder.dead_letter(
            state["event_id"], stage, attempts, error, state["payload"]
        )
        await recorder.update_event(state["event_id"], status="dead_lettered")
        log.error("event.dead_lettered", stage=stage, attempts=attempts, error=error)
        return {**state, "status": "dead_lettered", "error": error}

    async def _recorded(state: EventState, stage: str, fn) -> EventState:
        """Run one stage under its YAML retry policy. Each attempt is a
        stage_runs row; exhaustion (or a PermanentError) dead-letters."""
        policy = cfg.stage(stage).retry
        last_error = ""
        for attempt in range(1, policy.max_attempts + 1):
            run_id = await recorder.stage_started(state["event_id"], stage, attempt)
            try:
                update = await fn(state)
            except PermanentError as exc:
                await recorder.stage_finished(
                    state["event_id"], run_id, "failed",
                    {"error": str(exc), "permanent": True},
                )
                log.warning("stage.permanent_failure", stage=stage, error=str(exc))
                return await _dead_letter(state, stage, attempt, str(exc))
            except Exception as exc:
                last_error = str(exc)
                await recorder.stage_finished(
                    state["event_id"], run_id, "failed", {"error": last_error}
                )
                log.warning(
                    "stage.retryable_failure",
                    stage=stage, attempt=attempt,
                    max_attempts=policy.max_attempts, error=last_error,
                )
                if attempt < policy.max_attempts:
                    await asyncio.sleep(policy.backoff_seconds * attempt)
                continue
            await recorder.stage_finished(
                state["event_id"], run_id, "succeeded", update
            )
            return {**state, **update}
        return await _dead_letter(state, stage, policy.max_attempts, last_error)

    async def intake(state: EventState) -> EventState:
        async def fn(s):
            missing = [f for f in cfg.required_fields if f not in s["payload"]]
            if missing:
                raise PermanentError(f"missing required fields: {missing}")
            await recorder.update_event(s["event_id"], status="running")
            return {"status": "running"}
        return await _recorded(state, "intake", fn)

    async def classify(state: EventState) -> EventState:
        async def fn(s):
            label = (
                _demo_classify(cfg, s["payload"])
                if demo_mode()
                else _llm_classify(cfg, s["payload"])
            )
            await recorder.update_event(s["event_id"], classification=label)
            return {"classification": label}
        return await _recorded(state, "classify", fn)

    async def route(state: EventState) -> EventState:
        async def fn(s):
            action = cfg.routing[s["classification"]]
            fields = {**s["payload"], "classification": s["classification"]}
            reason = cfg.approval_reason(fields)
            if reason:
                await recorder.create_approval(s["event_id"], reason)
                await recorder.update_event(
                    s["event_id"], route=action, status="pending_approval"
                )
                return {"route": action, "status": "pending_approval"}
            await recorder.update_event(s["event_id"], route=action)
            return {"route": action}
        return await _recorded(state, "route", fn)

    async def act(state: EventState) -> EventState:
        async def fn(s):
            await recorder.update_event(
                s["event_id"], action=s["route"], status="completed"
            )
            return {"action": s["route"], "status": "completed"}
        return await _recorded(state, "act", fn)

    def after_intake(state: EventState) -> str:
        return "halt" if state.get("status") == "dead_lettered" else "classify"

    def after_classify(state: EventState) -> str:
        return "halt" if state.get("status") == "dead_lettered" else "route"

    def after_route(state: EventState) -> str:
        if state.get("status") in ("dead_lettered", "pending_approval"):
            return "halt"
        return "act"

    g = StateGraph(EventState)
    g.add_node("intake", intake)
    g.add_node("classify", classify)
    g.add_node("route", route)
    g.add_node("act", act)
    g.set_entry_point("intake")
    g.add_conditional_edges("intake", after_intake, {"classify": "classify", "halt": END})
    g.add_conditional_edges("classify", after_classify, {"route": "route", "halt": END})
    g.add_conditional_edges("route", after_route, {"act": "act", "halt": END})
    g.add_edge("act", END)
    return g.compile()


async def process_event(
    cfg: PipelineConfig, recorder: Recorder, payload: dict
) -> EventState:
    event_id = await recorder.create_event(cfg.pipeline, payload)
    graph = build_graph(cfg, recorder)
    initial: EventState = {
        "event_id": event_id,
        "pipeline": cfg.pipeline,
        "payload": payload,
        "status": "received",
    }
    return await graph.ainvoke(initial)