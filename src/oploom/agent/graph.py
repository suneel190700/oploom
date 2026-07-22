"""The agent core: a LangGraph state machine driven entirely by pipeline YAML.

Stages: intake -> classify -> route -> act.

Modes:
  OPLOOM_DEMO=1 (default)  deterministic classifier, no API key needed
  OPLOOM_DEMO=0            Claude via structured output, model from YAML
"""

from __future__ import annotations

import os
import uuid
from typing import Any, TypedDict

from langgraph.graph import END, StateGraph

from ..config import PipelineConfig


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
    """Deterministic stand-in so the full graph runs offline."""
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
        raise ValueError(f"model returned unknown label {result.label!r}")
    return result.label


def build_graph(cfg: PipelineConfig):
    async def intake(state: EventState) -> EventState:
        missing = [f for f in cfg.required_fields if f not in state["payload"]]
        if missing:
            return {
                **state,
                "status": "dead_lettered",
                "error": f"missing required fields: {missing}",
            }
        return {**state, "status": "running"}

    async def classify(state: EventState) -> EventState:
        label = (
            _demo_classify(cfg, state["payload"])
            if demo_mode()
            else _llm_classify(cfg, state["payload"])
        )
        return {**state, "classification": label}

    async def route(state: EventState) -> EventState:
        action = cfg.routing[state["classification"]]
        fields = {**state["payload"], "classification": state["classification"]}
        if cfg.approval_reason(fields):
            return {**state, "route": action, "status": "pending_approval"}
        return {**state, "route": action}

    async def act(state: EventState) -> EventState:
        # Real side effects arrive with n8n in module 5; recording the action
        # completes the state machine end to end.
        return {**state, "action": state["route"], "status": "completed"}

    def after_intake(state: EventState) -> str:
        return "halt" if state.get("status") == "dead_lettered" else "classify"

    def after_route(state: EventState) -> str:
        return "halt" if state.get("status") == "pending_approval" else "act"

    g = StateGraph(EventState)
    g.add_node("intake", intake)
    g.add_node("classify", classify)
    g.add_node("route", route)
    g.add_node("act", act)
    g.set_entry_point("intake")
    g.add_conditional_edges("intake", after_intake, {"classify": "classify", "halt": END})
    g.add_edge("classify", "route")
    g.add_conditional_edges("route", after_route, {"act": "act", "halt": END})
    g.add_edge("act", END)
    return g.compile()


async def process_event(cfg: PipelineConfig, payload: dict) -> EventState:
    graph = build_graph(cfg)
    initial: EventState = {
        "event_id": str(uuid.uuid4()),
        "pipeline": cfg.pipeline,
        "payload": payload,
        "status": "received",
    }
    return await graph.ainvoke(initial)