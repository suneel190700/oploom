"""Agent API. n8n posts events here; the graph runs and the final state
comes back in the response so n8n workflows can branch on it.

Run:  uvicorn oploom.api.main:app --reload
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException

from ..agent.graph import demo_mode, process_event
from ..config import CONFIG_DIR, load_pipeline
from ..db import make_recorder
from ..logging import get_logger, setup_logging

log = get_logger(component="api")


def load_all(config_dir: Path = CONFIG_DIR):
    return {p.stem: load_pipeline(p.stem, config_dir)
            for p in sorted(config_dir.glob("*.yaml"))}


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    app.state.pipelines = load_all()
    app.state.recorder = await make_recorder()
    log.info("startup", pipelines=sorted(app.state.pipelines), demo_mode=demo_mode())
    yield
    close = getattr(app.state.recorder, "close", None)
    if close:
        await close()


app = FastAPI(title="Oploom Agent API", lifespan=lifespan)


@app.get("/health")
async def health():
    return {
        "ok": True,
        "pipelines": sorted(app.state.pipelines),
        "demo_mode": demo_mode(),
    }


@app.post("/events/{pipeline}")
async def submit_event(pipeline: str, payload: dict):
    cfg = app.state.pipelines.get(pipeline)
    if cfg is None:
        raise HTTPException(404, f"unknown pipeline: {pipeline}")
    state = await process_event(cfg, app.state.recorder, payload)
    return {
        "event_id": state["event_id"],
        "status": state.get("status"),
        "classification": state.get("classification"),
        "route": state.get("route"),
        "error": state.get("error"),
    }