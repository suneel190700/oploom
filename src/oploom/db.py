"""Persistence layer. The graph is the only writer; Retool and oploom-mcp read.

DATABASE_URL set   -> PostgresRecorder (Supabase)
DATABASE_URL unset -> NullRecorder (structured logs only, keeps tests offline)
"""

from __future__ import annotations

import json
import os
import uuid
from typing import Any, Protocol

import asyncpg

from .logging import get_logger

log = get_logger(component="db")


class Recorder(Protocol):
    async def create_event(self, pipeline: str, payload: dict) -> str: ...
    async def update_event(self, event_id: str, **fields: Any) -> None: ...
    async def stage_started(self, event_id: str, stage: str, attempt: int) -> int: ...
    async def stage_finished(
        self, event_id: str, run_id: int, status: str, detail: dict | None = None
    ) -> None: ...
    async def create_approval(self, event_id: str, reason: str) -> None: ...
    async def dead_letter(
        self, event_id: str, stage: str, attempts: int, error: str, payload: dict
    ) -> None: ...


class NullRecorder:
    async def create_event(self, pipeline: str, payload: dict) -> str:
        event_id = str(uuid.uuid4())
        log.info("event.created", event_id=event_id, pipeline=pipeline)
        return event_id

    async def update_event(self, event_id: str, **fields: Any) -> None:
        log.info("event.updated", event_id=event_id, **fields)

    async def stage_started(self, event_id: str, stage: str, attempt: int) -> int:
        log.info("stage.started", event_id=event_id, stage=stage, attempt=attempt)
        return 0

    async def stage_finished(self, event_id, run_id, status, detail=None) -> None:
        log.info("stage.finished", event_id=event_id, status=status)

    async def create_approval(self, event_id: str, reason: str) -> None:
        log.info("approval.created", event_id=event_id, reason=reason)

    async def dead_letter(self, event_id, stage, attempts, error, payload) -> None:
        log.info("dlq.inserted", event_id=event_id, stage=stage, attempts=attempts)


class PostgresRecorder:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    @classmethod
    async def connect(cls, dsn: str) -> "PostgresRecorder":
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=5)
        return cls(pool)

    async def close(self) -> None:
        await self.pool.close()

    async def create_event(self, pipeline: str, payload: dict) -> str:
        row = await self.pool.fetchrow(
            "insert into events (pipeline, payload) values ($1, $2) returning id",
            pipeline,
            json.dumps(payload),
        )
        return str(row["id"])

    async def update_event(self, event_id: str, **fields: Any) -> None:
        cols, vals = zip(*fields.items())
        sets = ", ".join(f"{c} = ${i + 2}" for i, c in enumerate(cols))
        await self.pool.execute(
            f"update events set {sets}, updated_at = now() where id = $1",
            uuid.UUID(event_id),
            *vals,
        )

    async def stage_started(self, event_id: str, stage: str, attempt: int) -> int:
        row = await self.pool.fetchrow(
            "insert into stage_runs (event_id, stage, attempt, status) "
            "values ($1, $2, $3, 'started') returning id",
            uuid.UUID(event_id),
            stage,
            attempt,
        )
        return row["id"]

    async def stage_finished(self, event_id, run_id, status, detail=None) -> None:
        await self.pool.execute(
            "update stage_runs set status = $2, detail = $3, finished_at = now() "
            "where id = $1",
            run_id,
            status,
            json.dumps(detail) if detail is not None else None,
        )

    async def create_approval(self, event_id: str, reason: str) -> None:
        await self.pool.execute(
            "insert into approvals (event_id, reason) values ($1, $2)",
            uuid.UUID(event_id),
            reason,
        )

    async def dead_letter(self, event_id, stage, attempts, error, payload) -> None:
        await self.pool.execute(
            "insert into dlq (event_id, stage, attempts, error, payload) "
            "values ($1, $2, $3, $4, $5)",
            uuid.UUID(event_id),
            stage,
            attempts,
            error,
            json.dumps(payload),
        )


async def make_recorder() -> Recorder:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        log.info("recorder.mode", mode="null")
        return NullRecorder()
    log.info("recorder.mode", mode="postgres")
    return await PostgresRecorder.connect(dsn)