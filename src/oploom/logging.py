"""Structured logging: one JSON line per occurrence, keyed by event_id/stage.

The log vocabulary deliberately matches the stage_runs table — module 7's
completion metrics can be cross-checked against either."""

import logging
import sys

import structlog


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(stream=sys.stdout, level=level, format="%(message)s")
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.add_log_level,
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)
        ),
        cache_logger_on_first_use=True,
    )


def get_logger(**initial_context):
    return structlog.get_logger().bind(**initial_context)