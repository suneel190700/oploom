"""YAML pipeline config loading and validation.

Every behavioral knob of a pipeline lives in its YAML file under configs/.
Nothing pipeline-specific is hardcoded in Python — config errors fail at
load time, not at event time.
"""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

CONFIG_DIR = Path(__file__).resolve().parents[2] / "configs"


class RetryPolicy(BaseModel):
    max_attempts: int = Field(ge=1, le=10)
    backoff_seconds: float = Field(ge=0)


class StageConfig(BaseModel):
    name: str
    retry: RetryPolicy


class ClassificationConfig(BaseModel):
    model: str
    labels: list[str] = Field(min_length=2)
    instructions: str


class ApprovalRule(BaseModel):
    field: str
    equals: str | int | float | None = None
    greater_than: float | None = None

    @model_validator(mode="after")
    def _exactly_one_condition(self):
        set_count = (self.equals is not None) + (self.greater_than is not None)
        if set_count != 1:
            raise ValueError("approval rule takes exactly one of `equals`/`greater_than`")
        return self

    def matches(self, event_fields: dict) -> bool:
        value = event_fields.get(self.field)
        if value is None:
            return False
        if self.equals is not None:
            return value == self.equals
        try:
            return float(value) > self.greater_than
        except (TypeError, ValueError):
            return False


class ApprovalConfig(BaseModel):
    required_when: list[ApprovalRule] = []


class PipelineConfig(BaseModel):
    pipeline: str
    description: str
    required_fields: list[str]
    stages: list[StageConfig]
    classification: ClassificationConfig
    routing: dict[str, str]
    approval: ApprovalConfig = ApprovalConfig()

    @field_validator("routing")
    @classmethod
    def _routing_covers_labels(cls, v, info):
        cls_cfg = info.data.get("classification")
        if cls_cfg is not None:
            missing = set(cls_cfg.labels) - set(v)
            if missing:
                raise ValueError(f"routing missing labels: {sorted(missing)}")
        return v

    def stage(self, name: str) -> StageConfig:
        for s in self.stages:
            if s.name == name:
                return s
        raise KeyError(f"stage {name!r} not in pipeline {self.pipeline!r}")

    def approval_reason(self, event_fields: dict) -> str | None:
        """Description of the first matching approval rule, else None."""
        for rule in self.approval.required_when:
            if rule.matches(event_fields):
                if rule.equals is not None:
                    return f"{rule.field} == {rule.equals}"
                return f"{rule.field} > {rule.greater_than:g}"
        return None


def load_pipeline(name: str, config_dir: Path = CONFIG_DIR) -> PipelineConfig:
    path = config_dir / f"{name}.yaml"
    if not path.exists():
        raise FileNotFoundError(f"no pipeline config: {path}")
    with open(path) as f:
        raw = yaml.safe_load(f)
    cfg = PipelineConfig.model_validate(raw)
    if cfg.pipeline != name:
        raise ValueError(f"{path.name}: `pipeline: {cfg.pipeline}` != filename")
    return cfg