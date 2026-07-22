import pytest
from pydantic import ValidationError

from oploom.config import ApprovalRule, PipelineConfig, load_pipeline


def test_invoice_config_loads():
    cfg = load_pipeline("invoice_intake")
    assert cfg.pipeline == "invoice_intake"
    assert [s.name for s in cfg.stages] == ["intake", "classify", "route", "act"]
    assert set(cfg.classification.labels) <= set(cfg.routing)


def test_missing_pipeline_raises():
    with pytest.raises(FileNotFoundError):
        load_pipeline("nonexistent")


def test_routing_must_cover_labels():
    cfg = load_pipeline("invoice_intake").model_dump()
    del cfg["routing"]["anomalous"]
    with pytest.raises(ValidationError, match="routing missing labels"):
        PipelineConfig.model_validate(cfg)


def test_amount_rule():
    rule = ApprovalRule(field="amount", greater_than=5000)
    assert rule.matches({"amount": 12000}) is True
    assert rule.matches({"amount": 3000}) is False
    assert rule.matches({}) is False


def test_approval_reason_first_match():
    cfg = load_pipeline("invoice_intake")
    assert cfg.approval_reason({"classification": "anomalous"}) == "classification == anomalous"
    assert cfg.approval_reason({"classification": "standard", "amount": 9000}) == "amount > 5000"
    assert cfg.approval_reason({"classification": "standard", "amount": 100}) is None


def test_rule_rejects_zero_or_two_conditions():
    with pytest.raises(ValidationError):
        ApprovalRule(field="amount")
    with pytest.raises(ValidationError):
        ApprovalRule(field="amount", equals=5, greater_than=5)