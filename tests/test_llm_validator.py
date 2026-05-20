"""A5 — LLM validator tests.

Covers parse_reject_tags_csv + should_reject_llm_validation (pure).
The HTTP path (llm_validate_signal) is covered indirectly by integration
tests run against the real ai-edge:8030 service during soak.
"""
from __future__ import annotations

import os

os.environ.setdefault("POSTGRES_DSN", "postgresql://test:test@localhost:5432/test")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")

from side_stream.llm_validator import (
    DEFAULT_REJECT_TAGS,
    parse_reject_tags_csv,
    should_reject_llm_validation,
)

# ─── parse_reject_tags_csv ─────────────────────────────────────────────


def test_parse_reject_tags_csv_uses_defaults_when_empty():
    """Empty CSV falls back to DEFAULT_REJECT_TAGS — defensive default
    so an unset env doesn't silently drop the safety net."""
    assert parse_reject_tags_csv("") == set(DEFAULT_REJECT_TAGS)
    assert parse_reject_tags_csv(None) == set(DEFAULT_REJECT_TAGS)  # type: ignore[arg-type]


def test_parse_reject_tags_csv_strips_whitespace():
    assert parse_reject_tags_csv("  foo, bar ,baz  ") == {"foo", "bar", "baz"}


def test_parse_reject_tags_csv_drops_empty_entries():
    assert parse_reject_tags_csv("a,,b,") == {"a", "b"}


# ─── should_reject_llm_validation ──────────────────────────────────────


def test_no_response_fails_open():
    """LLM unreachable → don't reject (fail-OPEN). The brand promise of
    quality lives in A4 (Sharpe snapshot), not A5 (LLM filter)."""
    reject, reason = should_reject_llm_validation(
        None, reject_tags=set(DEFAULT_REJECT_TAGS),
    )
    assert reject is False
    assert reason == "no_response_unforced"


def test_looks_real_false_rejects():
    """LLM says 'this isn't a real signal' → drop."""
    resp = {"looks_real": False, "risk_tag": "ok"}
    reject, reason = should_reject_llm_validation(
        resp, reject_tags=set(DEFAULT_REJECT_TAGS),
    )
    assert reject is True
    assert reason == "looks_real_false"


def test_missing_looks_real_defaults_to_true():
    """Absence of looks_real key is NOT a veto — the LLM might omit
    fields it considers nominal."""
    resp = {"risk_tag": "ok"}
    reject, reason = should_reject_llm_validation(
        resp, reject_tags=set(DEFAULT_REJECT_TAGS),
    )
    assert reject is False
    assert reason == "ok"


def test_impossible_price_tag_rejects():
    resp = {"looks_real": True, "risk_tag": "impossible_price"}
    reject, reason = should_reject_llm_validation(
        resp, reject_tags=set(DEFAULT_REJECT_TAGS),
    )
    assert reject is True
    assert "impossible_price" in reason


def test_stale_price_tag_rejects():
    resp = {"looks_real": True, "risk_tag": "stale_price"}
    reject, reason = should_reject_llm_validation(
        resp, reject_tags=set(DEFAULT_REJECT_TAGS),
    )
    assert reject is True
    assert "stale_price" in reason


def test_unknown_risk_tag_does_not_reject():
    """Tag NOT in reject set → allow. Future-proof: new tags surface
    in logs but don't auto-veto."""
    resp = {"looks_real": True, "risk_tag": "some_new_tag"}
    reject, reason = should_reject_llm_validation(
        resp, reject_tags=set(DEFAULT_REJECT_TAGS),
    )
    assert reject is False
    assert reason == "ok"


def test_custom_reject_tag_set_overrides_default():
    """Operator can configure a different tag set via env."""
    resp = {"looks_real": True, "risk_tag": "custom_veto"}
    reject, reason = should_reject_llm_validation(
        resp, reject_tags={"custom_veto"},
    )
    assert reject is True
    assert "custom_veto" in reason


def test_risk_tag_none_does_not_reject():
    """Missing risk_tag is not a veto."""
    resp = {"looks_real": True, "risk_tag": None}
    reject, _ = should_reject_llm_validation(
        resp, reject_tags=set(DEFAULT_REJECT_TAGS),
    )
    assert reject is False


def test_risk_tag_non_string_does_not_crash():
    """Defensive: malformed response (e.g., risk_tag is a number) doesn't
    raise — treat as no veto."""
    resp = {"looks_real": True, "risk_tag": 42}
    reject, _ = should_reject_llm_validation(
        resp, reject_tags=set(DEFAULT_REJECT_TAGS),
    )
    assert reject is False
