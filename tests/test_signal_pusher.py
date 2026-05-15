"""Pure-helper tests for signal_pusher."""
from __future__ import annotations

import os

os.environ.setdefault("POSTGRES_DSN", "postgresql://test:test@localhost:5432/test")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")

from side_stream.signal_pusher import (
    format_signal_message,
    parse_chainlink_eval,
    select_free_top_n,
)


def test_parse_chainlink_eval_skips_non_emit_decisions():
    # 'eval' is just the eval-log — not broadcast-worthy
    out = parse_chainlink_eval({
        "slug": "btc-updown-5m-1778856300",
        "decision": "eval",
        "edge_pp": 0.05,
        "fair_yes": 0.5,
        "market_yes": 0.515,
        "cadence": "5m",
        "evaluated_at_unix": 1778853000.0,
    })
    assert out is None


def test_parse_chainlink_eval_accepts_would_emit():
    out = parse_chainlink_eval({
        "slug": "btc-updown-5m-1778856300",
        "decision": "would_emit",
        "edge_pp": 0.07,
        "fair_yes": 0.42,
        "market_yes": 0.49,
        "cadence": "5m",
        "market_volume_24h": 12345.0,
        "evaluated_at_unix": 1778853000.0,
        "direction": "long",
    })
    assert out is not None
    assert out["slug"] == "btc-updown-5m-1778856300"
    assert out["decision"] == "would_emit"
    assert out["edge_pp"] == 0.07
    assert out["direction"] == "long"


def test_parse_chainlink_eval_handles_malformed():
    assert parse_chainlink_eval("not a dict") is None  # type: ignore[arg-type]
    assert parse_chainlink_eval({}) is None
    assert parse_chainlink_eval({"slug": "x", "decision": "would_emit"}) is None
    assert parse_chainlink_eval(
        {"slug": "x", "decision": "would_emit", "edge_pp": "not a number"}
    ) is None


def test_format_signal_message_free_is_shorter_than_pro():
    norm = {
        "slug": "btc-updown-5m-1778856300",
        "direction": "long",
        "edge_pp": 0.07,
        "fair_yes": 0.42,
        "market_yes": 0.49,
        "cadence": "5m",
        "market_volume_24h": 12345.0,
        "evaluated_at_unix": 1778853000.0,
    }
    free = format_signal_message(norm, brand_name="TestBrand", is_pro=False)
    pro = format_signal_message(norm, brand_name="TestBrand", is_pro=True)
    assert len(free) < len(pro)
    assert "TestBrand" in free
    assert "TestBrand" in pro
    # Pro tier shows full edge math, free does not.
    assert "fair_yes" in pro
    assert "fair_yes" not in free


def test_select_free_top_n_returns_highest_edge():
    buf = [
        {"slug": "a", "edge_pp": 0.05},
        {"slug": "b", "edge_pp": 0.10},
        {"slug": "c", "edge_pp": 0.07},
        {"slug": "d", "edge_pp": 0.03},
    ]
    top = select_free_top_n(buf, top_n=2)
    assert len(top) == 2
    assert top[0]["slug"] == "b"
    assert top[1]["slug"] == "c"


def test_select_free_top_n_handles_empty():
    assert select_free_top_n([], top_n=3) == []


def test_select_free_top_n_smaller_than_n():
    buf = [{"slug": "a", "edge_pp": 0.05}]
    top = select_free_top_n(buf, top_n=3)
    assert len(top) == 1
