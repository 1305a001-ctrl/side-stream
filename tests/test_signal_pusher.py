"""Pure-helper tests for signal_pusher."""
from __future__ import annotations

import os

os.environ.setdefault("POSTGRES_DSN", "postgresql://test:test@localhost:5432/test")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")

from side_stream.signal_pusher import (
    format_gmx_alert,
    format_signal_message,
    parse_chainlink_eval,
    parse_gmx_execution,
    passes_gmx_alert_gate,
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


# ─── parse_gmx_execution + GMX alert gate (pure) ──────────────────


def _gmx_fields(**overrides) -> dict:
    """Sample Redis stream fields as emitted by gmx-strategies."""
    base = {
        "ts_unix": "1778919763",
        "user": "0x0171d947ee6ce0f487490bd4f8d89878ff2d88ba",
        "market": "btc",
        "is_long": "1",
        "size_usd": "389292.1514",
        "collateral_usd": "49.8008",
        "distance_to_liq_pct": "-0.4872",
        "expected_fee_usd": "1946.4608",
        "expected_gas_usd": "1.5000",
        "expected_net_pnl_usd": "1944.9608",
        "confidence": "0.5244",
        "reason": "trigger",
        "mode": "paper",
        "tx_hash": "",
    }
    base.update(overrides)
    return base


def test_parse_gmx_execution_full_record():
    norm = parse_gmx_execution(_gmx_fields())
    assert norm is not None
    assert norm["user"] == "0x0171d947ee6ce0f487490bd4f8d89878ff2d88ba"
    assert norm["market"] == "btc"
    assert norm["is_long"] is True
    assert norm["size_usd"] == 389292.1514
    assert norm["expected_net_pnl_usd"] == 1944.9608
    assert norm["confidence"] == 0.5244


def test_parse_gmx_execution_short_position():
    norm = parse_gmx_execution(_gmx_fields(is_long="0"))
    assert norm is not None
    assert norm["is_long"] is False


def test_parse_gmx_execution_rejects_missing_user():
    fields = _gmx_fields()
    del fields["user"]
    assert parse_gmx_execution(fields) is None


def test_parse_gmx_execution_rejects_non_dict():
    assert parse_gmx_execution("not-a-dict") is None  # type: ignore[arg-type]
    assert parse_gmx_execution(None) is None  # type: ignore[arg-type]
    assert parse_gmx_execution([1, 2]) is None  # type: ignore[arg-type]


def test_parse_gmx_execution_handles_missing_numerics():
    """Producer sometimes omits fields; coercion must default to 0.0 not raise."""
    norm = parse_gmx_execution({
        "user": "0xabc", "market": "btc",
    })
    assert norm is not None
    assert norm["size_usd"] == 0.0
    assert norm["expected_net_pnl_usd"] == 0.0


def test_passes_gmx_alert_gate_accepts_whale_with_profit():
    norm = parse_gmx_execution(_gmx_fields())
    assert passes_gmx_alert_gate(
        norm, min_net_pnl_usd=500.0, min_size_usd=50_000.0,
    ) is True


def test_passes_gmx_alert_gate_rejects_small_size():
    """The Pro+ value prop is whale-scale — small accounts get filtered."""
    norm = parse_gmx_execution(_gmx_fields(size_usd="5000.0"))
    assert passes_gmx_alert_gate(
        norm, min_net_pnl_usd=500.0, min_size_usd=50_000.0,
    ) is False


def test_passes_gmx_alert_gate_rejects_low_pnl():
    norm = parse_gmx_execution(_gmx_fields(
        size_usd="100000.0", expected_net_pnl_usd="100.0",
    ))
    assert passes_gmx_alert_gate(
        norm, min_net_pnl_usd=500.0, min_size_usd=50_000.0,
    ) is False


def test_format_gmx_alert_includes_essential_fields():
    norm = parse_gmx_execution(_gmx_fields())
    msg = format_gmx_alert(norm, brand_name="Streams Edge")
    assert "Streams Edge" in msg
    assert "LONG" in msg
    assert "BTC" in msg
    # Whale-scale size formatted with commas
    assert "$389,292" in msg
    # Address shown abbreviated for readability — 0x0171…88ba
    assert "0x0171" in msg
    assert "88ba" in msg
    # Distance to liq with sign
    assert "-0.49pp" in msg


def test_format_gmx_alert_short_position_marked():
    norm = parse_gmx_execution(_gmx_fields(is_long="0"))
    msg = format_gmx_alert(norm, brand_name="Streams Edge")
    assert "SHORT" in msg
    assert "LONG" not in msg
