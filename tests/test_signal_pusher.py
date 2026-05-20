"""Pure-helper tests for signal_pusher."""
from __future__ import annotations

import os

os.environ.setdefault("POSTGRES_DSN", "postgresql://test:test@localhost:5432/test")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")

from unittest.mock import AsyncMock

import pytest

from side_stream.signal_pusher import (
    format_gmx_alert,
    format_signal_message,
    is_emitted_signal,
    is_source_halted,
    parse_chainlink_eval,
    parse_gmx_execution,
    passes_gmx_alert_gate,
    select_free_top_n,
    source_to_strategy_slug,
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


# ─── A1 — halt-aware broadcast (eval doc 2026-05-20) ───────────────────


def test_source_to_strategy_slug_chainlink_passthrough():
    """chainlink_lag broadcast source uses the same strategy slug."""
    assert source_to_strategy_slug("chainlink_lag") == "chainlink_lag"


def test_source_to_strategy_slug_gmx_maps_to_liquidator():
    """GMX broadcast 'gmx_liquidation' maps to upstream strategy
    'gmx_liquidator' — the strategy:halts set uses the latter name."""
    assert source_to_strategy_slug("gmx_liquidation") == "gmx_liquidator"


def test_source_to_strategy_slug_unknown_passes_through():
    """Unknown sources fall through to their own name — defensive default
    so a typo doesn't silently break halt-checking."""
    assert source_to_strategy_slug("future_source") == "future_source"


def _make_fake_redis(*, strategy: set[str], publishing: set[str]) -> AsyncMock:
    """Build an AsyncMock with a sismember that reads from the given sets."""
    fake = AsyncMock()

    async def sismember(key: str, member: str) -> int:
        if key == "strategy:halts":
            return 1 if member in strategy else 0
        if key == "publishing:halts":
            return 1 if member in publishing else 0
        return 0

    fake.sismember = sismember
    return fake


@pytest.mark.asyncio
async def test_is_source_halted_returns_false_when_no_halts():
    fake = _make_fake_redis(strategy=set(), publishing=set())
    assert await is_source_halted(fake, source="chainlink_lag") is False


@pytest.mark.asyncio
async def test_is_source_halted_when_strategy_halted():
    """Trading-side halt for the strategy cascades to publishing — the
    brand-decay guard. If we're not trading it, we don't broadcast it."""
    fake = _make_fake_redis(strategy={"chainlink_lag"}, publishing=set())
    assert await is_source_halted(fake, source="chainlink_lag") is True


@pytest.mark.asyncio
async def test_is_source_halted_when_strategy_halted_gmx_slug_mapped():
    """GMX broadcast source must check against 'gmx_liquidator' (slug),
    not 'gmx_liquidation' (broadcast label)."""
    fake = _make_fake_redis(strategy={"gmx_liquidator"}, publishing=set())
    assert await is_source_halted(fake, source="gmx_liquidation") is True


@pytest.mark.asyncio
async def test_is_source_halted_when_publishing_paused_for_source():
    """Operator paused this specific source via /v1/admin/pause."""
    fake = _make_fake_redis(strategy=set(), publishing={"chainlink_lag"})
    assert await is_source_halted(fake, source="chainlink_lag") is True


@pytest.mark.asyncio
async def test_is_source_halted_when_publishing_paused_all():
    """Blanket 'all' pause halts every source — used for maintenance windows."""
    fake = _make_fake_redis(strategy=set(), publishing={"all"})
    assert await is_source_halted(fake, source="chainlink_lag") is True
    assert await is_source_halted(fake, source="gmx_liquidation") is True


@pytest.mark.asyncio
async def test_is_source_halted_fails_open_on_redis_error():
    """Redis hiccup must NOT silently freeze all publishing. A1 fails OPEN
    on transient errors — the Sharpe-gate (A4, separate) enforces the
    quality-based pause; A1 is only operator + upstream-cascade halt."""
    fake = AsyncMock()
    fake.sismember = AsyncMock(side_effect=ConnectionError("redis down"))
    assert await is_source_halted(fake, source="chainlink_lag") is False


# ─── A8 — eat-own-dogfood gate (eval doc kill-list rule 5) ─────────────


def test_is_emitted_signal_accepts_emitted():
    """Only 'emitted' = strategy actually sent to OMS = we trade it."""
    assert is_emitted_signal({"decision": "emitted"}) is True


def test_is_emitted_signal_rejects_would_emit():
    """'would_emit' = candidate, blocked by some other gate — Pro subs
    pay for what we trade, not what we considered."""
    assert is_emitted_signal({"decision": "would_emit"}) is False


def test_is_emitted_signal_rejects_unknown_decision():
    """Defensive: unknown decision values block (safer than allowing)."""
    assert is_emitted_signal({"decision": "skipped"}) is False
    assert is_emitted_signal({}) is False
    assert is_emitted_signal({"decision": None}) is False
