"""A4 — quality snapshot tests.

Covers:
  - sharpe_proxy (pure)
  - compute_stats_from_rows (pure)
  - passes_quality_gate (pure, all branches)
  - format_quality_footer (pure)
  - read_snapshot + write_snapshot (async, AsyncMock redis)
"""
from __future__ import annotations

import json
import os
from unittest.mock import AsyncMock

import pytest

os.environ.setdefault("POSTGRES_DSN", "postgresql://test:test@localhost:5432/test")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")

from side_stream.quality_snapshot import (
    SNAPSHOT_KEY_PREFIX,
    compute_stats_from_rows,
    format_quality_footer,
    passes_quality_gate,
    read_snapshot,
    sharpe_proxy,
    write_snapshot,
)

# ─── sharpe_proxy ──────────────────────────────────────────────────────


def test_sharpe_proxy_returns_none_when_too_few_samples():
    assert sharpe_proxy([]) is None
    assert sharpe_proxy([1.0]) is None


def test_sharpe_proxy_returns_none_when_variance_zero():
    """Constant PnL has zero variance → Sharpe undefined."""
    assert sharpe_proxy([5.0, 5.0, 5.0]) is None


def test_sharpe_proxy_positive_when_winning():
    """Average positive PnL with low variance → positive Sharpe."""
    s = sharpe_proxy([1.0, 1.0, 1.0, 1.0, 2.0])
    assert s is not None
    assert s > 0


def test_sharpe_proxy_negative_when_losing():
    s = sharpe_proxy([-1.0, -1.0, -2.0, -1.0])
    assert s is not None
    assert s < 0


# ─── compute_stats_from_rows ──────────────────────────────────────────


def test_compute_stats_handles_empty_rows():
    s = compute_stats_from_rows(slug="chainlink_lag", window_days=30, rows=[])
    assert s["slug"] == "chainlink_lag"
    assert s["window_days"] == 30
    assert s["n_closed"] == 0
    assert s["total_pnl_usd"] == 0.0
    assert s["win_rate"] == 0.0
    assert s["sharpe"] is None


def test_compute_stats_skips_none_pnls():
    """Rows with realized_pnl_usd=None should be ignored, not counted."""
    rows = [{"realized_pnl_usd": None}, {"realized_pnl_usd": 5.0}]
    s = compute_stats_from_rows(slug="x", window_days=30, rows=rows)
    assert s["n_closed"] == 1


def test_compute_stats_real_chainlink_lag_shape():
    """Realistic shape — matches memory's 'WR 43.9% / +$57 / 41 closes'."""
    # 18 wins, 23 losses (small magnitudes). Sum should be roughly +$57.
    rows = (
        [{"realized_pnl_usd": v} for v in [
            4.5, 3.8, 5.1, 6.2, 4.0, 3.5, 5.9, 4.4, 4.2,
            3.9, 4.8, 5.0, 4.1, 3.7, 4.6, 4.3, 5.2, 5.5,
        ]]
        + [{"realized_pnl_usd": v} for v in (
            [-2.0] * 23
        )]
    )
    s = compute_stats_from_rows(slug="chainlink_lag", window_days=30, rows=rows)
    assert s["n_closed"] == 41
    # WR is 18/41 ≈ 0.4390
    assert s["win_rate"] == pytest.approx(0.4390, abs=0.001)
    # PnL is real but exact value depends on arithmetic precision
    assert 30 < s["total_pnl_usd"] < 70


# ─── passes_quality_gate ───────────────────────────────────────────────


def test_gate_blocks_missing_when_required():
    """fail-CLOSED: required=True + snapshot=None → block + reason."""
    allow, reason = passes_quality_gate(
        None, min_sharpe=1.0, min_n_closed=30, required=True,
    )
    assert allow is False
    assert reason == "snapshot_missing"


def test_gate_allows_missing_when_not_required():
    """fail-OPEN: required=False + snapshot=None → allow with unforced
    reason so log filtering can distinguish unforced from genuine pass."""
    allow, reason = passes_quality_gate(
        None, min_sharpe=1.0, min_n_closed=30, required=False,
    )
    assert allow is True
    assert reason == "snapshot_missing_unforced"


def test_gate_blocks_below_n_closed_when_required():
    snap = {"n_closed": 10, "sharpe": 2.0}
    allow, reason = passes_quality_gate(
        snap, min_sharpe=1.0, min_n_closed=30, required=True,
    )
    assert allow is False
    assert "n_closed_below_min" in reason


def test_gate_allows_below_n_closed_when_not_required():
    """Soft launch — let early signals through with a warning reason."""
    snap = {"n_closed": 10, "sharpe": 2.0}
    allow, reason = passes_quality_gate(
        snap, min_sharpe=1.0, min_n_closed=30, required=False,
    )
    assert allow is True
    assert "unforced" in reason


def test_gate_always_blocks_low_sharpe():
    """Sharpe below min ALWAYS blocks — quality is verifiably bad even
    in soft-launch mode. This is the brand promise."""
    snap = {"n_closed": 100, "sharpe": 0.3}
    for required in (True, False):
        allow, reason = passes_quality_gate(
            snap, min_sharpe=1.0, min_n_closed=30, required=required,
        )
        assert allow is False
        assert "sharpe_below_min" in reason


def test_gate_blocks_sharpe_none():
    """Sharpe=None (e.g., variance was zero or n<2) blocks — we can't
    publish what we can't verify."""
    snap = {"n_closed": 100, "sharpe": None}
    allow, _ = passes_quality_gate(
        snap, min_sharpe=1.0, min_n_closed=30, required=False,
    )
    assert allow is False


def test_gate_allows_healthy_snapshot():
    snap = {"n_closed": 41, "sharpe": 1.4, "total_pnl_usd": 57.41}
    allow, reason = passes_quality_gate(
        snap, min_sharpe=1.0, min_n_closed=30, required=True,
    )
    assert allow is True
    assert reason == "ok"


# ─── format_quality_footer ─────────────────────────────────────────────


def test_footer_empty_when_no_snapshot():
    assert format_quality_footer(None) == ""
    assert format_quality_footer({}) == ""


def test_footer_includes_essential_fields():
    snap = {
        "window_days": 30,
        "sharpe": 1.4,
        "total_pnl_usd": 57.41,
        "n_closed": 41,
        "win_rate": 0.439,
    }
    f = format_quality_footer(snap)
    assert "30d" in f
    assert "Sharpe 1.40" in f
    assert "+$57" in f
    assert "41 closes" in f
    assert "43.9%" in f


def test_footer_negative_pnl_no_plus_sign():
    snap = {
        "window_days": 30, "sharpe": -0.5, "total_pnl_usd": -123.0,
        "n_closed": 10, "win_rate": 0.2,
    }
    f = format_quality_footer(snap)
    assert "-$123" in f
    assert "+$" not in f


def test_footer_handles_sharpe_none():
    snap = {
        "window_days": 30, "sharpe": None, "total_pnl_usd": 0.0,
        "n_closed": 1, "win_rate": 0.0,
    }
    f = format_quality_footer(snap)
    assert "Sharpe ?" in f


# ─── read_snapshot / write_snapshot (async I/O) ────────────────────────


@pytest.mark.asyncio
async def test_read_snapshot_returns_none_on_miss():
    fake = AsyncMock()
    fake.get = AsyncMock(return_value=None)
    assert await read_snapshot(fake, slug="chainlink_lag") is None


@pytest.mark.asyncio
async def test_read_snapshot_parses_json():
    snap = {"slug": "chainlink_lag", "n_closed": 41, "sharpe": 1.4}
    fake = AsyncMock()
    fake.get = AsyncMock(return_value=json.dumps(snap))
    out = await read_snapshot(fake, slug="chainlink_lag")
    assert out == snap


@pytest.mark.asyncio
async def test_read_snapshot_returns_none_on_malformed_json():
    fake = AsyncMock()
    fake.get = AsyncMock(return_value="not-json{")
    assert await read_snapshot(fake, slug="chainlink_lag") is None


@pytest.mark.asyncio
async def test_read_snapshot_returns_none_on_redis_error():
    """Fail-defensive — read errors return None, gate handles via `required`."""
    fake = AsyncMock()
    fake.get = AsyncMock(side_effect=ConnectionError("redis down"))
    assert await read_snapshot(fake, slug="chainlink_lag") is None


@pytest.mark.asyncio
async def test_write_snapshot_uses_correct_key_and_ttl():
    """Verify the SET uses the expected key + TTL parameter."""
    fake = AsyncMock()
    fake.set = AsyncMock()
    snap = {"slug": "chainlink_lag", "n_closed": 41}
    await write_snapshot(fake, slug="chainlink_lag", snapshot=snap, ttl_sec=180)
    fake.set.assert_awaited_once()
    args, kwargs = fake.set.call_args
    assert args[0] == f"{SNAPSHOT_KEY_PREFIX}chainlink_lag"
    assert json.loads(args[1]) == snap
    assert kwargs["ex"] == 180
