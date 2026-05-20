"""Backtest dashboard — shows paper Brier + Sharpe + sample signals to
prospects. Pro+ subscribers see the full feed.

Reads from Redis streams emitted by strategy-runners:
  chainlink:brier:daily             — daily Brier-score rollup per strategy
  tokenized_equity:perf:daily       — equity arb perf rollup
  liquidation:perf:daily            — liquidation perf rollup
  poly:sharpe:daily                 — rolling Sharpe per Polymarket strategy
  gmx:execution:paper_log           — recent GMX would-fire candidates
  liquidation:eval_log              — recent Aave near-liq candidates

All endpoints fail gracefully (return empty arrays) if a stream is
missing or Redis is unreachable — prospects browsing during a brief
outage shouldn't see error pages.
"""
from __future__ import annotations

import json
import logging
import time
from typing import Any

import redis.asyncio as redis_async
from fastapi import APIRouter, Query

from side_stream.settings import settings

log = logging.getLogger(__name__)


router = APIRouter(prefix="/v1/dashboard")


_pool: redis_async.Redis | None = None


def _get_redis() -> redis_async.Redis:
    global _pool
    if _pool is None:
        _pool = redis_async.from_url(settings.redis_url, decode_responses=True)
    return _pool


async def _read_latest(stream: str, count: int = 30) -> list[dict[str, Any]]:
    """Pure-ish: read the latest N entries from a Redis stream.

    Returns [] on any error (Redis down, stream missing, decode failure).
    """
    try:
        rc = _get_redis()
        entries = await rc.xrevrange(stream, count=count)
    except Exception as e:  # noqa: BLE001 — fail-defensive: any failure → graceful no-op
        log.debug("dashboard.xrevrange_failed stream=%s err=%s", stream, e)
        return []

    out: list[dict[str, Any]] = []
    for entry_id, fields in entries:
        row: dict[str, Any] = {"id": entry_id}
        for k, v in fields.items():
            if not isinstance(v, str):
                row[k] = v
                continue
            # Try JSON decode for fields that look like JSON.
            if v.startswith(("{", "[")):
                try:
                    row[k] = json.loads(v)
                    continue
                except ValueError:
                    pass
            # Try numeric decode.
            try:
                row[k] = float(v) if "." in v else int(v)
            except ValueError:
                row[k] = v
        out.append(row)
    return out


@router.get("/brier")
async def brier_summary(count: int = Query(30, ge=1, le=200)) -> dict[str, Any]:
    """Recent daily Brier rollups across strategies."""
    rows = await _read_latest("chainlink:brier:daily", count)
    return {
        "stream": "chainlink:brier:daily",
        "count": len(rows),
        "baseline": 0.25,
        "rows": rows,
        "fetched_at_unix": int(time.time()),
    }


@router.get("/equity-perf")
async def equity_perf(count: int = Query(30, ge=1, le=200)) -> dict[str, Any]:
    rows = await _read_latest("tokenized_equity:perf:daily", count)
    return {
        "stream": "tokenized_equity:perf:daily",
        "count": len(rows),
        "rows": rows,
        "fetched_at_unix": int(time.time()),
    }


@router.get("/liquidation-perf")
async def liquidation_perf(count: int = Query(30, ge=1, le=200)) -> dict[str, Any]:
    rows = await _read_latest("liquidation:perf:daily", count)
    return {
        "stream": "liquidation:perf:daily",
        "count": len(rows),
        "rows": rows,
        "fetched_at_unix": int(time.time()),
    }


@router.get("/gmx-recent")
async def gmx_recent(count: int = Query(30, ge=1, le=100)) -> dict[str, Any]:
    """Recent GMX execution candidates (paper mode). Pro+ tier."""
    rows = await _read_latest("gmx:execution:paper_log", count)
    return {
        "stream": "gmx:execution:paper_log",
        "count": len(rows),
        "rows": rows,
        "fetched_at_unix": int(time.time()),
    }


@router.get("/aave-recent")
async def aave_recent(count: int = Query(30, ge=1, le=100)) -> dict[str, Any]:
    """Recent Aave V3 near-liq candidates. Pro+ tier."""
    rows = await _read_latest("liquidation:eval_log", count)
    return {
        "stream": "liquidation:eval_log",
        "count": len(rows),
        "rows": rows,
        "fetched_at_unix": int(time.time()),
    }


@router.get("/poly-sharpe")
async def poly_sharpe(count: int = Query(30, ge=1, le=200)) -> dict[str, Any]:
    """Per-strategy rolling Sharpe on Polymarket fleet."""
    rows = await _read_latest("poly:sharpe:daily", count)
    return {
        "stream": "poly:sharpe:daily",
        "count": len(rows),
        "rows": rows,
        "fetched_at_unix": int(time.time()),
    }


@router.get("/summary")
async def all_lanes_summary() -> dict[str, Any]:
    """One-shot snapshot across all 3 lanes. Used by prospect-facing
    dashboard widget."""
    brier, equity, liq, poly_sharpe_rows = (
        await _read_latest("chainlink:brier:daily", 1),
        await _read_latest("tokenized_equity:perf:daily", 1),
        await _read_latest("liquidation:perf:daily", 1),
        await _read_latest("poly:sharpe:daily", 5),
    )
    return {
        "lanes": {
            "polymarket": {
                "latest_sharpe": poly_sharpe_rows[0] if poly_sharpe_rows else None,
                "active_strategies": len({r.get("strategy") for r in poly_sharpe_rows}),
            },
            "chainlink_lag": {
                "latest_brier": brier[0] if brier else None,
                "baseline": 0.25,
            },
            "equity_arb": {
                "latest": equity[0] if equity else None,
            },
            "liquidation": {
                "latest": liq[0] if liq else None,
            },
        },
        "fetched_at_unix": int(time.time()),
    }


@router.get("/quality")
async def quality_snapshots() -> dict[str, Any]:
    """Public quality leaderboard — A4 snapshot data (D3 differentiator).

    Reads publishing:quality:<slug> for each tracked strategy. Used by
    the landing page leaderboard widget — the transparency promise:
    'open the book, every Sunday' becomes 'open the book, live'.

    Each snapshot carries: n_closed (30d), total_pnl_usd, sharpe,
    win_rate, computed_at_unix. Missing snapshots are omitted (not
    returned as null) so a strategy that hasn't published yet doesn't
    look broken on the leaderboard.
    """
    from side_stream import quality_snapshot

    tracked = [
        s.strip() for s in settings.quality_tracked_slugs.split(",") if s.strip()
    ]
    rc = _get_redis()
    out: dict[str, Any] = {}
    for slug in tracked:
        snap = await quality_snapshot.read_snapshot(rc, slug=slug)
        if snap is not None:
            out[slug] = snap
    return {
        "snapshots": out,
        "tracked_slugs": tracked,
        "fetched_at_unix": int(time.time()),
    }


__all__ = ["router"]
