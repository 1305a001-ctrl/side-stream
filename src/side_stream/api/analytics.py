"""Signal → conversion analytics.

Tracks which broadcast signals correlate with sign-ups. Uses Postgres
view aggregations on existing tables:

  broadcast_logs   (signal_id, broadcast_at, channel)
  subscriptions    (started_at, payment_provider, tier)

Plus an optional `signal_clicks` table populated by short-link redirects
(landing-page CTAs carry ?ref=<signal_id> when shared from broadcasts).

This module ships the SQL queries + endpoints. The signal_clicks table
is created lazily via CREATE TABLE IF NOT EXISTS on first call.
"""
from __future__ import annotations

import logging
import time
from typing import Any

import asyncpg
from fastapi import APIRouter, Query

from side_stream.settings import settings

log = logging.getLogger(__name__)


router = APIRouter(prefix="/v1/analytics")


# Lazy connection pool; created on first call to avoid blocking app startup
# when Postgres is briefly unavailable.
_pg_pool: asyncpg.Pool | None = None


async def _pool() -> asyncpg.Pool | None:
    global _pg_pool
    if _pg_pool is not None:
        return _pg_pool
    try:
        _pg_pool = await asyncpg.create_pool(
            settings.postgres_dsn, min_size=1, max_size=4,
        )
    except Exception as e:  # noqa: BLE001 — fail-defensive: any failure → graceful no-op
        log.debug("analytics.pool_create_failed err=%s", e)
        return None
    return _pg_pool


SIGNAL_CLICKS_DDL = """
CREATE TABLE IF NOT EXISTS signal_clicks (
    id          BIGSERIAL PRIMARY KEY,
    signal_id   TEXT NOT NULL,
    clicked_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    user_agent  TEXT,
    referrer    TEXT,
    converted_to_signup_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS signal_clicks_signal_idx ON signal_clicks (signal_id);
CREATE INDEX IF NOT EXISTS signal_clicks_clicked_at_idx ON signal_clicks (clicked_at);
"""


async def _ensure_schema(p: asyncpg.Pool) -> None:
    try:
        async with p.acquire() as conn:
            await conn.execute(SIGNAL_CLICKS_DDL)
    except Exception as e:  # noqa: BLE001 — fail-defensive: any failure → graceful no-op
        log.debug("analytics.ensure_schema_failed err=%s", e)


@router.post("/track-click")
async def track_click(
    signal_id: str = Query(..., max_length=200),
    user_agent: str = Query("", max_length=500),
    referrer: str = Query("", max_length=500),
) -> dict[str, Any]:
    """Record a click on a broadcast signal's outbound link.

    Called by the landing-page redirect handler when a visitor arrives
    via ?ref=<signal_id>. Fail-graceful: if Postgres is down, returns
    {"ok": False} without erroring.
    """
    p = await _pool()
    if p is None:
        return {"ok": False, "reason": "pg_unavailable"}
    await _ensure_schema(p)
    try:
        async with p.acquire() as conn:
            await conn.execute(
                "INSERT INTO signal_clicks (signal_id, user_agent, referrer) "
                "VALUES ($1, $2, $3)",
                signal_id, user_agent[:500], referrer[:500],
            )
    except Exception as e:  # noqa: BLE001 — fail-defensive: any failure → graceful no-op
        log.debug("analytics.track_click_failed err=%s", e)
        return {"ok": False, "reason": "insert_failed"}
    return {"ok": True}


@router.get("/conversion-funnel")
async def conversion_funnel(days: int = Query(7, ge=1, le=90)) -> dict[str, Any]:
    """Funnel over last N days: broadcasts → clicks → sign-ups."""
    p = await _pool()
    if p is None:
        return {"ok": False, "reason": "pg_unavailable"}

    sql = """
    WITH window_start AS (
        SELECT NOW() - ($1 || ' days')::INTERVAL AS ts
    ),
    bc AS (
        SELECT COUNT(*) AS n
        FROM broadcast_logs, window_start
        WHERE broadcast_at >= window_start.ts
    ),
    cl AS (
        SELECT COUNT(*) AS n
        FROM signal_clicks, window_start
        WHERE clicked_at >= window_start.ts
    ),
    su AS (
        SELECT COUNT(*) AS n
        FROM subscriptions, window_start
        WHERE started_at >= window_start.ts
          AND tier <> 'free'
    )
    SELECT bc.n AS broadcasts, cl.n AS clicks, su.n AS paid_signups
    FROM bc, cl, su;
    """
    try:
        async with p.acquire() as conn:
            row = await conn.fetchrow(sql, str(days))
    except Exception as e:  # noqa: BLE001 — fail-defensive: any failure → graceful no-op
        log.debug("analytics.funnel_failed err=%s", e)
        return {"ok": False, "reason": "query_failed"}

    if row is None:
        return {
            "ok": True,
            "days": days,
            "broadcasts": 0, "clicks": 0, "paid_signups": 0,
            "ctr_pct": 0.0, "conversion_pct": 0.0,
        }
    broadcasts = int(row["broadcasts"] or 0)
    clicks = int(row["clicks"] or 0)
    paid = int(row["paid_signups"] or 0)
    return {
        "ok": True,
        "days": days,
        "broadcasts": broadcasts,
        "clicks": clicks,
        "paid_signups": paid,
        "ctr_pct": round(100 * clicks / broadcasts, 2) if broadcasts else 0.0,
        "conversion_pct": round(100 * paid / clicks, 2) if clicks else 0.0,
        "fetched_at_unix": int(time.time()),
    }


@router.get("/top-signals")
async def top_signals(
    days: int = Query(7, ge=1, le=90),
    limit: int = Query(20, ge=1, le=200),
) -> dict[str, Any]:
    """Which broadcast signals drove the most clicks in the last N days."""
    p = await _pool()
    if p is None:
        return {"ok": False, "reason": "pg_unavailable"}

    sql = """
    SELECT signal_id, COUNT(*) AS n
    FROM signal_clicks
    WHERE clicked_at >= NOW() - ($1 || ' days')::INTERVAL
    GROUP BY signal_id
    ORDER BY n DESC
    LIMIT $2
    """
    try:
        async with p.acquire() as conn:
            rows = await conn.fetch(sql, str(days), limit)
    except Exception as e:  # noqa: BLE001 — fail-defensive: any failure → graceful no-op
        log.debug("analytics.top_signals_failed err=%s", e)
        return {"ok": False, "reason": "query_failed"}

    return {
        "ok": True,
        "days": days,
        "rows": [
            {"signal_id": r["signal_id"], "clicks": int(r["n"])}
            for r in rows
        ],
        "fetched_at_unix": int(time.time()),
    }


__all__ = ["router"]
