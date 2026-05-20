"""Quality snapshot — periodic compute + Redis publish + read.

A4 (eval doc 2026-05-20). The keystone differentiator (D1 + D4 in the
launch plan):

  D1  "Calibration-gated publishing" — Sharpe < threshold halts publishing.
  D4  "Public quality leaderboard"   — every Pro signal card carries
                                       live 30d Sharpe + PnL + n_closed
                                       footer. Open the book.

Architecture:

  WRITER (runs alongside signal_pusher consumers via writer_loop):
    every `snapshot_interval_sec`, for each tracked strategy slug,
    pull last-window closes from Postgres → compute stats → write JSON
    to ``publishing:quality:<slug>`` with EXPIRE = ``snapshot_ttl_sec``
    (so a dead writer stops freezing stale data).

  READER (called by signal_pusher.broadcast helpers):
    ``read_snapshot(redis_client, slug)`` → dict | None
    ``passes_quality_gate(snapshot, ...)`` → (allow, reason)
    ``format_quality_footer(snapshot)``    → "_Verified 30d: Sharpe 1.4 …_"

Failure mode for missing/stale snapshot is governed by
``settings.quality_snapshot_required``:

  False (default; soft launch)    → broadcast with "(quality unknown)"
                                     footer; loud log warn.
  True  (flip for public launch)  → skip broadcast (fail-closed = brand
                                     promise).

Schema of the JSON blob in Redis:

  {
    "slug":             "chainlink_lag",
    "window_days":      30,
    "n_closed":         41,
    "total_pnl_usd":    57.41,
    "win_rate":         0.4390,
    "sharpe":           1.4,            # unitless mean/std proxy (matches
                                        # research-loop.sharpe_proxy)
    "computed_at_unix": 1747800000.0,
  }
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

import asyncpg
import redis.asyncio as redis_async

log = logging.getLogger(__name__)


SNAPSHOT_KEY_PREFIX = "publishing:quality:"


# ─── Pure helpers ──────────────────────────────────────────────────────


def sharpe_proxy(pnls: list[float]) -> float | None:
    """Unitless Sharpe proxy: mean(pnl) / std(pnl). None if n<2 or std=0.

    Mirrors research-loop's sharpe_proxy semantics so a strategy with
    sharpe>=1.0 here is comparable to research-loop's halt threshold.
    Not annualized — the comparison is to the gate threshold, not to
    an external benchmark.
    """
    if len(pnls) < 2:
        return None
    n = len(pnls)
    mean_pnl = sum(pnls) / n
    var = sum((p - mean_pnl) ** 2 for p in pnls) / (n - 1)
    if var <= 0:
        return None
    return mean_pnl / (var ** 0.5)


def compute_stats_from_rows(
    *, slug: str, window_days: int, rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """Pure: aggregate position rows → snapshot dict.

    `rows` is the result of fetching `realized_pnl_usd` per closed
    position; we tolerate Decimal or float values.
    """
    pnls: list[float] = []
    wins = 0
    for r in rows:
        raw = r.get("realized_pnl_usd")
        if raw is None:
            continue
        try:
            pnl = float(raw)
        except (TypeError, ValueError):
            continue
        pnls.append(pnl)
        if pnl > 0:
            wins += 1
    n = len(pnls)
    return {
        "slug": slug,
        "window_days": window_days,
        "n_closed": n,
        "total_pnl_usd": round(sum(pnls), 2),
        "win_rate": round(wins / n, 4) if n else 0.0,
        "sharpe": sharpe_proxy(pnls),
        "computed_at_unix": time.time(),
    }


def passes_quality_gate(
    snapshot: dict[str, Any] | None,
    *,
    min_sharpe: float,
    min_n_closed: int,
    required: bool,
) -> tuple[bool, str]:
    """Pure: should we broadcast given this snapshot?

    Returns ``(allow, reason)``. Cases:

      snapshot is None:
        required=True  → (False, "snapshot_missing")        ← fail closed
        required=False → (True,  "snapshot_missing_unforced")← fail open
      snapshot has too few closes:
        required=True  → (False, "n_closed_below_min")
        required=False → (True,  "n_closed_below_min_unforced")
      sharpe is None or below min:
        always (False, "sharpe_below_min")  — quality is verifiably bad
      otherwise:
        (True, "ok")
    """
    if snapshot is None:
        if required:
            return False, "snapshot_missing"
        return True, "snapshot_missing_unforced"
    n = int(snapshot.get("n_closed") or 0)
    if n < min_n_closed:
        if required:
            return False, f"n_closed_below_min:{n}<{min_n_closed}"
        return True, f"n_closed_below_min_unforced:{n}<{min_n_closed}"
    sharpe = snapshot.get("sharpe")
    if sharpe is None or float(sharpe) < min_sharpe:
        return False, f"sharpe_below_min:{sharpe}<{min_sharpe}"
    return True, "ok"


def format_quality_footer(snapshot: dict[str, Any] | None) -> str:
    """Pure: build the markdown footer line shown under every Pro signal.

    Empty string when snapshot is None (don't show "(unknown)" — looks
    unprofessional; signal_pusher logs the absence internally instead).

    Format intentionally short — one line — so the card stays scannable:

      _30d: Sharpe 1.40 · +$57 · 41 closes · WR 43.9%_
    """
    if not snapshot:
        return ""
    sharpe = snapshot.get("sharpe")
    pnl = float(snapshot.get("total_pnl_usd") or 0.0)
    n = int(snapshot.get("n_closed") or 0)
    wr = float(snapshot.get("win_rate") or 0.0)
    window = int(snapshot.get("window_days") or 30)
    sharpe_str = f"{float(sharpe):.2f}" if sharpe is not None else "?"
    pnl_sign = "+" if pnl >= 0 else "-"
    return (
        f"_{window}d: Sharpe {sharpe_str} · "
        f"{pnl_sign}${abs(pnl):,.0f} · "
        f"{n} closes · WR {wr * 100:.1f}%_"
    )


# ─── I/O ───────────────────────────────────────────────────────────────


async def fetch_recent_closes(
    pool: asyncpg.Pool, *, slug: str, window_days: int,
) -> list[dict[str, Any]]:
    """Pull last-window closed positions for one strategy slug.

    Uses the canonical positions JOIN strategies pattern (mirrors
    risk-watcher + oms-gateway). Returns list of dicts with one key
    we use: ``realized_pnl_usd``. Anything else is ignored.
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT p.realized_pnl_usd
            FROM positions p
            JOIN strategies s ON s.id = p.strategy_id
            WHERE s.slug = $1
              AND p.status = 'closed'
              AND p.closed_at > NOW() - INTERVAL '{int(window_days)} days'
            """,  # noqa: S608 — window_days is int-coerced, no injection
            slug,
        )
    return [{"realized_pnl_usd": r["realized_pnl_usd"]} for r in rows]


async def compute_snapshot(
    pool: asyncpg.Pool, *, slug: str, window_days: int,
) -> dict[str, Any]:
    """Compose: fetch closes + aggregate → snapshot dict."""
    rows = await fetch_recent_closes(pool, slug=slug, window_days=window_days)
    return compute_stats_from_rows(
        slug=slug, window_days=window_days, rows=rows,
    )


async def write_snapshot(
    redis_client: redis_async.Redis,
    *,
    slug: str,
    snapshot: dict[str, Any],
    ttl_sec: int,
) -> None:
    """SET ``publishing:quality:<slug>`` with EXPIRE.

    TTL is the deadman switch: if the writer dies, snapshots go stale
    in ttl_sec and the gate falls back to its `required` policy.
    """
    key = f"{SNAPSHOT_KEY_PREFIX}{slug}"
    await redis_client.set(key, json.dumps(snapshot), ex=int(ttl_sec))


async def read_snapshot(
    redis_client: redis_async.Redis, *, slug: str,
) -> dict[str, Any] | None:
    """GET + JSON-parse. Returns None on miss/parse-error (defensive)."""
    key = f"{SNAPSHOT_KEY_PREFIX}{slug}"
    try:
        raw = await redis_client.get(key)
    except Exception:
        log.exception("quality_snapshot.read_failed slug=%s", slug)
        return None
    if not raw:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="replace")
    try:
        out = json.loads(raw)
    except (json.JSONDecodeError, TypeError, ValueError):
        log.warning("quality_snapshot.parse_failed slug=%s", slug)
        return None
    return out if isinstance(out, dict) else None


async def writer_loop(
    pool: asyncpg.Pool,
    redis_client: redis_async.Redis,
    *,
    slugs: list[str],
    interval_sec: int,
    ttl_sec: int,
    window_days: int,
    stop: asyncio.Event,
) -> None:
    """Forever loop: every interval, recompute + publish each slug.

    Defensive per-slug: a SQL error on one strategy doesn't kill the
    others. Loop logs and continues.
    """
    log.info(
        "quality_snapshot.writer_started slugs=%s interval=%ds ttl=%ds window=%dd",
        slugs, interval_sec, ttl_sec, window_days,
    )
    while not stop.is_set():
        for slug in slugs:
            try:
                snap = await compute_snapshot(
                    pool, slug=slug, window_days=window_days,
                )
                await write_snapshot(
                    redis_client, slug=slug, snapshot=snap, ttl_sec=ttl_sec,
                )
                log.debug(
                    "quality_snapshot.published slug=%s n=%d sharpe=%s pnl=%.2f",
                    slug, snap["n_closed"], snap["sharpe"], snap["total_pnl_usd"],
                )
            except Exception:
                log.exception("quality_snapshot.compute_failed slug=%s", slug)
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval_sec)
        except TimeoutError:
            pass
    log.info("quality_snapshot.writer_stopped")


__all__ = [
    "SNAPSHOT_KEY_PREFIX",
    "compute_snapshot",
    "compute_stats_from_rows",
    "fetch_recent_closes",
    "format_quality_footer",
    "passes_quality_gate",
    "read_snapshot",
    "sharpe_proxy",
    "write_snapshot",
    "writer_loop",
]
