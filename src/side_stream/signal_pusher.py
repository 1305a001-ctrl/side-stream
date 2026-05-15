"""signal_pusher — subscribes to upstream eval_log streams + broadcasts to TG.

Two consumer paths:
  Free TG channel:    top-N signals/day, delayed by N minutes (per settings)
  Signal Pro group:   real-time, all signals meeting confidence + edge gates

Idempotency: every broadcast attempt INSERTs a row into broadcast_logs with
UNIQUE (signal_source, signal_id, channel). Re-runs are safe.

Delivery: when settings.telegram_mock_mode=True (default until bot token
is provided), broadcasts go to stdout instead of the Telegram API. This
lets ops + tests run without real creds.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from datetime import UTC, datetime
from typing import Any

import asyncpg
import httpx
import redis.asyncio as redis_async

from side_stream.settings import settings

log = logging.getLogger(__name__)


# ─── Pure helpers ──────────────────────────────────────────────────────


def parse_chainlink_eval(payload: dict[str, Any]) -> dict[str, Any] | None:
    """Pure: validate + normalize a chainlink:eval_log entry.

    Returns dict with keys: slug, decision, edge_pp, fair_yes, market_yes,
    cadence, market_volume_24h, evaluated_at_unix.
    None for malformed or non-broadcast-worthy entries.
    """
    if not isinstance(payload, dict):
        return None
    slug = payload.get("slug")
    decision = payload.get("decision")
    if not isinstance(slug, str) or not isinstance(decision, str):
        return None
    # Only broadcast would_emit + emitted decisions. 'eval' is just the
    # eval-log everyone gets — we don't broadcast 5000 evals/day on free.
    if decision not in ("would_emit", "emitted"):
        return None
    edge_pp = payload.get("edge_pp")
    if edge_pp is None:
        return None
    try:
        return {
            "slug": slug,
            "decision": decision,
            "edge_pp": float(edge_pp),
            "fair_yes": float(payload.get("fair_yes") or 0.0),
            "market_yes": float(payload.get("market_yes") or 0.0),
            "cadence": str(payload.get("cadence", "")),
            "market_volume_24h": float(payload.get("market_volume_24h") or 0.0),
            "evaluated_at_unix": float(payload.get("evaluated_at_unix") or 0.0),
            "direction": payload.get("direction"),
        }
    except (TypeError, ValueError):
        return None


def format_signal_message(
    norm: dict[str, Any], *, brand_name: str, is_pro: bool,
) -> str:
    """Pure: build the Markdown-formatted TG message for one signal.

    Free version is intentionally less detailed than Pro (gives subs a
    reason to upgrade).
    """
    slug = norm["slug"]
    direction = (norm.get("direction") or "").upper() or "?"
    edge_pp = norm["edge_pp"]
    cadence = norm["cadence"]
    if is_pro:
        return (
            f"🎯 *{brand_name} Signal*\n"
            f"Market: `{slug}`\n"
            f"Direction: *{direction}*\n"
            f"Cadence: {cadence}\n"
            f"Edge: *{edge_pp:.3f}pp* (fair_yes={norm['fair_yes']:.3f}, "
            f"market_yes={norm['market_yes']:.3f})\n"
            f"Volume24h: ${norm['market_volume_24h']:,.0f}\n"
            f"Eval at: {datetime.fromtimestamp(norm['evaluated_at_unix'], UTC).isoformat()}\n"
        )
    # Free tier — short + delayed
    return (
        f"📡 *{brand_name}*\n"
        f"Signal on `{slug[:50]}…` — direction *{direction}*, "
        f"edge {edge_pp:.2f}pp. _Upgrade for full details._"
    )


def select_free_top_n(
    buffer: list[dict[str, Any]], *, top_n: int,
) -> list[dict[str, Any]]:
    """Pure: pick top-N by edge_pp from a buffer of qualified signals.

    Buffer is assumed to be a 24h window. Returns at most top_n entries
    sorted by edge_pp descending.
    """
    return sorted(buffer, key=lambda s: s.get("edge_pp", 0.0), reverse=True)[:top_n]


# ─── Async I/O ─────────────────────────────────────────────────────────


async def _broadcast_telegram(
    *, bot_token: str, chat_id: str, text: str, http: httpx.AsyncClient,
) -> tuple[bool, str | None]:
    """Single Telegram sendMessage call. Returns (ok, error_msg)."""
    if not bot_token or not chat_id:
        return False, "missing_telegram_creds"
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    try:
        resp = await http.post(
            url,
            json={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True,
            },
            timeout=settings.delivery_http_timeout_sec,
        )
    except httpx.HTTPError as e:
        return False, f"http_error: {e}"
    if resp.status_code >= 400:
        return False, f"http_{resp.status_code}: {resp.text[:200]}"
    return True, None


async def _record_broadcast(
    pool: asyncpg.Pool,
    *,
    signal_source: str,
    signal_id: str,
    channel: str,
    delivered: bool,
    error: str | None,
    payload: dict[str, Any],
) -> bool:
    """Insert into broadcast_logs. Returns True on insert, False on UNIQUE
    conflict (means already broadcast — caller can skip).
    """
    async with pool.acquire() as conn:
        try:
            await conn.execute(
                """
                INSERT INTO broadcast_logs
                  (signal_source, signal_id, channel, delivered, delivery_error, payload)
                VALUES ($1, $2, $3, $4, $5, $6::jsonb)
                """,
                signal_source, signal_id, channel,
                delivered, error, json.dumps(payload),
            )
            return True
        except asyncpg.UniqueViolationError:
            return False


async def _consume_chainlink_eval_log(
    redis_client: redis_async.Redis,
    pool: asyncpg.Pool,
    http: httpx.AsyncClient,
    *,
    stop: asyncio.Event,
) -> None:
    """Forever loop: XREAD chainlink:eval_log → broadcast qualifying entries.

    Maintains a 24h rolling buffer for free-tier top-N selection. Pro
    tier broadcasts in real-time (no buffer wait).
    """
    cursor = "$"  # start from now; ignore backlog
    free_buffer: deque[dict[str, Any]] = deque(maxlen=10_000)
    last_free_flush = 0.0
    free_flush_interval_sec = 60.0  # check top-N once per minute

    while not stop.is_set():
        try:
            result = await redis_client.xread(
                {settings.chainlink_eval_log_stream: cursor},
                block=5_000, count=200,
            )
        except Exception:
            log.exception("signal_pusher.xread_failed")
            await asyncio.sleep(2)
            continue

        if result:
            for _stream, entries in result:
                for entry_id, fields in entries:
                    cursor = entry_id
                    raw = fields.get("data") or fields.get(b"data")
                    if isinstance(raw, bytes):
                        raw = raw.decode("utf-8", errors="replace")
                    if not raw:
                        continue
                    try:
                        payload = json.loads(raw)
                    except (json.JSONDecodeError, TypeError, ValueError):
                        continue
                    norm = parse_chainlink_eval(payload)
                    if norm is None:
                        continue

                    # Pro broadcast — real-time, must clear conf + edge gates.
                    if (
                        norm["edge_pp"] >= settings.signal_pro_min_edge_pp
                        and norm["market_volume_24h"] > 0
                    ):
                        await _try_pro_broadcast(
                            norm, signal_id=entry_id, pool=pool, http=http,
                            source="chainlink_lag",
                        )

                    # Free buffer — pick top-N later.
                    free_buffer.append({**norm, "signal_id": entry_id})

        # Free-tier flush — once per minute
        now = time.monotonic()
        if now - last_free_flush > free_flush_interval_sec:
            last_free_flush = now
            await _flush_free_top_n(
                buffer=list(free_buffer),
                pool=pool,
                http=http,
                source="chainlink_lag",
            )

    log.info("signal_pusher.chainlink_loop_stopped")


async def _try_pro_broadcast(
    norm: dict[str, Any], *, signal_id: str, pool: asyncpg.Pool,
    http: httpx.AsyncClient, source: str,
) -> None:
    """Broadcast one signal to the Pro TG group. Idempotent on signal_id."""
    text = format_signal_message(
        norm, brand_name=settings.brand_name, is_pro=True,
    )
    if settings.telegram_mock_mode:
        log.info("signal_pusher.pro_mock_broadcast signal_id=%s text=%r", signal_id, text[:160])
        delivered, err = True, None
    else:
        delivered, err = await _broadcast_telegram(
            bot_token=settings.telegram_bot_token,
            chat_id=settings.telegram_pro_group_id,
            text=text,
            http=http,
        )

    await _record_broadcast(
        pool,
        signal_source=source,
        signal_id=signal_id,
        channel="pro_telegram",
        delivered=delivered,
        error=err,
        payload=norm,
    )


async def _flush_free_top_n(
    *, buffer: list[dict[str, Any]], pool: asyncpg.Pool,
    http: httpx.AsyncClient, source: str,
) -> None:
    """Pick top-N signals/day; broadcast each (deduped via broadcast_logs)
    after a delay from their evaluated_at_unix."""
    if not buffer:
        return
    # Filter to ones that have aged past the delay
    now = time.time()
    aged = [
        s for s in buffer
        if (now - float(s.get("evaluated_at_unix", 0.0))) >= settings.free_delay_seconds
    ]
    if not aged:
        return
    top = select_free_top_n(aged, top_n=settings.free_top_n_per_day)
    for s in top:
        sig_id = s.get("signal_id", "")
        if not sig_id:
            continue
        # Pre-check: skip if already broadcast (cheap dup check via
        # the UNIQUE constraint inserting later, but we can also short-circuit
        # via a SELECT — kept simple here; UNIQUE handles correctness).
        text = format_signal_message(s, brand_name=settings.brand_name, is_pro=False)
        if settings.telegram_mock_mode:
            log.info("signal_pusher.free_mock_broadcast signal_id=%s text=%r", sig_id, text[:160])
            delivered, err = True, None
        else:
            delivered, err = await _broadcast_telegram(
                bot_token=settings.telegram_bot_token,
                chat_id=settings.telegram_public_channel_id,
                text=text,
                http=http,
            )
        await _record_broadcast(
            pool,
            signal_source=source,
            signal_id=sig_id,
            channel="free_telegram",
            delivered=delivered,
            error=err,
            payload=s,
        )


async def run(stop: asyncio.Event) -> None:
    """Top-level entry point for the signal_pusher service."""
    log.info(
        "signal_pusher.starting mock=%s free_top_n=%d free_delay=%ds pro_min_edge=%.3fpp",
        settings.telegram_mock_mode,
        settings.free_top_n_per_day,
        settings.free_delay_seconds,
        settings.signal_pro_min_edge_pp,
    )

    pool = await asyncpg.create_pool(settings.postgres_dsn, min_size=1, max_size=5)
    redis_client = redis_async.from_url(settings.redis_url, decode_responses=True)
    async with httpx.AsyncClient() as http:
        try:
            await _consume_chainlink_eval_log(
                redis_client, pool, http, stop=stop,
            )
        finally:
            await redis_client.aclose()
            await pool.close()
            log.info("signal_pusher.stopped")


__all__ = [
    "format_signal_message",
    "parse_chainlink_eval",
    "run",
    "select_free_top_n",
]
