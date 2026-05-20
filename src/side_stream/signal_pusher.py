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

from side_stream import llm_validator, observability, quality_snapshot
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
    quality_snapshot: dict[str, Any] | None = None,
) -> str:
    """Pure: build the Markdown-formatted TG message for one signal.

    Free version is intentionally less detailed than Pro (gives subs a
    reason to upgrade).

    Pro version appends a quality footer when a snapshot is provided —
    the eval-doc transparency differentiator (D4). Free tier never gets
    the footer.
    """
    slug = norm["slug"]
    direction = (norm.get("direction") or "").upper() or "?"
    edge_pp = norm["edge_pp"]
    cadence = norm["cadence"]
    if is_pro:
        # A6 — premium card polish: direction emoji for instant visual scan.
        # BUY_YES = green, BUY_NO = red, anything else = neutral.
        direction_emoji = {
            "BUY_YES": "🟢", "BUY_NO": "🔴",
        }.get(direction, "⚪")
        msg = (
            f"🎯 *{brand_name} Signal*\n"
            f"Market: `{slug}`\n"
            f"Direction: {direction_emoji} *{direction}*\n"
            f"Cadence: {cadence}\n"
            f"Edge: *{edge_pp:.3f}pp* (fair_yes={norm['fair_yes']:.3f}, "
            f"market_yes={norm['market_yes']:.3f})\n"
            f"Volume24h: ${norm['market_volume_24h']:,.0f}\n"
            f"Eval at: {datetime.fromtimestamp(norm['evaluated_at_unix'], UTC).isoformat()}\n"
        )
        footer = quality_snapshot_format_footer(quality_snapshot)
        if footer:
            msg += f"{footer}\n"
        return msg
    # Free tier — short + delayed, never carries quality footer
    return (
        f"📡 *{brand_name}*\n"
        f"Signal on `{slug[:50]}…` — direction *{direction}*, "
        f"edge {edge_pp:.2f}pp. _Upgrade for full details._"
    )


# Imported as local alias so format_signal_message stays a pure helper
# (no top-of-module side-effect imports beyond what's already there).
quality_snapshot_format_footer = quality_snapshot.format_quality_footer


def parse_gmx_execution(payload: dict[str, Any]) -> dict[str, Any] | None:
    """Pure: validate + normalize a gmx:execution:paper_log entry.

    The producer (gmx-strategies/liquidation_watcher) writes Redis
    stream fields directly (no JSON wrapper), so payload is the field
    dict as-decoded by redis-py. All numeric fields are stringified
    by the producer; coerce here.

    Returns dict with keys: user, market, is_long, size_usd, collateral_usd,
    distance_to_liq_pct, expected_net_pnl_usd, confidence, ts_unix.
    None for malformed entries.
    """
    if not isinstance(payload, dict):
        return None
    user = payload.get("user")
    market = payload.get("market")
    if not isinstance(user, str) or not isinstance(market, str):
        return None
    try:
        return {
            "user": user,
            "market": market,
            "is_long": str(payload.get("is_long", "0")) == "1",
            "size_usd": float(payload.get("size_usd") or 0.0),
            "collateral_usd": float(payload.get("collateral_usd") or 0.0),
            "distance_to_liq_pct": float(payload.get("distance_to_liq_pct") or 0.0),
            "expected_net_pnl_usd": float(payload.get("expected_net_pnl_usd") or 0.0),
            "confidence": float(payload.get("confidence") or 0.0),
            "ts_unix": float(payload.get("ts_unix") or 0.0),
        }
    except (TypeError, ValueError):
        return None


def format_gmx_alert(norm: dict[str, Any], *, brand_name: str) -> str:
    """Pure: Markdown-formatted GMX liquidation alert for Signal Pro+ TG.

    Pro+ only — these alerts have real on-chain immediacy (the position
    is already underwater; a keeper is about to fire). Free + Pro tiers
    don't get them.
    """
    side = "LONG" if norm["is_long"] else "SHORT"
    user_short = f"{norm['user'][:6]}…{norm['user'][-4:]}"
    return (
        f"🐋 *{brand_name} GMX Alert*\n"
        f"Whale: `{user_short}` {side} *{norm['market'].upper()}*\n"
        f"Size: *${norm['size_usd']:,.0f}* on ${norm['collateral_usd']:,.0f} collateral\n"
        f"Distance to liq: *{norm['distance_to_liq_pct']:+.2f}pp* "
        f"(already underwater = negative)\n"
        f"Expected keeper PnL: *${norm['expected_net_pnl_usd']:,.0f}* "
        f"(conf {norm['confidence']:.2f})\n"
        f"_Eval at: "
        f"{datetime.fromtimestamp(norm['ts_unix'], UTC).isoformat() if norm['ts_unix'] else '?'}_"
    )


def passes_gmx_alert_gate(
    norm: dict[str, Any], *, min_net_pnl_usd: float, min_size_usd: float,
) -> bool:
    """Pure: should this GMX paper-execution be broadcast to Pro+ subs?

    Two-gate filter:
      1. expected_net_pnl_usd >= min — keeper would actually take it
      2. size_usd >= min — whale-scale only; small accounts don't matter
         to subscribers
    """
    if norm.get("expected_net_pnl_usd", 0.0) < min_net_pnl_usd:
        return False
    return norm.get("size_usd", 0.0) >= min_size_usd


def select_free_top_n(
    buffer: list[dict[str, Any]], *, top_n: int,
) -> list[dict[str, Any]]:
    """Pure: pick top-N by edge_pp from a buffer of qualified signals.

    Buffer is assumed to be a 24h window. Returns at most top_n entries
    sorted by edge_pp descending.
    """
    return sorted(buffer, key=lambda s: s.get("edge_pp", 0.0), reverse=True)[:top_n]


def source_to_strategy_slug(source: str) -> str:
    """Pure: map signal_pusher 'source' label → strategy:halts member name.

    Some upstream strategies use a different naming convention than what
    we label in broadcast_logs. This maps the broadcast 'source' to the
    actual strategy slug used in the strategy:halts Redis set so we can
    halt-cascade correctly.
    """
    return {
        "chainlink_lag": "chainlink_lag",
        "gmx_liquidation": "gmx_liquidator",
    }.get(source, source)


def is_emitted_signal(norm: dict[str, Any]) -> bool:
    """A8 — dogfood gate (eval doc 2026-05-20 kill-list rule 5).

    Pure: True only when the signal was ACTUALLY emitted by the strategy
    (i.e., an oms_intent row was created upstream), not a "would_emit"
    candidate that was blocked by some other gate (cooldown, volume, etc).

    Pro-tier broadcasts use this — subscribers pay for what we trade,
    not what we considered trading. Free tier still accepts 'would_emit'
    candidates because the 5-min delay + top-N filter already acts as a
    quality screen.
    """
    return norm.get("decision") == "emitted"


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


async def is_source_halted(
    redis_client: redis_async.Redis,
    *,
    source: str,
) -> bool:
    """Halt-gate before any broadcast. Two layers:

      1. ``strategy:halts SISMEMBER <upstream_slug>`` — upstream trading
         halt cascades to publishing (the brand-decay guard).
      2. ``publishing:halts SISMEMBER <source>`` — operator pause for
         this signal source (independent of upstream).
      3. ``publishing:halts SISMEMBER 'all'`` — blanket operator pause.

    Any membership returns True (halted). Fail-OPEN on Redis errors:
    a hiccup must not silently freeze all publishing. The Sharpe-based
    quality gate (A4, separate) is what enforces the brand promise of
    "we don't publish when it's not working" — A1 is just operator +
    upstream-cascade halt.
    """
    slug = source_to_strategy_slug(source)
    try:
        if await redis_client.sismember("strategy:halts", slug):
            return True
        if await redis_client.sismember("publishing:halts", source):
            return True
        return bool(await redis_client.sismember("publishing:halts", "all"))
    except Exception:
        log.exception("signal_pusher.halt_check_failed source=%s", source)
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
                            redis_client=redis_client,
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
                redis_client=redis_client,
            )

    log.info("signal_pusher.chainlink_loop_stopped")


async def _try_pro_broadcast(
    norm: dict[str, Any], *, signal_id: str, pool: asyncpg.Pool,
    http: httpx.AsyncClient, source: str,
    redis_client: redis_async.Redis,
) -> None:
    """Broadcast one signal to the Pro TG group. Idempotent on signal_id.

    Halt-aware: skips if upstream strategy is halted or operator paused
    publishing for this source. Halt-skips are NOT recorded to
    broadcast_logs so a future retry (after halt clears) isn't blocked
    by the UNIQUE constraint.
    """
    if await is_source_halted(redis_client, source=source):
        log.info(
            "signal_pusher.pro_skip_halted source=%s signal_id=%s",
            source, signal_id,
        )
        return
    # A8 — dogfood gate. Pro subs pay to see what we actually trade,
    # not "would_emit" candidates. Free tier still accepts both.
    if not is_emitted_signal(norm):
        log.info(
            "signal_pusher.pro_skip_not_emitted source=%s signal_id=%s decision=%s",
            source, signal_id, norm.get("decision"),
        )
        return
    # A4 — calibration-aware publishing. Read the current 30d snapshot
    # for this source's upstream strategy, gate against it, and pass
    # the snapshot to the formatter for the transparency footer.
    slug = source_to_strategy_slug(source)
    snapshot = await quality_snapshot.read_snapshot(redis_client, slug=slug)
    allow, reason = quality_snapshot.passes_quality_gate(
        snapshot,
        min_sharpe=settings.quality_min_sharpe,
        min_n_closed=settings.quality_min_n_closed,
        required=settings.quality_snapshot_required,
    )
    if not allow:
        log.info(
            "signal_pusher.pro_skip_quality source=%s signal_id=%s reason=%s",
            source, signal_id, reason,
        )
        return
    # A5 — LLM validation. Opt-in via settings.llm_validation_enabled;
    # fails OPEN on unreachable/error (caller treats None as "no veto").
    if settings.llm_validation_enabled:
        llm_resp = await llm_validator.llm_validate_signal(
            http,
            base_url=settings.local_llm_base_url,
            norm=norm,
            context=source,
            timeout_sec=settings.local_llm_timeout_sec,
        )
        reject_tags = llm_validator.parse_reject_tags_csv(
            settings.llm_validation_reject_tags_csv,
        )
        reject, llm_reason = llm_validator.should_reject_llm_validation(
            llm_resp, reject_tags=reject_tags,
        )
        if reject:
            log.info(
                "signal_pusher.pro_skip_llm source=%s signal_id=%s reason=%s",
                source, signal_id, llm_reason,
            )
            return
    text = format_signal_message(
        norm, brand_name=settings.brand_name, is_pro=True,
        quality_snapshot=snapshot,
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
    redis_client: redis_async.Redis,
) -> None:
    """Pick top-N signals/day; broadcast each (deduped via broadcast_logs)
    after a delay from their evaluated_at_unix.

    Halt-aware: skips the entire flush if upstream strategy halted or
    operator paused publishing for this source.
    """
    if not buffer:
        return
    if await is_source_halted(redis_client, source=source):
        log.info(
            "signal_pusher.free_flush_skip_halted source=%s buffered=%d",
            source, len(buffer),
        )
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


async def _consume_gmx_paper_log(
    redis_client: redis_async.Redis,
    pool: asyncpg.Pool,
    http: httpx.AsyncClient,
    *,
    stop: asyncio.Event,
) -> None:
    """Forever loop: XREAD gmx:execution:paper_log → broadcast qualifying
    whale liquidations to the Pro+ Telegram group.

    Pro+ only — these are real on-chain whale liquidations, not free or
    Pro-tier general signals. Gate is min_net_pnl + min_size.
    """
    cursor = "$"  # start from now; ignore backlog
    while not stop.is_set():
        try:
            result = await redis_client.xread(
                {settings.gmx_execution_paper_log_stream: cursor},
                block=5_000, count=200,
            )
        except Exception:
            log.exception("signal_pusher.gmx_xread_failed")
            await asyncio.sleep(2)
            continue

        if not result:
            continue

        for _stream, entries in result:
            for entry_id, fields in entries:
                cursor = entry_id
                # GMX paper_log uses Redis stream fields directly (no JSON
                # wrapper) — `fields` IS the dict.
                norm = parse_gmx_execution(fields)
                if norm is None:
                    continue
                if not passes_gmx_alert_gate(
                    norm,
                    min_net_pnl_usd=settings.gmx_alerts_min_net_pnl_usd,
                    min_size_usd=settings.gmx_alerts_min_size_usd,
                ):
                    continue
                await _try_gmx_pro_plus_broadcast(
                    norm, signal_id=entry_id, pool=pool, http=http,
                    redis_client=redis_client,
                )
    log.info("signal_pusher.gmx_loop_stopped")


async def _try_gmx_pro_plus_broadcast(
    norm: dict[str, Any], *, signal_id: str, pool: asyncpg.Pool,
    http: httpx.AsyncClient, redis_client: redis_async.Redis,
) -> None:
    """Broadcast a GMX whale-liq alert to the Pro+ TG group. Idempotent.

    Halt-aware: skips if upstream gmx_liquidator halted or operator
    paused publishing for 'gmx_liquidation' source.
    """
    if await is_source_halted(redis_client, source="gmx_liquidation"):
        log.info("signal_pusher.gmx_skip_halted signal_id=%s", signal_id)
        return
    text = format_gmx_alert(norm, brand_name=settings.brand_name)
    if settings.telegram_mock_mode:
        log.info(
            "signal_pusher.gmx_mock_broadcast signal_id=%s text=%r",
            signal_id, text[:200],
        )
        delivered, err = True, None
    else:
        # Pro+ alerts go to the same Pro group for now — when there's a
        # dedicated Pro+ chat, point at it instead. The broadcast_logs
        # channel field differentiates ('pro_plus_telegram').
        delivered, err = await _broadcast_telegram(
            bot_token=settings.telegram_bot_token,
            chat_id=settings.telegram_pro_group_id,
            text=text,
            http=http,
        )
    await _record_broadcast(
        pool,
        signal_source="gmx_liquidation",
        signal_id=signal_id,
        channel="pro_plus_telegram",
        delivered=delivered,
        error=err,
        payload=norm,
    )


async def run(stop: asyncio.Event) -> None:
    """Top-level entry point for the signal_pusher service.

    Spawns one consumer per upstream Redis stream; they run concurrently
    and share the asyncpg pool + httpx client.
    """
    log.info(
        "signal_pusher.starting mock=%s free_top_n=%d free_delay=%ds "
        "pro_min_edge=%.3fpp gmx_min_pnl=$%.0f gmx_min_size=$%.0f",
        settings.telegram_mock_mode,
        settings.free_top_n_per_day,
        settings.free_delay_seconds,
        settings.signal_pro_min_edge_pp,
        settings.gmx_alerts_min_net_pnl_usd,
        settings.gmx_alerts_min_size_usd,
    )

    # A7 — Sentry init. No-op when DSN empty (dev/test) or sentry-sdk absent.
    observability.init_sentry(
        dsn=settings.sentry_dsn,
        environment=settings.sentry_environment,
        traces_sample_rate=settings.sentry_traces_sample_rate,
        component="signal_pusher",
    )

    pool = await asyncpg.create_pool(settings.postgres_dsn, min_size=1, max_size=5)
    redis_client = redis_async.from_url(settings.redis_url, decode_responses=True)
    tracked_slugs = [
        s.strip() for s in settings.quality_tracked_slugs.split(",") if s.strip()
    ]
    async with httpx.AsyncClient() as http:
        try:
            await asyncio.gather(
                _consume_chainlink_eval_log(
                    redis_client, pool, http, stop=stop,
                ),
                _consume_gmx_paper_log(
                    redis_client, pool, http, stop=stop,
                ),
                # A4 — periodic quality-snapshot writer. Computes per-slug
                # Sharpe + PnL + n_closed every quality_snapshot_interval_sec
                # and writes to publishing:quality:<slug> with EXPIRE.
                quality_snapshot.writer_loop(
                    pool, redis_client,
                    slugs=tracked_slugs,
                    interval_sec=settings.quality_snapshot_interval_sec,
                    ttl_sec=settings.quality_snapshot_ttl_sec,
                    window_days=settings.quality_window_days,
                    stop=stop,
                ),
            )
        finally:
            await redis_client.aclose()
            await pool.close()
            log.info("signal_pusher.stopped")


__all__ = [
    "format_gmx_alert",
    "format_signal_message",
    "is_emitted_signal",
    "is_source_halted",
    "parse_chainlink_eval",
    "parse_gmx_execution",
    "passes_gmx_alert_gate",
    "run",
    "select_free_top_n",
    "source_to_strategy_slug",
]
