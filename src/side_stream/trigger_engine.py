"""trigger_engine — sub-second price-cross trigger evaluation for Pro Alerts.

Subscribes to `chainlink:<alias>:reports` for each monitored asset. On
every report, evaluates all active triggers for that asset. Fires
delivery (via delivery.py) on cross + cooldown not active.

Active rules loaded from Postgres every settings.trigger_reload_interval_sec
into an in-memory index by asset_alias. Index is rebuilt atomically so
mid-cycle updates don't cause inconsistent state.

Capacity: one node with ~10k active triggers across 7 feeds is fine.
Each report → ~lookup-in-dict-bucket → ~10-100 trigger evaluations.
Bottleneck is delivery I/O, not trigger logic.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass

import asyncpg
import httpx
import redis.asyncio as redis_async

from side_stream.delivery import deliver_alert
from side_stream.settings import settings

log = logging.getLogger(__name__)


# ─── Domain types ──────────────────────────────────────────────────────


@dataclass(frozen=True)
class TriggerRule:
    """Immutable snapshot of one trigger rule, loaded from Postgres."""
    id: str                     # UUID as string
    user_id: str
    asset_alias: str            # lowercase
    rule_kind: str              # 'crosses_above' | 'crosses_below' | 'either'
    threshold_usd: float
    cooldown_seconds: int
    last_fired_at_unix: float   # 0.0 if never fired
    # Delivery channels — each is (enabled, target_url_or_address)
    deliver_webhook: bool
    webhook_url: str | None
    deliver_discord: bool
    discord_webhook_url: str | None
    deliver_slack: bool
    slack_webhook_url: str | None
    deliver_email: bool
    email_address: str | None
    label: str | None


@dataclass(frozen=True)
class PriceTick:
    """One Chainlink Data Streams report parsed into the fields we need."""
    asset_alias: str
    benchmark_price_usd: float
    observations_ts_unix: int


# ─── Pure decision logic ───────────────────────────────────────────────


def should_fire_trigger(
    *, rule: TriggerRule, prev_price: float | None, curr_price: float,
    now_unix: float,
) -> bool:
    """Pure: decide whether to fire this trigger on the latest tick.

    Crosses are defined as: the prev tick was on one side of the threshold,
    the current tick is on the other side (strict crossing).

    Cooldown gate: skip if last_fired_at_unix + cooldown_seconds > now.
    """
    if curr_price <= 0:
        return False
    if (now_unix - rule.last_fired_at_unix) < rule.cooldown_seconds:
        return False
    if prev_price is None:
        # First tick — can't detect a cross without a prior. Skip.
        return False
    th = rule.threshold_usd
    crossed_up = prev_price <= th < curr_price
    crossed_dn = prev_price >= th > curr_price
    if rule.rule_kind == "crosses_above":
        return crossed_up
    if rule.rule_kind == "crosses_below":
        return crossed_dn
    if rule.rule_kind == "either":
        return crossed_up or crossed_dn
    return False


def group_rules_by_asset(rules: list[TriggerRule]) -> dict[str, list[TriggerRule]]:
    """Pure: bucket rules into a dict[asset_alias] → list of rules. Used to
    avoid scanning ALL rules on every tick — only the rules for the asset
    that just ticked.
    """
    out: dict[str, list[TriggerRule]] = {}
    for r in rules:
        out.setdefault(r.asset_alias, []).append(r)
    return out


# ─── Async I/O — Postgres loading + Redis subscribing ─────────────────


async def _load_active_rules(pool: asyncpg.Pool) -> list[TriggerRule]:
    """SELECT all active triggers from Postgres. Called every 30s to
    pick up new triggers + remove deactivated ones."""
    sql = """
        SELECT id, user_id, asset_alias, rule_kind, threshold_usd,
               cooldown_seconds,
               COALESCE(EXTRACT(EPOCH FROM last_fired_at), 0) AS last_fired_at_unix,
               deliver_webhook, webhook_url,
               deliver_discord, discord_webhook_url,
               deliver_slack, slack_webhook_url,
               deliver_email, email_address, label
        FROM triggers
        WHERE is_active = TRUE
        ORDER BY id
        LIMIT 100000
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql)
    return [
        TriggerRule(
            id=str(r["id"]),
            user_id=str(r["user_id"]),
            asset_alias=str(r["asset_alias"]).lower(),
            rule_kind=str(r["rule_kind"]),
            threshold_usd=float(r["threshold_usd"]),
            cooldown_seconds=int(r["cooldown_seconds"]),
            last_fired_at_unix=float(r["last_fired_at_unix"]),
            deliver_webhook=bool(r["deliver_webhook"]),
            webhook_url=r["webhook_url"],
            deliver_discord=bool(r["deliver_discord"]),
            discord_webhook_url=r["discord_webhook_url"],
            deliver_slack=bool(r["deliver_slack"]),
            slack_webhook_url=r["slack_webhook_url"],
            deliver_email=bool(r["deliver_email"]),
            email_address=r["email_address"],
            label=r["label"],
        )
        for r in rows
    ]


def _decode_chainlink_report(raw: str) -> PriceTick | None:
    """Parse one chainlink:<alias>:reports stream entry's `data` field."""
    try:
        d = json.loads(raw)
    except (json.JSONDecodeError, TypeError, ValueError):
        return None
    if not isinstance(d, dict):
        return None
    alias = d.get("asset_alias")
    price = d.get("benchmark_price_float64") or d.get("benchmark_price")
    ts = d.get("observations_ts") or d.get("valid_from_ts")
    if not isinstance(alias, str):
        return None
    try:
        return PriceTick(
            asset_alias=alias.lower(),
            benchmark_price_usd=float(price),
            observations_ts_unix=int(ts),
        )
    except (TypeError, ValueError):
        return None


async def _mark_fired(pool: asyncpg.Pool, rule_id: str) -> None:
    """UPDATE triggers SET last_fired_at = NOW() for this rule. Best-effort."""
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE triggers SET last_fired_at = NOW() WHERE id = $1",
                rule_id,
            )
    except Exception:
        log.exception("trigger_engine.mark_fired_failed rule=%s", rule_id)


async def _evaluate_tick(
    *, tick: PriceTick, rules_by_asset: dict[str, list[TriggerRule]],
    last_prices: dict[str, float], pool: asyncpg.Pool, http: httpx.AsyncClient,
) -> None:
    """Run all rules for tick.asset_alias against the new price."""
    prev = last_prices.get(tick.asset_alias)
    now_unix = time.time()
    rules = rules_by_asset.get(tick.asset_alias, [])
    for rule in rules:
        if not should_fire_trigger(
            rule=rule, prev_price=prev, curr_price=tick.benchmark_price_usd,
            now_unix=now_unix,
        ):
            continue
        # Fire — deliver + mark fired. Best-effort; failures are logged.
        await deliver_alert(
            rule=rule, tick=tick, pool=pool, http=http,
        )
        await _mark_fired(pool, rule.id)
    last_prices[tick.asset_alias] = tick.benchmark_price_usd


async def _consume_chainlink_reports(
    redis_client: redis_async.Redis,
    *,
    pool: asyncpg.Pool,
    http: httpx.AsyncClient,
    stop: asyncio.Event,
) -> None:
    """Forever loop: XREAD chainlink:<alias>:reports for monitored assets.

    Reloads rules every settings.trigger_reload_interval_sec to pick up
    new/changed triggers.
    """
    monitored = [
        a.strip().lower()
        for a in settings.monitored_assets.split(",")
        if a.strip()
    ]
    streams = {
        settings.chainlink_report_stream_pattern.format(alias=a): "$"
        for a in monitored
    }

    rules = await _load_active_rules(pool)
    rules_by_asset = group_rules_by_asset(rules)
    log.info(
        "trigger_engine.starting monitored_assets=%s active_rules=%d",
        monitored, len(rules),
    )

    last_prices: dict[str, float] = {}
    last_reload = time.monotonic()

    while not stop.is_set():
        try:
            result = await redis_client.xread(streams, block=5_000, count=50)
        except Exception:
            log.exception("trigger_engine.xread_failed")
            await asyncio.sleep(2)
            continue

        if result:
            for stream_name, entries in result:
                for entry_id, fields in entries:
                    streams[stream_name] = entry_id
                    raw = fields.get("data") or fields.get(b"data")
                    if isinstance(raw, bytes):
                        raw = raw.decode("utf-8", errors="replace")
                    if not raw:
                        continue
                    tick = _decode_chainlink_report(raw)
                    if tick is None:
                        continue
                    await _evaluate_tick(
                        tick=tick, rules_by_asset=rules_by_asset,
                        last_prices=last_prices, pool=pool, http=http,
                    )

        # Periodic rule reload — atomic swap of the index.
        if time.monotonic() - last_reload > settings.trigger_reload_interval_sec:
            try:
                rules = await _load_active_rules(pool)
                rules_by_asset = group_rules_by_asset(rules)
                log.debug("trigger_engine.rules_reloaded n=%d", len(rules))
            except Exception:
                log.exception("trigger_engine.reload_failed")
            last_reload = time.monotonic()

    log.info("trigger_engine.stopped")


async def run(stop: asyncio.Event) -> None:
    """Top-level entry point."""
    pool = await asyncpg.create_pool(settings.postgres_dsn, min_size=1, max_size=5)
    redis_client = redis_async.from_url(settings.redis_url, decode_responses=True)
    async with httpx.AsyncClient() as http:
        try:
            await _consume_chainlink_reports(
                redis_client, pool=pool, http=http, stop=stop,
            )
        finally:
            await redis_client.aclose()
            await pool.close()


__all__ = [
    "PriceTick",
    "TriggerRule",
    "group_rules_by_asset",
    "run",
    "should_fire_trigger",
]
