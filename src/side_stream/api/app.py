"""FastAPI app + routes — trigger CRUD + Whop/Stripe webhooks + admin + health."""
from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager
from typing import Any

import asyncpg
import redis.asyncio as redis_async
from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, EmailStr, Field, field_validator

from side_stream import observability
from side_stream.settings import settings
from side_stream.stripe_webhooks import (
    normalize_stripe_event,
    stripe_price_to_tier,
)
from side_stream.webhook_security import (
    verify_stripe_signature,
    verify_whop_signature,
)

log = logging.getLogger(__name__)


# ─── Pydantic request/response models ──────────────────────────────────


class TriggerCreate(BaseModel):
    """Request body for POST /v1/triggers."""
    user_email: EmailStr
    asset_alias: str = Field(min_length=1, max_length=20)
    rule_kind: str = Field(pattern="^(crosses_above|crosses_below|either)$")
    threshold_usd: float = Field(gt=0)
    cooldown_seconds: int = Field(default=300, ge=0, le=86_400)
    deliver_webhook: bool = False
    webhook_url: str | None = None
    deliver_discord: bool = False
    discord_webhook_url: str | None = None
    deliver_slack: bool = False
    slack_webhook_url: str | None = None
    deliver_email: bool = False
    email_address: str | None = None
    label: str | None = Field(default=None, max_length=200)

    @field_validator("asset_alias")
    @classmethod
    def _lower_alias(cls, v: str) -> str:
        return v.lower().strip()

    def has_any_delivery(self) -> bool:
        return any([
            self.deliver_webhook, self.deliver_discord,
            self.deliver_slack, self.deliver_email,
        ])


class TriggerOut(BaseModel):
    id: str
    asset_alias: str
    rule_kind: str
    threshold_usd: float
    cooldown_seconds: int
    label: str | None
    is_active: bool
    last_fired_at_unix: float | None


class WhopWebhookEvent(BaseModel):
    """Minimal Whop webhook schema — Whop sends many event types; we only
    care about subscription.created / subscription.canceled / payment.failed.

    Real Whop payloads are richer; this captures the fields we use.
    """
    event_type: str
    user_id: str | None = None
    product_id: str | None = None
    subscription_id: str | None = None
    user_email: EmailStr | None = None


# ─── Lifespan: open + close Postgres pool ──────────────────────────────


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("api.starting payment_mode=%s", settings.payment_mode)
    # A7 — Sentry init. No-op when DSN empty (dev/test) or sentry-sdk absent.
    observability.init_sentry(
        dsn=settings.sentry_dsn,
        environment=settings.sentry_environment,
        traces_sample_rate=settings.sentry_traces_sample_rate,
        component="side-stream-api",
    )
    app.state.pg_pool = await asyncpg.create_pool(
        settings.postgres_dsn, min_size=1, max_size=5,
    )
    # Redis client used by /v1/admin/pause + /v1/admin/resume to flip
    # publishing:halts membership. signal_pusher reads the same set.
    app.state.redis_client = redis_async.from_url(
        settings.redis_url, decode_responses=True,
    )
    try:
        yield
    finally:
        await app.state.redis_client.aclose()
        await app.state.pg_pool.close()
        log.info("api.stopped")


app = FastAPI(
    title=f"{settings.brand_name} API",
    version="0.1.0",
    lifespan=lifespan,
)


# ─── Health ────────────────────────────────────────────────────────────


@app.get("/health")
async def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "brand": settings.brand_name,
        "payment_mode": settings.payment_mode,
        "telegram_mock_mode": settings.telegram_mock_mode,
    }


# ─── Trigger CRUD ──────────────────────────────────────────────────────


def _tier_to_max_triggers(tier: str | None) -> int:
    """Per-tier ceiling on active triggers per user.

    3-SKU model post 2026-05-20 eval-doc prune (was 5 tiers).
    """
    return {
        "free": 0,           # Free tier: TG only, no custom triggers
        "standard": 100,     # $29 founding / $49 standard
        "pro": 500,          # $99 — bundles GMX liq alerts + news triggers
    }.get(tier or "", 0)


async def _get_or_create_user(pool: asyncpg.Pool, email: str) -> dict[str, Any]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id FROM users WHERE LOWER(email) = LOWER($1)", email,
        )
        if row is not None:
            return {"id": str(row["id"]), "email": email}
        # Create + return
        row = await conn.fetchrow(
            "INSERT INTO users (email) VALUES ($1) RETURNING id", email,
        )
        return {"id": str(row["id"]), "email": email}


async def _get_active_tier(pool: asyncpg.Pool, user_id: str) -> str | None:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """SELECT tier FROM subscriptions
               WHERE user_id = $1 AND status = 'active'
               ORDER BY CASE tier
                  WHEN 'pro' THEN 3
                  WHEN 'standard' THEN 2
                  WHEN 'free' THEN 1
                  ELSE 0 END DESC
               LIMIT 1""",
            user_id,
        )
    return str(row["tier"]) if row else None


@app.post("/v1/triggers", response_model=TriggerOut)
async def create_trigger(req: TriggerCreate, http_req: Request) -> dict[str, Any]:
    if not req.has_any_delivery():
        raise HTTPException(400, "at least one delivery channel required")
    pool: asyncpg.Pool = http_req.app.state.pg_pool
    user = await _get_or_create_user(pool, req.user_email)
    user_id = user["id"]

    # Entitlement gate — paid tiers can create triggers; free cannot.
    tier = await _get_active_tier(pool, user_id)
    if tier is None and settings.payment_mode != "none":
        raise HTTPException(402, "active subscription required")
    if settings.payment_mode != "none":
        cap = _tier_to_max_triggers(tier)
        # Count existing active triggers
        async with pool.acquire() as conn:
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM triggers WHERE user_id = $1 AND is_active = TRUE",
                user_id,
            )
        if count >= cap:
            raise HTTPException(
                403,
                f"tier '{tier}' allows {cap} active triggers; you have {count}",
            )

    # Validate delivery URLs are present for enabled channels
    if req.deliver_webhook and not req.webhook_url:
        raise HTTPException(400, "webhook_url required when deliver_webhook=true")
    if req.deliver_discord and not req.discord_webhook_url:
        raise HTTPException(400, "discord_webhook_url required when deliver_discord=true")
    if req.deliver_slack and not req.slack_webhook_url:
        raise HTTPException(400, "slack_webhook_url required when deliver_slack=true")
    if req.deliver_email and not req.email_address:
        raise HTTPException(400, "email_address required when deliver_email=true")

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO triggers
              (user_id, asset_alias, rule_kind, threshold_usd, cooldown_seconds,
               deliver_webhook, webhook_url,
               deliver_discord, discord_webhook_url,
               deliver_slack, slack_webhook_url,
               deliver_email, email_address, label)
            VALUES ($1, $2, $3, $4, $5,
                    $6, $7, $8, $9, $10, $11, $12, $13, $14)
            RETURNING id, asset_alias, rule_kind, threshold_usd, cooldown_seconds,
                      label, is_active,
                      COALESCE(EXTRACT(EPOCH FROM last_fired_at), 0) AS last_fired_at_unix
            """,
            user_id, req.asset_alias, req.rule_kind, req.threshold_usd,
            req.cooldown_seconds,
            req.deliver_webhook, req.webhook_url,
            req.deliver_discord, req.discord_webhook_url,
            req.deliver_slack, req.slack_webhook_url,
            req.deliver_email, req.email_address, req.label,
        )
    return {
        "id": str(row["id"]),
        "asset_alias": row["asset_alias"],
        "rule_kind": row["rule_kind"],
        "threshold_usd": float(row["threshold_usd"]),
        "cooldown_seconds": int(row["cooldown_seconds"]),
        "label": row["label"],
        "is_active": bool(row["is_active"]),
        "last_fired_at_unix": float(row["last_fired_at_unix"]) or None,
    }


@app.get("/v1/triggers")
async def list_triggers(user_email: str, http_req: Request) -> list[dict[str, Any]]:
    pool: asyncpg.Pool = http_req.app.state.pg_pool
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT t.id, t.asset_alias, t.rule_kind, t.threshold_usd,
                      t.cooldown_seconds, t.label, t.is_active,
                      COALESCE(EXTRACT(EPOCH FROM t.last_fired_at), 0) AS last_fired_at_unix
               FROM triggers t JOIN users u ON u.id = t.user_id
               WHERE LOWER(u.email) = LOWER($1)
               ORDER BY t.created_at DESC""",
            user_email,
        )
    return [
        {
            "id": str(r["id"]),
            "asset_alias": r["asset_alias"],
            "rule_kind": r["rule_kind"],
            "threshold_usd": float(r["threshold_usd"]),
            "cooldown_seconds": int(r["cooldown_seconds"]),
            "label": r["label"],
            "is_active": bool(r["is_active"]),
            "last_fired_at_unix": float(r["last_fired_at_unix"]) or None,
        }
        for r in rows
    ]


@app.delete("/v1/triggers/{trigger_id}")
async def delete_trigger(trigger_id: str, http_req: Request) -> dict[str, str]:
    pool: asyncpg.Pool = http_req.app.state.pg_pool
    async with pool.acquire() as conn:
        result = await conn.execute(
            "UPDATE triggers SET is_active = FALSE WHERE id = $1", trigger_id,
        )
    if "UPDATE 0" in result:
        raise HTTPException(404, "trigger not found")
    return {"status": "deactivated"}


# ─── Whop webhook stub ─────────────────────────────────────────────────


_WHOP_PRODUCT_TIER_MAP: dict[str, str] = {}


def _refresh_whop_map() -> None:
    """Build the product_id → tier lookup from settings.

    3-SKU model. Founding ($29) + Standard ($49) both map to tier='standard'
    — same access, different price. Founding is a launch-only urgency SKU
    capped at 50 lifetime (kill-list rule 4, eval doc 2026-05-20).
    """
    global _WHOP_PRODUCT_TIER_MAP
    _WHOP_PRODUCT_TIER_MAP = {}
    if settings.whop_founding_product_id:
        _WHOP_PRODUCT_TIER_MAP[settings.whop_founding_product_id] = "standard"
    if settings.whop_standard_product_id:
        _WHOP_PRODUCT_TIER_MAP[settings.whop_standard_product_id] = "standard"
    if settings.whop_pro_product_id:
        _WHOP_PRODUCT_TIER_MAP[settings.whop_pro_product_id] = "pro"


_refresh_whop_map()


@app.post("/v1/webhooks/whop")
async def whop_webhook(
    http_req: Request,
    x_whop_signature: str = Header(default=""),
) -> dict[str, str]:
    """Whop subscription events → users + subscriptions table updates.

    HMAC-SHA256 signature gate: X-Whop-Signature header must match
    HMAC(whop_webhook_secret, raw_body). Forgery attempt = 401.
    """
    pool: asyncpg.Pool = http_req.app.state.pg_pool

    # Read raw body BEFORE parsing — signature is computed on the raw bytes
    raw_body = await http_req.body()
    verification = verify_whop_signature(
        body=raw_body,
        signature_header=x_whop_signature,
        secret=settings.whop_webhook_secret,
    )
    if not verification.valid:
        log.warning("whop.signature_invalid reason=%s", verification.reason)
        raise HTTPException(401, f"signature: {verification.reason}")

    try:
        event = WhopWebhookEvent(**json.loads(raw_body))
    except (json.JSONDecodeError, TypeError, ValueError) as e:
        raise HTTPException(400, f"invalid_body: {e}") from None

    et = event.event_type
    if et not in (
        "subscription.created", "subscription.renewed",
        "subscription.canceled", "payment.failed",
    ):
        # Ack unknown events but no-op (Whop expects 2xx)
        return {"status": "ignored"}

    if event.product_id is None or event.user_email is None:
        raise HTTPException(400, "product_id + user_email required")

    tier = _WHOP_PRODUCT_TIER_MAP.get(event.product_id)
    if tier is None:
        # Unknown product — log + ack
        log.warning("whop.unknown_product product_id=%s", event.product_id)
        return {"status": "unknown_product"}

    user = await _get_or_create_user(pool, event.user_email)
    user_id = user["id"]
    if event.user_id:
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE users SET whop_user_id = $1 WHERE id = $2",
                event.user_id, user_id,
            )

    new_status = "active"
    if et == "subscription.canceled":
        new_status = "canceled"
    elif et == "payment.failed":
        new_status = "past_due"

    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO subscriptions
              (user_id, tier, status, payment_provider, payment_provider_sub_id)
            VALUES ($1, $2, $3, 'whop', $4)
            ON CONFLICT (user_id, tier, status) DO UPDATE
              SET updated_at = NOW(),
                  payment_provider_sub_id = EXCLUDED.payment_provider_sub_id
            """,
            user_id, tier, new_status, event.subscription_id,
        )
    return {"status": "applied", "tier": tier, "subscription_status": new_status}


# ─── Stripe webhook ────────────────────────────────────────────────────


def _stripe_price_tier_map_from_settings() -> dict[str, str]:
    """Parse settings.stripe_price_tier_map_csv → dict[price_id, tier].

    Format: "price_1Abc=standard,price_2Def=pro"
    """
    out: dict[str, str] = {}
    raw = settings.stripe_price_tier_map_csv or ""
    for part in raw.split(","):
        if "=" not in part:
            continue
        price_id, _, tier = part.partition("=")
        price_id = price_id.strip()
        tier = tier.strip()
        if price_id and tier:
            out[price_id] = tier
    return out


@app.post("/v1/webhooks/stripe")
async def stripe_webhook(
    http_req: Request,
    stripe_signature: str = Header(default=""),
) -> dict[str, str]:
    """Stripe webhook handler.

    Signature gate: Stripe-Signature header (t=<ts>,v1=<sig>) must
    verify against HMAC(stripe_webhook_secret, '<ts>.<body>') AND
    timestamp within ±5min tolerance (replay defense).
    """
    pool: asyncpg.Pool = http_req.app.state.pg_pool

    raw_body = await http_req.body()
    verification = verify_stripe_signature(
        body=raw_body,
        signature_header=stripe_signature,
        secret=settings.stripe_webhook_secret,
    )
    if not verification.valid:
        log.warning("stripe.signature_invalid reason=%s", verification.reason)
        raise HTTPException(401, f"signature: {verification.reason}")

    try:
        event_json = json.loads(raw_body)
    except (json.JSONDecodeError, TypeError, ValueError) as e:
        raise HTTPException(400, f"invalid_body: {e}") from None

    normalized = normalize_stripe_event(event_json)
    if normalized is None:
        # Unknown / unhandled event type — ack so Stripe stops retrying
        return {"status": "ignored"}

    price_tier_map = _stripe_price_tier_map_from_settings()
    tier = stripe_price_to_tier(
        price_id=normalized.price_id, price_tier_map=price_tier_map,
    )
    if tier is None:
        log.warning(
            "stripe.unknown_price price_id=%s event=%s",
            normalized.price_id, normalized.event_type,
        )
        return {"status": "unknown_price"}

    if not normalized.customer_email:
        # Subscription events sometimes omit email — we can't link without it
        log.warning(
            "stripe.no_customer_email event=%s sub=%s",
            normalized.event_type, normalized.subscription_id,
        )
        return {"status": "no_email"}

    user = await _get_or_create_user(pool, normalized.customer_email)
    user_id = user["id"]
    async with pool.acquire() as conn:
        # Optional: store stripe customer_id on the user row for later lookups
        if normalized.customer_id:
            await conn.execute(
                "UPDATE users SET stripe_customer_id = $1 WHERE id = $2",
                normalized.customer_id, user_id,
            )
        await conn.execute(
            """
            INSERT INTO subscriptions
              (user_id, tier, status, payment_provider, payment_provider_sub_id)
            VALUES ($1, $2, $3, 'stripe', $4)
            ON CONFLICT (user_id, tier, status) DO UPDATE
              SET updated_at = NOW(),
                  payment_provider_sub_id = EXCLUDED.payment_provider_sub_id
            """,
            user_id, tier, normalized.new_status, normalized.subscription_id,
        )
    return {
        "status": "applied",
        "tier": tier,
        "subscription_status": normalized.new_status,
    }


# ─── Admin endpoints ────────────────────────────────────────────────


def _require_admin(authorization: str) -> None:
    """Raise 401 unless the Authorization: Bearer header matches the
    configured admin_bearer_token. Empty token in settings = always 401
    (default-deny so accidentally-exposed endpoints don't leak)."""
    if not settings.admin_bearer_token:
        raise HTTPException(401, "admin_disabled")
    expected = f"Bearer {settings.admin_bearer_token}"
    if authorization != expected:
        raise HTTPException(401, "invalid_token")


@app.get("/v1/admin/summary")
async def admin_summary(
    http_req: Request,
    authorization: str = Header(default=""),
) -> dict[str, Any]:
    """Top-level operator dashboard: user/subscription/revenue counts."""
    _require_admin(authorization)
    pool: asyncpg.Pool = http_req.app.state.pg_pool
    async with pool.acquire() as conn:
        users = await conn.fetchval("SELECT COUNT(*) FROM users")
        subs_by_tier = await conn.fetch(
            """SELECT tier, status, COUNT(*) AS n
               FROM subscriptions GROUP BY tier, status""",
        )
        active_triggers = await conn.fetchval(
            "SELECT COUNT(*) FROM triggers WHERE is_active = TRUE",
        )
        broadcasts_24h = await conn.fetchval(
            """SELECT COUNT(*) FROM broadcast_logs
               WHERE created_at > NOW() - INTERVAL '24 hours'""",
        )
    # Compute MRR estimate from active subs.
    # 3-SKU model. Founding $29 cohort (capped at 50) is undercounted here
    # by $20 × n_founding/mo — acceptable rough estimate; precise MRR
    # would require storing price_paid on the subscription row.
    tier_prices = {
        "standard": 49.0,
        "pro": 99.0,
    }
    mrr = 0.0
    by_tier_status: dict[str, dict[str, int]] = {}
    for r in subs_by_tier:
        tier = r["tier"]
        status = r["status"]
        n = int(r["n"])
        by_tier_status.setdefault(tier, {})[status] = n
        if status == "active":
            mrr += tier_prices.get(tier, 0.0) * n
    return {
        "users_total": int(users or 0),
        "subscriptions_by_tier_status": by_tier_status,
        "active_triggers": int(active_triggers or 0),
        "broadcasts_24h": int(broadcasts_24h or 0),
        "estimated_mrr_usd": round(mrr, 2),
    }


@app.get("/v1/admin/users")
async def admin_list_users(
    http_req: Request,
    limit: int = 50,
    authorization: str = Header(default=""),
) -> list[dict[str, Any]]:
    """List recent users with their highest active tier."""
    _require_admin(authorization)
    pool: asyncpg.Pool = http_req.app.state.pg_pool
    limit = max(1, min(limit, 500))
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT u.id, u.email, u.created_at,
                   (
                     SELECT s.tier FROM subscriptions s
                     WHERE s.user_id = u.id AND s.status = 'active'
                     ORDER BY CASE s.tier
                       WHEN 'pro' THEN 3
                       WHEN 'standard' THEN 2
                       WHEN 'free' THEN 1
                       ELSE 0 END DESC
                     LIMIT 1
                   ) AS active_tier
            FROM users u
            ORDER BY u.created_at DESC
            LIMIT $1
            """,
            limit,
        )
    return [
        {
            "id": str(r["id"]),
            "email": r["email"],
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            "active_tier": r["active_tier"],
        }
        for r in rows
    ]


@app.get("/v1/admin/recent-broadcasts")
async def admin_recent_broadcasts(
    http_req: Request,
    limit: int = 50,
    authorization: str = Header(default=""),
) -> list[dict[str, Any]]:
    """Last N broadcasts across all channels — for ops eyeball."""
    _require_admin(authorization)
    pool: asyncpg.Pool = http_req.app.state.pg_pool
    limit = max(1, min(limit, 500))
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT signal_source, signal_id, channel, delivered,
                      delivery_error, created_at
               FROM broadcast_logs
               ORDER BY created_at DESC LIMIT $1""",
            limit,
        )
    return [
        {
            "signal_source": r["signal_source"],
            "signal_id": r["signal_id"],
            "channel": r["channel"],
            "delivered": bool(r["delivered"]),
            "delivery_error": r["delivery_error"],
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
        }
        for r in rows
    ]


# ─── Publishing halt control (A1 — eval doc 2026-05-20) ────────────────


VALID_PAUSE_SOURCES: set[str] = {"chainlink_lag", "gmx_liquidation", "all"}


@app.post("/v1/admin/pause")
async def admin_pause_publishing(
    http_req: Request,
    source: str = "all",
    authorization: str = Header(default=""),
) -> dict[str, Any]:
    """Operator pause — SADD into publishing:halts. Stops signal_pusher
    broadcasting for the given source (or 'all').

    Independent of strategy:halts (which cascades from trading-side halts).
    Either set membership halts broadcasts; this gives operators a
    publishing-only override without touching the trading book.
    """
    _require_admin(authorization)
    if source not in VALID_PAUSE_SOURCES:
        raise HTTPException(
            400,
            f"source must be one of {sorted(VALID_PAUSE_SOURCES)}",
        )
    redis_client = http_req.app.state.redis_client
    await redis_client.sadd("publishing:halts", source)
    log.warning("admin.publishing_paused source=%s", source)
    return {"status": "paused", "source": source}


@app.post("/v1/admin/resume")
async def admin_resume_publishing(
    http_req: Request,
    source: str = "all",
    authorization: str = Header(default=""),
) -> dict[str, Any]:
    """Operator resume — SREM from publishing:halts. Re-enables broadcasts.

    Does NOT clear strategy:halts (those are managed by the trading
    halt path — the right way to resume a trading-halted strategy is
    via that strategy's own resume procedure).
    """
    _require_admin(authorization)
    if source not in VALID_PAUSE_SOURCES:
        raise HTTPException(
            400,
            f"source must be one of {sorted(VALID_PAUSE_SOURCES)}",
        )
    redis_client = http_req.app.state.redis_client
    removed = await redis_client.srem("publishing:halts", source)
    log.warning("admin.publishing_resumed source=%s removed=%d", source, removed)
    return {"status": "resumed", "source": source, "was_paused": bool(removed)}


@app.get("/v1/admin/publishing-status")
async def admin_publishing_status(
    http_req: Request,
    authorization: str = Header(default=""),
) -> dict[str, Any]:
    """Inspect current halt state across both halt layers.

    Returns both publishing:halts (operator-controlled) and strategy:halts
    (trading-cascade) so an operator can see "why isn't it publishing"
    at a glance.
    """
    _require_admin(authorization)
    redis_client = http_req.app.state.redis_client
    publishing = set(await redis_client.smembers("publishing:halts"))
    strategy = set(await redis_client.smembers("strategy:halts"))
    # Map broadcast 'source' → upstream strategy slug for the blocked-for view
    src_to_slug = {
        "chainlink_lag": "chainlink_lag",
        "gmx_liquidation": "gmx_liquidator",
    }
    blocked = [
        source for source, slug in src_to_slug.items()
        if "all" in publishing or source in publishing or slug in strategy
    ]
    return {
        "publishing_halts": sorted(publishing),
        "strategy_halts": sorted(strategy),
        "broadcasts_currently_blocked_for": blocked,
    }


__all__ = ["app"]
