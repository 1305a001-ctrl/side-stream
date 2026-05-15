"""FastAPI app + routes — trigger CRUD + Whop/Stripe webhooks + health."""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Any

import asyncpg
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, EmailStr, Field, field_validator

from side_stream.settings import settings

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
    app.state.pg_pool = await asyncpg.create_pool(
        settings.postgres_dsn, min_size=1, max_size=5,
    )
    try:
        yield
    finally:
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
    """Per-tier ceiling on active triggers per user."""
    return {
        "free": 0,                # Free tier doesn't get custom triggers
        "pro_alerts": 100,
        "signal_pro": 100,        # bundled
        "signal_pro_plus": 500,
        "enterprise": 5_000,
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
                  WHEN 'enterprise' THEN 5
                  WHEN 'signal_pro_plus' THEN 4
                  WHEN 'signal_pro' THEN 3
                  WHEN 'pro_alerts' THEN 2
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
    """Build the product_id → tier lookup from settings."""
    global _WHOP_PRODUCT_TIER_MAP
    _WHOP_PRODUCT_TIER_MAP = {}
    if settings.whop_pro_alerts_product_id:
        _WHOP_PRODUCT_TIER_MAP[settings.whop_pro_alerts_product_id] = "pro_alerts"
    if settings.whop_signal_pro_product_id:
        _WHOP_PRODUCT_TIER_MAP[settings.whop_signal_pro_product_id] = "signal_pro"
    if settings.whop_signal_pro_plus_product_id:
        _WHOP_PRODUCT_TIER_MAP[settings.whop_signal_pro_plus_product_id] = "signal_pro_plus"
    if settings.whop_enterprise_product_id:
        _WHOP_PRODUCT_TIER_MAP[settings.whop_enterprise_product_id] = "enterprise"


_refresh_whop_map()


@app.post("/v1/webhooks/whop")
async def whop_webhook(event: WhopWebhookEvent, http_req: Request) -> dict[str, str]:
    """Whop subscription events → users + subscriptions table updates.

    Stub implementation — real Whop sends an Authorization header we
    should verify; add HMAC validation before production.
    """
    pool: asyncpg.Pool = http_req.app.state.pg_pool
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


__all__ = ["app"]
