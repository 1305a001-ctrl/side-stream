-- Streams Edge — Postgres schema
-- Idempotent: CREATE TABLE IF NOT EXISTS + CREATE INDEX IF NOT EXISTS
-- Apply with: psql -d sidestream -f db/schema.sql

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ─── Users ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS users (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email           TEXT UNIQUE NOT NULL,
    telegram_id     BIGINT UNIQUE,              -- optional, for TG-only flows
    whop_user_id    TEXT UNIQUE,                -- nullable; populated on Whop webhook
    stripe_customer_id TEXT UNIQUE,             -- nullable
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS users_email_idx ON users (LOWER(email));
CREATE INDEX IF NOT EXISTS users_telegram_idx ON users (telegram_id) WHERE telegram_id IS NOT NULL;

-- ─── Subscriptions ────────────────────────────────────────────────────
-- One row per active SKU per user. tier mirrors the pricing tiers.
-- status='active' | 'past_due' | 'canceled' | 'expired'
CREATE TABLE IF NOT EXISTS subscriptions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    tier            TEXT NOT NULL CHECK (
        -- 3-SKU model (eval doc 2026-05-20 prune): free / standard / pro.
        -- Founding ($29) and Standard ($49) both write tier='standard'.
        tier IN ('free', 'standard', 'pro')
    ),
    status          TEXT NOT NULL DEFAULT 'active'
                    CHECK (status IN ('active', 'past_due', 'canceled', 'expired')),
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    current_period_end TIMESTAMPTZ,             -- Whop / Stripe billing cycle
    cancel_at_period_end BOOLEAN NOT NULL DEFAULT FALSE,
    -- Whop or Stripe subscription identifier — kept opaque
    payment_provider TEXT NOT NULL CHECK (payment_provider IN ('whop', 'stripe', 'manual')),
    payment_provider_sub_id TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- A user can have ONE active subscription per tier. Higher tiers
    -- supersede lower (enforced in application layer).
    UNIQUE (user_id, tier, status)
);

CREATE INDEX IF NOT EXISTS subscriptions_user_active_idx
    ON subscriptions (user_id, status) WHERE status = 'active';
CREATE INDEX IF NOT EXISTS subscriptions_provider_sub_idx
    ON subscriptions (payment_provider, payment_provider_sub_id);

-- ─── Trigger rules (Pro Alerts tier) ──────────────────────────────────
-- Each row = one price-cross trigger configured by a user.
-- Active rules are loaded into trigger_engine memory every 30s.
CREATE TABLE IF NOT EXISTS triggers (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    asset_alias     TEXT NOT NULL,              -- 'btc', 'eth', etc.
    -- 'crosses_above' fires when price > threshold; 'crosses_below' fires
    -- when price < threshold; 'either' fires on any crossing of threshold.
    rule_kind       TEXT NOT NULL CHECK (rule_kind IN ('crosses_above', 'crosses_below', 'either')),
    threshold_usd   DOUBLE PRECISION NOT NULL CHECK (threshold_usd > 0),
    -- Cooldown after a fire — prevents flapping on prices oscillating around threshold.
    cooldown_seconds INTEGER NOT NULL DEFAULT 300 CHECK (cooldown_seconds >= 0),
    last_fired_at   TIMESTAMPTZ,                -- updated when trigger fires
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,

    -- Delivery channels (multi-select; at least one required).
    deliver_webhook BOOLEAN NOT NULL DEFAULT FALSE,
    webhook_url     TEXT,                        -- required when deliver_webhook=true
    deliver_discord BOOLEAN NOT NULL DEFAULT FALSE,
    discord_webhook_url TEXT,
    deliver_slack   BOOLEAN NOT NULL DEFAULT FALSE,
    slack_webhook_url TEXT,
    deliver_email   BOOLEAN NOT NULL DEFAULT FALSE,
    email_address   TEXT,

    -- User-friendly label for the trigger.
    label           TEXT,

    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- At least one delivery channel.
    CONSTRAINT triggers_must_have_delivery CHECK (
        deliver_webhook OR deliver_discord OR deliver_slack OR deliver_email
    )
);

CREATE INDEX IF NOT EXISTS triggers_active_by_asset_idx
    ON triggers (asset_alias, is_active) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS triggers_by_user_idx ON triggers (user_id);

-- ─── Delivery logs ────────────────────────────────────────────────────
-- One row per delivery attempt (or non-attempt due to cooldown / disabled).
-- Trimmed to last 30 days via cron job (separate from this schema).
CREATE TABLE IF NOT EXISTS delivery_logs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    trigger_id      UUID NOT NULL REFERENCES triggers(id) ON DELETE CASCADE,
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    channel         TEXT NOT NULL CHECK (channel IN ('webhook', 'discord', 'slack', 'email')),
    status          TEXT NOT NULL CHECK (
        status IN ('sent', 'http_4xx', 'http_5xx', 'timeout', 'network_error', 'skipped_cooldown')
    ),
    response_code   INTEGER,                     -- HTTP status code if applicable
    error_message   TEXT,                        -- truncated to 500 chars in application
    payload         JSONB NOT NULL,
    fired_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS delivery_logs_trigger_idx ON delivery_logs (trigger_id, fired_at DESC);
CREATE INDEX IF NOT EXISTS delivery_logs_user_idx ON delivery_logs (user_id, fired_at DESC);

-- ─── Signal broadcast logs ────────────────────────────────────────────
-- Records every signal broadcast attempt to the free + pro TG channels.
-- Used for analytics + de-dup + churn investigation.
CREATE TABLE IF NOT EXISTS broadcast_logs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    signal_source   TEXT NOT NULL CHECK (signal_source IN ('chainlink_lag', 'tokenized_equity')),
    signal_id       TEXT NOT NULL,               -- the upstream eval_log entry id
    channel         TEXT NOT NULL CHECK (channel IN ('free_telegram', 'pro_telegram')),
    delivered       BOOLEAN NOT NULL DEFAULT FALSE,
    delivery_error  TEXT,
    payload         JSONB NOT NULL,
    broadcast_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Idempotency: don't re-broadcast the same signal to the same channel.
    UNIQUE (signal_source, signal_id, channel)
);

CREATE INDEX IF NOT EXISTS broadcast_logs_source_recent_idx
    ON broadcast_logs (signal_source, broadcast_at DESC);

-- ─── updated_at triggers ──────────────────────────────────────────────
CREATE OR REPLACE FUNCTION update_updated_at_col() RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS users_updated_at ON users;
CREATE TRIGGER users_updated_at BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_col();

DROP TRIGGER IF EXISTS subscriptions_updated_at ON subscriptions;
CREATE TRIGGER subscriptions_updated_at BEFORE UPDATE ON subscriptions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_col();

DROP TRIGGER IF EXISTS triggers_updated_at ON triggers;
CREATE TRIGGER triggers_updated_at BEFORE UPDATE ON triggers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_col();
