"""Env-driven settings.

ALL secrets via env — never committed. See README.md for required vars.
"""
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # ─── Brand + identity ──────────────────────────────────────────────
    # Placeholder; override via env on launch.
    brand_name: str = "Streams Edge"
    public_channel_url: str = ""    # https://t.me/streamsedge — set after BotFather
    signal_pro_group_url: str = ""  # https://t.me/+xyz — invite link from Whop

    # ─── Backend connections ───────────────────────────────────────────
    # Defaults assume running on ai-primary alongside existing stack.
    postgres_dsn: str = (
        "postgresql://benadmin:CHANGEME@postgres:5432/sidestream"
    )
    redis_url: str = "redis://redis:6379/0"

    # Existing strategy-runners streams we consume — DO NOT CHANGE without
    # corresponding strategy-runners config update.
    chainlink_eval_log_stream: str = "chainlink:eval_log"
    tokenized_equity_eval_log_stream: str = "tokenized_equity:eval_log"
    gmx_execution_paper_log_stream: str = "gmx:execution:paper_log"

    # Chainlink reports — per-asset key pattern: chainlink:<alias>:reports
    chainlink_report_stream_pattern: str = "chainlink:{alias}:reports"
    monitored_assets: str = "btc,eth,sol,bnb,xrp,doge,hype"

    # ─── Telegram delivery ─────────────────────────────────────────────
    # Bot token from @BotFather — operator provides via env before launch.
    telegram_bot_token: str = ""
    telegram_public_channel_id: str = ""   # @streamsedge_free or chat id like -100xxx
    telegram_pro_group_id: str = ""        # private chat id from Whop

    # Mock mode: when bot token is empty, signal_pusher logs to stdout
    # instead of calling the Telegram API. Lets ops + tests run without
    # real bot creds.
    telegram_mock_mode: bool = True

    # ─── Signal broadcast policy ───────────────────────────────────────
    # Free channel: top-N signals/day, delayed by N minutes
    free_top_n_per_day: int = 3
    free_delay_seconds: int = 300

    # Signal Pro: gating
    signal_pro_min_confidence: float = 0.7    # only broadcast >0.7 confidence
    signal_pro_min_edge_pp: float = 0.03      # only broadcast >3pp edge

    # Signal Pro+ tier: GMX liquidation alerts. Only broadcast paper-log
    # entries with expected net PnL >= this gate AND distance_to_liq <=
    # this many percentage points below zero (i.e., already underwater).
    # The Pro+ value proposition is "the WHALES that are about to be
    # liquidated" — not noise. Defaults derived from the gmx-strategies
    # `execution_min_net_profit_usd` so this matches the producer.
    gmx_alerts_min_net_pnl_usd: float = 500.0
    gmx_alerts_min_size_usd: float = 50_000.0

    # ─── Trigger engine ────────────────────────────────────────────────
    # Re-load active triggers from Postgres every N seconds.
    trigger_reload_interval_sec: int = 30
    # Max triggers per user (Pro Alerts tier sets the actual cap — this is
    # the system-wide safety ceiling).
    trigger_max_per_user: int = 500

    # ─── Delivery channels ─────────────────────────────────────────────
    delivery_http_timeout_sec: float = 10.0
    delivery_max_retries: int = 3
    delivery_backoff_base_sec: float = 2.0

    # Discord delivery — webhook-based, no API key needed (per-user webhook URL).
    # Slack delivery — webhook-based, same pattern.
    # Email delivery — SMTP. Operator provides server + creds via env.
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_username: str = ""
    smtp_password: str = ""
    smtp_from_email: str = "alerts@streamsedge.io"

    # ─── Payment integration (mode switch) ─────────────────────────────
    # 'whop' | 'stripe' | 'none' (in-memory entitlements for dev/test)
    payment_mode: str = "none"
    whop_api_key: str = ""
    whop_webhook_secret: str = ""    # for X-Whop-Signature HMAC verification
    # 3-SKU model (Free / $49 Standard / $99 Pro) — eval doc 2026-05-20:
    # >3 tiers drops Whop conversion 15-20%. Founding $29 is a price SKU
    # within 'standard' (capped at 50 lifetime per kill-list rule 4), not
    # a separate tier — both founding + standard product IDs map to
    # tier='standard'.
    whop_founding_product_id: str = ""   # $29/mo, capped at 50 lifetime
    whop_standard_product_id: str = ""   # $49/mo
    whop_pro_product_id: str = ""        # $99/mo
    stripe_api_key: str = ""
    stripe_webhook_secret: str = ""
    # Stripe price_id → tier map. Operator sets one entry per active
    # price in their Stripe account. Example value:
    #   "price_1Abc...=standard,price_2Def...=pro"
    stripe_price_tier_map_csv: str = ""

    # ─── LLM validation (A5 — eval doc 2026-05-20 D5) ──────────────────
    # ai-edge local-llm v0.1 is LIVE on :8030 with /validate-signal endpoint.
    # When enabled, every Pro-tier broadcast is gated through it; the LLM
    # vetoes 'impossible_price' / 'stale_price' / 'looks_real=False'.
    local_llm_base_url: str = "http://ai-edge:8030"
    local_llm_timeout_sec: float = 8.0
    llm_validation_enabled: bool = False  # flip on once ai-edge link verified
    # CSV of risk_tag values that cause rejection. Empty → DEFAULT_REJECT_TAGS.
    llm_validation_reject_tags_csv: str = "impossible_price,stale_price"

    # ─── Quality snapshot (A4 — eval doc 2026-05-20 D1+D4) ─────────────
    # Tracked strategy slugs (CSV). signal_pusher's writer_loop computes
    # a 30d snapshot per slug every quality_snapshot_interval_sec and
    # writes to ``publishing:quality:<slug>`` with EXPIRE.
    quality_tracked_slugs: str = "chainlink_lag"
    quality_window_days: int = 30
    quality_snapshot_interval_sec: int = 60
    quality_snapshot_ttl_sec: int = 180   # 3× interval — deadman if writer dies
    quality_min_sharpe: float = 1.0       # gate floor — below = no broadcast
    quality_min_n_closed: int = 30        # gate floor — too few closes = no broadcast
    # If True, missing snapshot blocks publish (fail-CLOSED — brand promise).
    # If False, missing snapshot allows publish with footer absent (fail-OPEN).
    # Start False for soft-launch; flip True for public launch.
    quality_snapshot_required: bool = False

    # ─── Observability (A7 — eval doc 2026-05-20) ──────────────────────
    # Sentry free tier (5k events/mo) covers a 100-sub product. Operator
    # pastes DSN to enable; empty = silent no-op.
    sentry_dsn: str = ""
    sentry_environment: str = "production"
    sentry_traces_sample_rate: float = 0.1

    # ─── Admin endpoints ───────────────────────────────────────────────
    # Bearer token required on /v1/admin/* routes. Empty = admin routes
    # refuse all requests (default-deny).
    admin_bearer_token: str = ""

    # ─── HTTP server ───────────────────────────────────────────────────
    http_host: str = "0.0.0.0"  # noqa: S104 — bound to 127.0.0.1 in compose
    http_port: int = 8020       # outside existing port range (8000-8013)
    log_level: str = "INFO"


settings = Settings()  # type: ignore[call-arg]
