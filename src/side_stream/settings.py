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
    whop_signal_pro_product_id: str = ""
    whop_pro_alerts_product_id: str = ""
    whop_signal_pro_plus_product_id: str = ""
    whop_enterprise_product_id: str = ""
    stripe_api_key: str = ""
    stripe_webhook_secret: str = ""

    # ─── HTTP server ───────────────────────────────────────────────────
    http_host: str = "0.0.0.0"  # noqa: S104 — bound to 127.0.0.1 in compose
    http_port: int = 8020       # outside existing port range (8000-8013)
    log_level: str = "INFO"


settings = Settings()  # type: ignore[call-arg]
