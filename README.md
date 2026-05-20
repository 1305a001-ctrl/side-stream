# Streams Edge — paid signal channel + alerts SaaS

> Combined Telegram signal channel + sub-second webhook/Discord/Slack/email alerts SaaS on the existing Chainlink Data Streams pipeline.

## What this is

Two product surfaces on one backend:

- **Signal Pro Telegram** — broadcasts qualifying `chainlink-lag` + `tokenized-equity-arb` signals to subscribers in real-time
- **Pro Alerts webhooks** — per-user price-cross triggers fired sub-second from Chainlink Data Streams, delivered via webhook / Discord / Slack / email

Shared Postgres for users + subscriptions + trigger rules + delivery logs. Shared Redis as the price source (chainlink:&lt;alias&gt;:reports streams from chainlink-streams Go service).

## SKUs (3-tier post 2026-05-20 eval prune)

| Tier | Price | Includes |
|---|---|---|
| Free | $0 | Public TG channel, 5-min delay, top 3 signals/day |
| Standard | $49/mo (or $29 founding × 50 lifetime cap) | Private TG real-time signals + 100 custom price-cross triggers (webhook/Discord/Slack/email) + sub-second alerts |
| Pro | $99/mo | Standard + 500 triggers + GMX liquidation alerts + news-driven Polymarket triggers + weekly Sunday write-up |

5-tier model archived 2026-05-20 — eval doc found >3 tiers drops Whop conversion 15-20%. Founding $29 is a price SKU within 'standard', not a separate tier; capped at 50 lifetime then closed permanently (kill-list rule 4).

## Repo layout

```
side-stream/
├── src/side_stream/
│   ├── signal_pusher.py       Subscribes to chainlink:eval_log + tokenized_equity:eval_log,
│   │                          broadcasts to TG channels (free + pro)
│   ├── trigger_engine.py      Subscribes to chainlink:<alias>:reports,
│   │                          evaluates user trigger rules from Postgres
│   ├── delivery.py            Webhook / Discord / Slack / email senders + retry logic
│   ├── settings.py            Pydantic settings (env-driven)
│   ├── main.py                Service entrypoint (runs all three concurrently)
│   └── api/
│       └── app.py             FastAPI: trigger CRUD + Whop webhook handler
├── tests/                     Pure-helper unit tests (21 passing)
├── db/schema.sql              Postgres schema (users, subscriptions, triggers, delivery_logs, broadcast_logs)
├── docker/docker-compose.yml  Compose stanza for ai-primary
├── Dockerfile                 Slim Python 3.11 image
├── pyproject.toml             Deps + ruff config
└── README.md
```

## Running locally

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'

# Tests (no external deps required — pure-helper tests only)
python -m pytest tests/ -q

# Lint
python -m ruff check src/ tests/

# Run service (requires postgres + redis available at the configured URLs)
export POSTGRES_DSN="postgresql://benadmin:CHANGEME@localhost:5432/sidestream"
export REDIS_URL="redis://localhost:6379/0"
python -m side_stream.main
```

## First-time setup

1. **Create database**:
   ```bash
   psql -U postgres -c "CREATE DATABASE sidestream;"
   psql -U benadmin -d sidestream -f db/schema.sql
   ```

2. **Pick a brand name** (default placeholder is "Streams Edge"). Set `BRAND_NAME` env if changing.

3. **Create a Telegram bot** via [@BotFather](https://t.me/BotFather):
   - Free public channel: `@streamsedge_free` (or pick a name)
   - Private Signal Pro group: created with invite link
   - Bot must be admin in both
   - Paste token: `TELEGRAM_BOT_TOKEN=...`
   - Paste channel IDs: `TELEGRAM_PUBLIC_CHANNEL_ID=@streamsedge_free`, `TELEGRAM_PRO_GROUP_ID=-100...`
   - Set `TELEGRAM_MOCK_MODE=false` once configured (default true = stdout logging only)

4. **Pick payment provider**:
   - **Whop (recommended)** — create 3 products at whop.com, paste product IDs:
     - `WHOP_FOUNDING_PRODUCT_ID=...`  ($29/mo, capped at 50 lifetime, then close permanently)
     - `WHOP_STANDARD_PRODUCT_ID=...`  ($49/mo)
     - `WHOP_PRO_PRODUCT_ID=...`       ($99/mo)
     - Set `PAYMENT_MODE=whop`
     - Configure Whop webhook → `https://api.<your-domain>/v1/webhooks/whop`
   - **Stripe (alternative)** — set `PAYMENT_MODE=stripe` + Stripe creds + connect to existing Stripe products

5. **SMTP** (optional, for email delivery channel):
   - `SMTP_HOST=smtp.fastmail.com`, `SMTP_USERNAME=alerts@...`, `SMTP_PASSWORD=...`

## Deploy on ai-primary

```bash
# 1. Build + push image (after committing + pushing to github.com/1305a001-ctrl/side-stream)
docker build -t ghcr.io/1305a001-ctrl/side-stream:latest .
docker push ghcr.io/1305a001-ctrl/side-stream:latest

# 2. SSH ai-primary, create env file
sudo nano /srv/secrets/side-stream.env  # paste all SAAS env vars
sudo chmod 600 /srv/secrets/side-stream.env

# 3. Copy compose + start
sudo cp docker/docker-compose.yml /srv/compose/side-stream/docker-compose.yml
cd /srv/compose/side-stream && sudo docker compose up -d

# 4. Apply schema
sudo docker exec postgres psql -U benadmin -d sidestream -f /tmp/schema.sql
```

## Operational notes

- **`telegram_mock_mode=true`** is the default — until you provide a real bot token, all "broadcasts" go to stdout. Safe for local dev + ai-primary soft-launch.
- **`payment_mode=none`** is the default — trigger CRUD works without any subscription gating, so you can manually create + test triggers without a billing provider. Set to 'whop' or 'stripe' before public launch.
- **Idempotency**: `broadcast_logs` table has `UNIQUE (signal_source, signal_id, channel)` — re-running the service is safe.
- **Trigger cooldown**: configured per-trigger in DB (default 300s). Prevents flapping when price oscillates around a threshold.
- **Delivery retry**: exponential backoff on 5xx + network errors. 4xx errors are treated as permanent (user webhook removed, etc.) and not retried.

## API endpoints

| Endpoint | Method | Purpose |
|---|---|---|
| `/health` | GET | Service status + mode |
| `/v1/triggers` | POST | Create new trigger for user (gated by active subscription) |
| `/v1/triggers?user_email=...` | GET | List user's triggers |
| `/v1/triggers/{id}` | DELETE | Deactivate trigger |
| `/v1/webhooks/whop` | POST | Whop subscription event handler |

## What this delivers (per the scope doc)

- **Capital required: $0 trading**
- **Infra cost**: ~$50/mo (ai-primary shared)
- **Whop take rate**: ~5% (only on paid SKUs)
- **Month 6 target**: $1,750/mo net (per scope) → could hit $2,800 with proposed Signal Pro+ + Enterprise tiers
- **Month 12 aggressive**: $8-13k/mo

## Risk + mitigations

| Risk | Mitigation |
|---|---|
| Signal quality drift → Signal Pro churn | Tie signal_pusher pause/resume to same Sharpe>1 gate as live-trading flip. Auto-pause on quality regression. |
| "Trading signals" regulatory grey zone | Position as "educational alerts on public oracle data." Avoid "buy"/"sell" language in messages — use "price crossed", "edge detected". |
| Trigger engine load | Bucket by asset_alias; in-memory index rebuilt every 30s. 10k+ triggers fits on one node. |
| Streams feed outage | Health check + auto-fallback: "Streams feed degraded — alerts paused." Auto-resume when feed returns. |
| Whop dependency | Acceptable while small. Migrate to custom Stripe + raw Telegram Bot API at MRR > $10k. |
| Webhook delivery failures | Exponential backoff retry. Delivery log per attempt. Disable trigger after N consecutive fails + email user. |

## What's NOT yet built (gated on Ben action)

- [ ] Whop account + product configuration → need Ben to create
- [ ] Telegram bot creation via @BotFather → need Ben (~2 min)
- [ ] Domain + landing page → need Ben to decide brand name + register
- [ ] Real signal_pusher → Telegram bot token rotation → enable `telegram_mock_mode=false`
- [ ] Production secret in `/srv/secrets/side-stream.env` on ai-primary
- [ ] Postgres DB `sidestream` created on ai-primary

## What IS built (this PR)

- ✅ Full Postgres schema (users, subscriptions, triggers, delivery_logs, broadcast_logs)
- ✅ `signal_pusher.py` — Redis stream consumer + free-tier top-N + Pro-tier real-time broadcast (mock mode default)
- ✅ `trigger_engine.py` — sub-second price-cross evaluation, in-memory index, cooldown gate
- ✅ `delivery.py` — webhook / Discord / Slack / email senders with retry + delivery logging
- ✅ FastAPI app — trigger CRUD + Whop webhook handler + entitlement gate per tier
- ✅ Docker image + compose stanza for ai-primary
- ✅ 21 unit tests passing, ruff clean
