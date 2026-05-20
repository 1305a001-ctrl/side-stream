# Phase C — Deploy `side-stream` on ai-primary

Soup-to-nuts deployment runbook. Run this on **ai-primary** after Phase B
(operator gates) is complete.

---

## Pre-flight (must clear before starting)

These are Phase B operator gates. If any of these is missing, stop and
finish Phase B first.

- [ ] Repo pushed to GitHub (`gh repo create 1305a001-ctrl/side-stream --private --source=. --push` from Mac)
- [ ] CI built the image successfully → `ghcr.io/1305a001-ctrl/side-stream:latest` exists
- [ ] `/root/.docker/config.json` on ai-primary has ghcr auth (copied from `benadmin`)
- [ ] Brand name chosen
- [ ] Domain bought + Cloudflare DNS configured
- [ ] Cloudflare Tunnel `cloudflared` running on ai-primary, pointing `api.<brand>.<tld>` → `localhost:8020`
- [ ] 3 Whop products created (founding $29 / standard $49 / pro $99) with product IDs + webhook secret + API key
- [ ] @BotFather bot created + public channel + private group + bot is admin in both

---

## Step 1 — Create the Postgres database

```bash
# ai-primary
ssh ai-primary
sudo docker exec postgres psql -U postgres -c "CREATE DATABASE sidestream;"
sudo docker exec postgres psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE sidestream TO benadmin;"
```

**Why:** all of subscriptions / users / triggers / delivery_logs /
broadcast_logs lives here. Schema is idempotent (CREATE IF NOT EXISTS)
so safe to re-run.

## Step 2 — Apply the schema

```bash
# ai-primary
cd /tmp && curl -L -o sidestream-schema.sql \
  https://raw.githubusercontent.com/1305a001-ctrl/side-stream/main/db/schema.sql
sudo docker cp /tmp/sidestream-schema.sql postgres:/tmp/sidestream-schema.sql
sudo docker exec postgres psql -U benadmin -d sidestream -f /tmp/sidestream-schema.sql
sudo docker exec postgres psql -U benadmin -d sidestream -c "\dt"
```

Expected output: 5 tables — `users`, `subscriptions`, `triggers`,
`delivery_logs`, `broadcast_logs`.

## Step 3 — Create `/srv/secrets/side-stream.env`

```bash
# ai-primary
sudo install -m 600 /dev/null /srv/secrets/side-stream.env
sudo $EDITOR /srv/secrets/side-stream.env
```

Paste this template, fill in `<...>` placeholders:

```bash
# ─── Brand ──────────────────────────────────────────────────────────
BRAND_NAME=<your brand>
PUBLIC_CHANNEL_URL=https://t.me/<your_channel>
SIGNAL_PRO_GROUP_URL=https://t.me/+<invite>

# ─── Backend ────────────────────────────────────────────────────────
POSTGRES_DSN=postgresql://benadmin:<password>@postgres:5432/sidestream
REDIS_URL=redis://:<redis_password>@redis:6379/0

# ─── Telegram (from @BotFather) ────────────────────────────────────
TELEGRAM_BOT_TOKEN=<bot_token>
TELEGRAM_PUBLIC_CHANNEL_ID=@<your_public_channel>
TELEGRAM_PRO_GROUP_ID=-100<your_group_id>
TELEGRAM_MOCK_MODE=true                  # KEEP true for first 24h soak

# ─── Payment (Whop) ─────────────────────────────────────────────────
PAYMENT_MODE=whop
WHOP_API_KEY=<whop_api_key>
WHOP_WEBHOOK_SECRET=<whop_webhook_secret>
WHOP_FOUNDING_PRODUCT_ID=<prod_id_for_29mo>
WHOP_STANDARD_PRODUCT_ID=<prod_id_for_49mo>
WHOP_PRO_PRODUCT_ID=<prod_id_for_99mo>

# ─── A4 calibration gate ────────────────────────────────────────────
QUALITY_TRACKED_SLUGS=chainlink_lag
QUALITY_MIN_SHARPE=1.0
QUALITY_MIN_N_CLOSED=30
QUALITY_SNAPSHOT_REQUIRED=false          # KEEP false for soft launch

# ─── A5 LLM validation (opt-in) ────────────────────────────────────
LOCAL_LLM_BASE_URL=http://ai-edge:8030
LLM_VALIDATION_ENABLED=false             # KEEP false until /validate-signal smoke passes

# ─── A7 Sentry (paste DSN to enable) ───────────────────────────────
SENTRY_DSN=                              # leave empty for now
SENTRY_ENVIRONMENT=production

# ─── Admin bearer (generate a long random token) ───────────────────
ADMIN_BEARER_TOKEN=<generate_with: openssl rand -hex 32>

# ─── HTTP server ────────────────────────────────────────────────────
HTTP_HOST=0.0.0.0
HTTP_PORT=8020
```

Verify permissions:
```bash
sudo chmod 600 /srv/secrets/side-stream.env
sudo ls -la /srv/secrets/side-stream.env
# -rw------- 1 root root ... /srv/secrets/side-stream.env
```

## Step 4 — Copy compose + first start

```bash
# ai-primary
sudo mkdir -p /srv/compose/side-stream
sudo cp /tmp/sidestream-schema.sql /srv/compose/side-stream/  # archive
sudo curl -L -o /srv/compose/side-stream/docker-compose.yml \
  https://raw.githubusercontent.com/1305a001-ctrl/side-stream/main/docker/docker-compose.yml
cd /srv/compose/side-stream
sudo docker compose pull
sudo docker compose up -d
sleep 3
sudo docker compose ps
sudo docker compose logs side-stream | tail -30
```

Expected: container `Up`, log shows `api.starting payment_mode=whop`,
`signal_pusher.starting mock=true ...`, no errors.

## Step 5 — Health check + smoke

```bash
# ai-primary
curl -s localhost:8020/health | python3 -m json.tool
```

Expected:
```json
{
  "status": "ok",
  "brand": "<your brand>",
  "payment_mode": "whop",
  "telegram_mock_mode": true
}
```

Landing page (public via Cloudflare Tunnel):
```bash
curl -sI https://api.<brand>.<tld>/
# HTTP/2 200
# content-type: text/html; charset=utf-8
```

Live quality endpoint (will be empty until snapshot writer publishes):
```bash
curl -s localhost:8020/v1/dashboard/quality | python3 -m json.tool
# After ~60s, snapshots: {} should populate with chainlink_lag stats
```

## Step 6 — Admin endpoint smoke (replace `<token>` with ADMIN_BEARER_TOKEN)

```bash
# Operator dashboard
curl -s -H "Authorization: Bearer <token>" localhost:8020/v1/admin/summary | python3 -m json.tool

# Publishing status (current halts)
curl -s -H "Authorization: Bearer <token>" localhost:8020/v1/admin/publishing-status | python3 -m json.tool

# Pause all publishing (operator override)
curl -s -X POST -H "Authorization: Bearer <token>" \
  "localhost:8020/v1/admin/pause?source=all" | python3 -m json.tool

# Resume
curl -s -X POST -H "Authorization: Bearer <token>" \
  "localhost:8020/v1/admin/resume?source=all" | python3 -m json.tool
```

## Step 7 — 24-hour soak in mock mode

Watch the logs without flipping anything:

```bash
# ai-primary
sudo docker compose logs -f side-stream | grep -E "pro_mock_broadcast|free_mock_broadcast|skip_halted|skip_quality|skip_not_emitted"
```

You should see `pro_mock_broadcast` lines flowing as chainlink_lag emits.
None should hit real Telegram (mock_mode=true).

After 24h, inspect:
```bash
# How many would have broadcast?
sudo docker exec postgres psql -U benadmin -d sidestream -c \
  "SELECT channel, COUNT(*) FROM broadcast_logs GROUP BY channel;"

# Quality snapshot fresh?
sudo docker exec redis redis-cli -a "<pw>" --no-auth-warning \
  GET publishing:quality:chainlink_lag

# Any halts active?
sudo docker exec redis redis-cli -a "<pw>" --no-auth-warning \
  SMEMBERS publishing:halts
```

## Step 8 — Public launch checklist

In strict order. Each step is reversible — flip TG mode back to true,
or flip the env flag back, if anything misbehaves.

```bash
# A. LLM validation ON (only after smoke from ai-primary to ai-edge:8030 passes)
sudo sed -i 's/LLM_VALIDATION_ENABLED=false/LLM_VALIDATION_ENABLED=true/' \
  /srv/secrets/side-stream.env

# B. Real Telegram broadcasts ON (mock_mode → false)
sudo sed -i 's/TELEGRAM_MOCK_MODE=true/TELEGRAM_MOCK_MODE=false/' \
  /srv/secrets/side-stream.env

# C. Sentry DSN paste (from sentry.io free tier)
sudo sed -i 's/SENTRY_DSN=/SENTRY_DSN=https:\/\/...@sentry.io\/.../' \
  /srv/secrets/side-stream.env

# D. Quality snapshot REQUIRED (fail-CLOSED — the brand promise)
sudo sed -i 's/QUALITY_SNAPSHOT_REQUIRED=false/QUALITY_SNAPSHOT_REQUIRED=true/' \
  /srv/secrets/side-stream.env

# Restart to pick up env changes
sudo docker compose restart side-stream
sudo docker compose logs --tail 30 side-stream
```

## Smoke after each flip

```bash
# After A (LLM): broadcast should still flow; if all signals get skipped
# with reason=llm_*, the ai-edge link is broken — flip A back.
sudo docker compose logs side-stream | grep -E "skip_llm|pro_skip" | tail -10

# After B (real TG): first signal should land in your channel within 60s.
# If nothing arrives after 5min, check TELEGRAM_BOT_TOKEN + channel ID
# is correct, bot is admin in channel.

# After D (quality required): if no snapshot is published yet, ALL pro
# broadcasts will skip with reason=snapshot_missing. The writer publishes
# every 60s — wait one cycle and re-check.
sudo docker exec redis redis-cli -a "<pw>" --no-auth-warning \
  TTL publishing:quality:chainlink_lag
# Should return a positive number (TTL in seconds). -2 means key missing.
```

## Rollback

If anything goes wrong:

```bash
# Pause ALL publishing (the panic button)
curl -s -X POST -H "Authorization: Bearer <token>" \
  "localhost:8020/v1/admin/pause?source=all"

# Or stop the container entirely
cd /srv/compose/side-stream && sudo docker compose stop side-stream

# Revert a specific env flag — see Step 8 — then restart
```

## Per-launch-step exit criteria

| Flip | Don't proceed unless |
|---|---|
| LLM ON  | At least 10 signals pass through pro broadcast path in last hour without `skip_llm` |
| TG real | Bot can post a manual test message to channel via curl + Telegram API |
| Sentry  | Test exception fires from /v1/admin/* and lands in Sentry dashboard |
| Quality required | `publishing:quality:chainlink_lag` has TTL > 0 |

---

## Ongoing ops

### Daily check (5 min)

```bash
# Subs growth
curl -s -H "Authorization: Bearer <token>" localhost:8020/v1/admin/summary \
  | python3 -c 'import sys,json; d=json.load(sys.stdin); print("Users:",d["users_total"], "MRR:$",d["estimated_mrr_usd"], "Triggers:",d["active_triggers"])'

# Quality is healthy
curl -s localhost:8020/v1/dashboard/quality \
  | python3 -c 'import sys,json; d=json.load(sys.stdin); [print(k, "Sharpe", v.get("sharpe"), "PnL $", v.get("total_pnl_usd")) for k,v in d.get("snapshots",{}).items()]'

# Yesterday's broadcasts
sudo docker exec postgres psql -U benadmin -d sidestream -c \
  "SELECT channel, COUNT(*) FILTER (WHERE delivered) AS delivered, COUNT(*) FILTER (WHERE NOT delivered) AS failed
   FROM broadcast_logs WHERE created_at > NOW() - INTERVAL '24 hours' GROUP BY channel;"
```

### Refund / abuse case

1. Find the user: `SELECT * FROM users WHERE email = '<email>';`
2. Cancel their subscription in Whop dashboard (Whop fires the webhook, our handler marks status='canceled')
3. Verify: `SELECT tier, status FROM subscriptions WHERE user_id = '<uuid>';`

### Adding a new tracked strategy slug

```bash
# Append to env
sudo sed -i 's/QUALITY_TRACKED_SLUGS=chainlink_lag/QUALITY_TRACKED_SLUGS=chainlink_lag,new_slug/' \
  /srv/secrets/side-stream.env
sudo docker compose restart side-stream
```

The writer loop will start publishing snapshots for the new slug on
next cycle (≤60s).
