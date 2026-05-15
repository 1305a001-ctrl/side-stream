"""delivery — webhook / Discord / Slack / email senders for trigger fires.

Each delivery channel:
  - Retries on 5xx + network errors (exponential backoff, capped retries)
  - Skips on 4xx (likely permanent config error — user webhook removed etc.)
  - Records every attempt + outcome in delivery_logs

Idempotency is NOT enforced at this layer — the trigger cooldown gate
in trigger_engine already prevents flapping. Each (trigger_id, fire_time)
is a unique event by construction.
"""
from __future__ import annotations

import asyncio
import json
import logging
import smtplib
from email.message import EmailMessage
from typing import TYPE_CHECKING, Any

import httpx

from side_stream.settings import settings

if TYPE_CHECKING:
    import asyncpg

    from side_stream.trigger_engine import PriceTick, TriggerRule

log = logging.getLogger(__name__)


# ─── Pure payload formatting ───────────────────────────────────────────


def format_alert_payload(
    rule: TriggerRule, tick: PriceTick,
) -> dict[str, Any]:
    """Build the JSON body delivered to webhook/Discord/Slack (all use
    JSON; email gets a text rendering)."""
    return {
        "trigger_id": rule.id,
        "label": rule.label or f"{rule.asset_alias.upper()} {rule.rule_kind}",
        "asset_alias": rule.asset_alias,
        "rule_kind": rule.rule_kind,
        "threshold_usd": rule.threshold_usd,
        "current_price_usd": tick.benchmark_price_usd,
        "observation_ts_unix": tick.observations_ts_unix,
        "source": "chainlink-data-streams",
    }


def format_discord_payload(
    rule: TriggerRule, tick: PriceTick,
) -> dict[str, Any]:
    """Discord webhook expects a different schema (embeds)."""
    label = rule.label or f"{rule.asset_alias.upper()} {rule.rule_kind}"
    direction = "above ⬆️" if "above" in rule.rule_kind else (
        "below ⬇️" if "below" in rule.rule_kind else "crossed"
    )
    return {
        "embeds": [{
            "title": f"{settings.brand_name} — {label}",
            "description": (
                f"**{rule.asset_alias.upper()}** crossed {direction} "
                f"**${rule.threshold_usd:,.2f}**"
            ),
            "fields": [
                {"name": "Current Price", "value": f"${tick.benchmark_price_usd:,.2f}", "inline": True},
                {"name": "Threshold", "value": f"${rule.threshold_usd:,.2f}", "inline": True},
                {"name": "Source", "value": "Chainlink Data Streams", "inline": False},
            ],
            "color": 0x00FF88 if "above" in rule.rule_kind else 0xFF4444,
        }],
    }


def format_slack_payload(
    rule: TriggerRule, tick: PriceTick,
) -> dict[str, Any]:
    """Slack webhook uses blocks API."""
    label = rule.label or f"{rule.asset_alias.upper()} {rule.rule_kind}"
    return {
        "blocks": [
            {"type": "header", "text": {"type": "plain_text", "text": f"{settings.brand_name} — {label}"}},
            {"type": "section", "text": {
                "type": "mrkdwn",
                "text": (
                    f"*{rule.asset_alias.upper()}* @ "
                    f"*${tick.benchmark_price_usd:,.2f}* "
                    f"({rule.rule_kind.replace('_', ' ')} ${rule.threshold_usd:,.2f})"
                ),
            }},
            {"type": "context", "elements": [
                {"type": "mrkdwn", "text": "Source: Chainlink Data Streams"},
            ]},
        ],
    }


# ─── Async senders ─────────────────────────────────────────────────────


async def _send_with_retries(
    *, url: str, json_payload: dict[str, Any], http: httpx.AsyncClient,
) -> tuple[bool, int | None, str | None]:
    """POST with exponential-backoff retry. Returns (delivered, status_code, error_msg).
    Retries on 5xx + network errors; doesn't retry on 4xx (assumed permanent)."""
    backoff = settings.delivery_backoff_base_sec
    last_status: int | None = None
    last_err: str | None = None
    for attempt in range(settings.delivery_max_retries + 1):
        try:
            resp = await http.post(
                url, json=json_payload,
                timeout=settings.delivery_http_timeout_sec,
            )
            last_status = resp.status_code
            if 200 <= resp.status_code < 300:
                return True, resp.status_code, None
            if 400 <= resp.status_code < 500:
                # Permanent — don't retry. e.g. user removed webhook.
                last_err = f"http_4xx: {resp.text[:300]}"
                return False, resp.status_code, last_err
            # 5xx — retry
            last_err = f"http_5xx: {resp.text[:300]}"
        except httpx.TimeoutException:
            last_err = "timeout"
        except httpx.HTTPError as e:
            last_err = f"network_error: {e}"
        if attempt < settings.delivery_max_retries:
            await asyncio.sleep(backoff)
            backoff *= 2
    return False, last_status, last_err


async def _log_delivery(
    pool: asyncpg.Pool,
    *,
    rule_id: str,
    user_id: str,
    channel: str,
    status_label: str,
    response_code: int | None,
    error_message: str | None,
    payload: dict[str, Any],
) -> None:
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO delivery_logs
                  (trigger_id, user_id, channel, status, response_code,
                   error_message, payload)
                VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
                """,
                rule_id, user_id, channel, status_label, response_code,
                (error_message or "")[:500], json.dumps(payload),
            )
    except Exception:
        log.exception("delivery._log_delivery_failed rule=%s ch=%s", rule_id, channel)


async def _send_email(
    *, to_email: str, subject: str, body: str,
) -> tuple[bool, str | None]:
    """SMTP send — synchronous lib wrapped in to_thread. SMTP creds must
    be configured via env or this returns (False, 'smtp_not_configured')."""
    if not settings.smtp_host or not settings.smtp_username:
        return False, "smtp_not_configured"

    def _send_sync() -> None:
        msg = EmailMessage()
        msg["From"] = settings.smtp_from_email
        msg["To"] = to_email
        msg["Subject"] = subject
        msg.set_content(body)
        with smtplib.SMTP(settings.smtp_host, settings.smtp_port) as s:
            s.starttls()
            s.login(settings.smtp_username, settings.smtp_password)
            s.send_message(msg)

    try:
        await asyncio.to_thread(_send_sync)
        return True, None
    except smtplib.SMTPException as e:
        return False, f"smtp_error: {e}"
    except Exception as e:  # noqa: BLE001
        return False, f"smtp_unexpected: {e}"


async def deliver_alert(
    *, rule: TriggerRule, tick: PriceTick,
    pool: asyncpg.Pool, http: httpx.AsyncClient,
) -> None:
    """Dispatch alert across all enabled channels for this rule.
    Each channel's success/failure is logged independently in delivery_logs.
    """
    payload = format_alert_payload(rule, tick)

    if rule.deliver_webhook and rule.webhook_url:
        ok, code, err = await _send_with_retries(
            url=rule.webhook_url, json_payload=payload, http=http,
        )
        await _log_delivery(
            pool, rule_id=rule.id, user_id=rule.user_id, channel="webhook",
            status_label=("sent" if ok else _http_status_label(code, err)),
            response_code=code, error_message=err, payload=payload,
        )

    if rule.deliver_discord and rule.discord_webhook_url:
        dpayload = format_discord_payload(rule, tick)
        ok, code, err = await _send_with_retries(
            url=rule.discord_webhook_url, json_payload=dpayload, http=http,
        )
        await _log_delivery(
            pool, rule_id=rule.id, user_id=rule.user_id, channel="discord",
            status_label=("sent" if ok else _http_status_label(code, err)),
            response_code=code, error_message=err, payload=dpayload,
        )

    if rule.deliver_slack and rule.slack_webhook_url:
        spayload = format_slack_payload(rule, tick)
        ok, code, err = await _send_with_retries(
            url=rule.slack_webhook_url, json_payload=spayload, http=http,
        )
        await _log_delivery(
            pool, rule_id=rule.id, user_id=rule.user_id, channel="slack",
            status_label=("sent" if ok else _http_status_label(code, err)),
            response_code=code, error_message=err, payload=spayload,
        )

    if rule.deliver_email and rule.email_address:
        label = rule.label or f"{rule.asset_alias.upper()} {rule.rule_kind}"
        subject = f"[{settings.brand_name}] {label}"
        body = (
            f"{rule.asset_alias.upper()} crossed {rule.rule_kind} "
            f"${rule.threshold_usd:,.2f}.\n"
            f"Current price: ${tick.benchmark_price_usd:,.2f}\n"
            f"Source: Chainlink Data Streams.\n"
        )
        ok, err = await _send_email(
            to_email=rule.email_address, subject=subject, body=body,
        )
        await _log_delivery(
            pool, rule_id=rule.id, user_id=rule.user_id, channel="email",
            status_label="sent" if ok else "network_error",
            response_code=None, error_message=err, payload=payload,
        )


def _http_status_label(code: int | None, err: str | None) -> str:
    """Map (status_code, error_msg) → DB-friendly status label."""
    if err == "timeout":
        return "timeout"
    if code is None:
        return "network_error"
    if 400 <= code < 500:
        return "http_4xx"
    if 500 <= code < 600:
        return "http_5xx"
    return "network_error"


__all__ = [
    "deliver_alert",
    "format_alert_payload",
    "format_discord_payload",
    "format_slack_payload",
]
