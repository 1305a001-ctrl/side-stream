"""Pure payload-formatting tests for delivery."""
from __future__ import annotations

import os

os.environ.setdefault("POSTGRES_DSN", "postgresql://test:test@localhost:5432/test")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")

from side_stream.delivery import (
    format_alert_payload,
    format_discord_payload,
    format_slack_payload,
)
from side_stream.trigger_engine import PriceTick, TriggerRule


def _rule(label: str | None = None, rule_kind: str = "crosses_above") -> TriggerRule:
    return TriggerRule(
        id="abc-1",
        user_id="user-1",
        asset_alias="btc",
        rule_kind=rule_kind,
        threshold_usd=70_000.0,
        cooldown_seconds=300,
        last_fired_at_unix=0,
        deliver_webhook=True,
        webhook_url="https://example.com/webhook",
        deliver_discord=False,
        discord_webhook_url=None,
        deliver_slack=False,
        slack_webhook_url=None,
        deliver_email=False,
        email_address=None,
        label=label,
    )


def _tick() -> PriceTick:
    return PriceTick(
        asset_alias="btc",
        benchmark_price_usd=72_500.0,
        observations_ts_unix=1_778_900_000,
    )


def test_format_alert_payload_round_trip():
    p = format_alert_payload(_rule(label="BTC Above 70k"), _tick())
    assert p["trigger_id"] == "abc-1"
    assert p["asset_alias"] == "btc"
    assert p["threshold_usd"] == 70_000.0
    assert p["current_price_usd"] == 72_500.0
    assert p["source"] == "chainlink-data-streams"
    assert p["label"] == "BTC Above 70k"


def test_format_alert_payload_default_label_when_missing():
    p = format_alert_payload(_rule(), _tick())
    assert p["label"] == "BTC crosses_above"


def test_format_discord_payload_uses_embeds():
    p = format_discord_payload(_rule("Custom Label"), _tick())
    assert "embeds" in p
    assert len(p["embeds"]) == 1
    e = p["embeds"][0]
    assert "Custom Label" in e["title"]
    assert "$72,500" in e["fields"][0]["value"]
    # crosses_above → green color
    assert e["color"] == 0x00FF88


def test_format_discord_payload_color_for_crosses_below():
    p = format_discord_payload(_rule(rule_kind="crosses_below"), _tick())
    assert p["embeds"][0]["color"] == 0xFF4444


def test_format_slack_payload_uses_blocks():
    p = format_slack_payload(_rule("My Trigger"), _tick())
    assert "blocks" in p
    assert any(b.get("type") == "header" for b in p["blocks"])
    text = "".join(
        b.get("text", {}).get("text", "") for b in p["blocks"] if b.get("text")
    )
    assert "$72,500" in text
    assert "$70,000" in text
