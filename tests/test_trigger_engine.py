"""Pure-helper tests for trigger_engine."""
from __future__ import annotations

import os

os.environ.setdefault("POSTGRES_DSN", "postgresql://test:test@localhost:5432/test")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")

from side_stream.trigger_engine import (
    TriggerRule,
    group_rules_by_asset,
    should_fire_trigger,
)


def _rule(
    rule_kind: str = "crosses_above",
    threshold: float = 100.0,
    cooldown: int = 300,
    last_fired: float = 0.0,
    asset: str = "btc",
) -> TriggerRule:
    return TriggerRule(
        id="00000000-0000-0000-0000-000000000001",
        user_id="00000000-0000-0000-0000-000000000002",
        asset_alias=asset,
        rule_kind=rule_kind,
        threshold_usd=threshold,
        cooldown_seconds=cooldown,
        last_fired_at_unix=last_fired,
        deliver_webhook=True,
        webhook_url="https://example.com/webhook",
        deliver_discord=False,
        discord_webhook_url=None,
        deliver_slack=False,
        slack_webhook_url=None,
        deliver_email=False,
        email_address=None,
        label="test",
    )


def test_crosses_above_fires_on_upward_cross():
    rule = _rule("crosses_above", threshold=100.0)
    assert should_fire_trigger(
        rule=rule, prev_price=99.0, curr_price=101.0, now_unix=1_000_000,
    )


def test_crosses_above_does_not_fire_on_downward_move():
    rule = _rule("crosses_above", threshold=100.0)
    assert not should_fire_trigger(
        rule=rule, prev_price=105.0, curr_price=99.0, now_unix=1_000_000,
    )


def test_crosses_above_does_not_fire_on_within_range_jitter():
    rule = _rule("crosses_above", threshold=100.0)
    assert not should_fire_trigger(
        rule=rule, prev_price=102.0, curr_price=104.0, now_unix=1_000_000,
    )


def test_crosses_below_fires_on_downward_cross():
    rule = _rule("crosses_below", threshold=100.0)
    assert should_fire_trigger(
        rule=rule, prev_price=101.0, curr_price=99.0, now_unix=1_000_000,
    )


def test_either_fires_in_both_directions():
    rule = _rule("either", threshold=100.0)
    assert should_fire_trigger(
        rule=rule, prev_price=99.0, curr_price=101.0, now_unix=1_000_000,
    )
    assert should_fire_trigger(
        rule=rule, prev_price=101.0, curr_price=99.0, now_unix=1_000_000,
    )


def test_cooldown_blocks_re_fire():
    rule = _rule("crosses_above", threshold=100.0, cooldown=300, last_fired=999_900)
    # 100s after last fire — still in cooldown
    assert not should_fire_trigger(
        rule=rule, prev_price=99.0, curr_price=101.0, now_unix=1_000_000,
    )
    # 400s after — cooldown expired
    assert should_fire_trigger(
        rule=rule, prev_price=99.0, curr_price=101.0, now_unix=1_000_300,
    )


def test_first_tick_does_not_fire_without_prev():
    rule = _rule("crosses_above", threshold=100.0)
    assert not should_fire_trigger(
        rule=rule, prev_price=None, curr_price=101.0, now_unix=1_000_000,
    )


def test_zero_price_does_not_fire():
    rule = _rule("crosses_above", threshold=100.0)
    assert not should_fire_trigger(
        rule=rule, prev_price=99.0, curr_price=0.0, now_unix=1_000_000,
    )


def test_group_rules_by_asset_buckets_correctly():
    rules = [
        _rule(asset="btc"),
        _rule(asset="eth"),
        _rule(asset="btc"),
        _rule(asset="sol"),
    ]
    grouped = group_rules_by_asset(rules)
    assert set(grouped.keys()) == {"btc", "eth", "sol"}
    assert len(grouped["btc"]) == 2
    assert len(grouped["eth"]) == 1
    assert len(grouped["sol"]) == 1
