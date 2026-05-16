"""Tests for stripe_webhooks.py — event normalization + price→tier mapping."""
from __future__ import annotations

import os

os.environ.setdefault("POSTGRES_DSN", "postgresql://test:test@localhost:5432/test")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")

from side_stream.stripe_webhooks import (  # noqa: E402
    HANDLED_EVENT_TYPES,
    normalize_stripe_event,
    stripe_price_to_tier,
)

# ─── normalize_stripe_event ────────────────────────────────────────


def test_checkout_session_completed():
    event = {
        "type": "checkout.session.completed",
        "data": {
            "object": {
                "customer": "cus_abc",
                "customer_email": "alice@example.com",
                "subscription": "sub_xyz",
                "metadata": {
                    "price_id": "price_1Pro",
                    "product_id": "prod_pro",
                },
            },
        },
    }
    norm = normalize_stripe_event(event)
    assert norm is not None
    assert norm.event_type == "checkout.session.completed"
    assert norm.customer_id == "cus_abc"
    assert norm.customer_email == "alice@example.com"
    assert norm.subscription_id == "sub_xyz"
    assert norm.price_id == "price_1Pro"
    assert norm.new_status == "active"


def test_checkout_session_email_in_customer_details():
    """Stripe Checkout may put email in customer_details instead of top-level."""
    event = {
        "type": "checkout.session.completed",
        "data": {
            "object": {
                "customer": "cus_abc",
                "customer_details": {"email": "bob@example.com"},
                "subscription": "sub_xyz",
                "metadata": {"price_id": "price_1Pro"},
            },
        },
    }
    norm = normalize_stripe_event(event)
    assert norm is not None
    assert norm.customer_email == "bob@example.com"


def test_invoice_payment_succeeded_pulls_price_from_lines():
    event = {
        "type": "invoice.payment_succeeded",
        "data": {
            "object": {
                "customer": "cus_abc",
                "customer_email": "charlie@example.com",
                "subscription": "sub_xyz",
                "lines": {
                    "data": [{
                        "price": {
                            "id": "price_1Pro",
                            "product": "prod_pro",
                        },
                    }],
                },
            },
        },
    }
    norm = normalize_stripe_event(event)
    assert norm is not None
    assert norm.price_id == "price_1Pro"
    assert norm.product_id == "prod_pro"
    assert norm.new_status == "active"


def test_invoice_payment_failed_maps_to_past_due():
    event = {
        "type": "invoice.payment_failed",
        "data": {
            "object": {
                "customer": "cus_abc",
                "customer_email": "dan@example.com",
                "subscription": "sub_xyz",
                "lines": {"data": [{"price": {"id": "price_1"}}]},
            },
        },
    }
    norm = normalize_stripe_event(event)
    assert norm is not None
    assert norm.new_status == "past_due"


def test_customer_subscription_deleted_maps_to_canceled():
    event = {
        "type": "customer.subscription.deleted",
        "data": {
            "object": {
                "id": "sub_xyz",
                "customer": "cus_abc",
                "items": {
                    "data": [{
                        "price": {"id": "price_1Pro", "product": "prod_pro"},
                    }],
                },
            },
        },
    }
    norm = normalize_stripe_event(event)
    assert norm is not None
    assert norm.new_status == "canceled"
    assert norm.subscription_id == "sub_xyz"


def test_customer_subscription_updated_active():
    event = {
        "type": "customer.subscription.updated",
        "data": {
            "object": {
                "id": "sub_xyz",
                "customer": "cus_abc",
                "items": {
                    "data": [{
                        "price": {"id": "price_1Plus", "product": "prod_plus"},
                    }],
                },
            },
        },
    }
    norm = normalize_stripe_event(event)
    assert norm is not None
    assert norm.new_status == "active"


def test_unhandled_event_returns_none():
    event = {
        "type": "customer.created",   # not in HANDLED_EVENT_TYPES
        "data": {"object": {}},
    }
    assert normalize_stripe_event(event) is None


def test_malformed_event_returns_none():
    assert normalize_stripe_event(None) is None  # type: ignore[arg-type]
    assert normalize_stripe_event({}) is None
    assert normalize_stripe_event({"type": "checkout.session.completed"}) is None


def test_missing_object_in_data_returns_none():
    event = {"type": "checkout.session.completed", "data": {}}
    assert normalize_stripe_event(event) is None


def test_handled_event_types_complete():
    """All the events the route handler dispatches on must be in the set."""
    for et in (
        "checkout.session.completed",
        "invoice.payment_succeeded",
        "invoice.payment_failed",
        "customer.subscription.deleted",
        "customer.subscription.updated",
    ):
        assert et in HANDLED_EVENT_TYPES


# ─── stripe_price_to_tier ──────────────────────────────────────────


def test_price_to_tier_lookup():
    m = {"price_1Pro": "pro_alerts", "price_2Plus": "signal_pro_plus"}
    assert stripe_price_to_tier(price_id="price_1Pro", price_tier_map=m) == "pro_alerts"
    assert (
        stripe_price_to_tier(price_id="price_2Plus", price_tier_map=m)
        == "signal_pro_plus"
    )


def test_price_to_tier_unknown_returns_none():
    m = {"price_1Pro": "pro_alerts"}
    assert stripe_price_to_tier(price_id="price_unknown", price_tier_map=m) is None


def test_price_to_tier_empty_returns_none():
    assert stripe_price_to_tier(price_id="", price_tier_map={"x": "y"}) is None
