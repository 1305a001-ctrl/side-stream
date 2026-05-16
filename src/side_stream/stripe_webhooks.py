"""Stripe webhook payload normalization.

Stripe event shapes for the events we care about:

  checkout.session.completed   — first-time subscription, payment confirmed
  invoice.payment_succeeded    — recurring renewal
  customer.subscription.deleted — cancellation
  invoice.payment_failed       — past-due / dunning

For each, we extract:
  - customer email (for our users table)
  - subscription_id (for idempotent linking)
  - product_id / price_id (for tier mapping)

Pure helpers — no DB, no HTTP. Route handler calls these after
signature verification, then writes to users + subscriptions tables.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

log = logging.getLogger(__name__)


# Event types we explicitly handle. Unknown events are ack-ignored
# (we still 2xx the webhook so Stripe doesn't retry).
HANDLED_EVENT_TYPES: set[str] = {
    "checkout.session.completed",
    "invoice.payment_succeeded",
    "customer.subscription.deleted",
    "customer.subscription.updated",
    "invoice.payment_failed",
}


@dataclass(frozen=True)
class StripeNormalizedEvent:
    """Normalized Stripe event with the only fields we use downstream."""
    event_type: str
    customer_id: str
    customer_email: str
    subscription_id: str
    price_id: str
    product_id: str
    new_status: str    # 'active' | 'canceled' | 'past_due' | 'unknown'


def _new_status_for(event_type: str) -> str:
    """Pure: map Stripe event_type → our subscription.status value."""
    return {
        "checkout.session.completed":     "active",
        "invoice.payment_succeeded":      "active",
        "customer.subscription.updated":  "active",
        "customer.subscription.deleted":  "canceled",
        "invoice.payment_failed":         "past_due",
    }.get(event_type, "unknown")


def normalize_stripe_event(event: dict[str, Any]) -> StripeNormalizedEvent | None:
    """Pure: Stripe event JSON → typed StripeNormalizedEvent, or None on
    unhandled / malformed input.

    Real Stripe payloads vary across event types — we navigate to the
    fields by event-type-specific paths. Defensive on every lookup so a
    schema drift produces None (route handler ignores), not an exception
    (route handler 500s).
    """
    if not isinstance(event, dict):
        return None
    event_type = event.get("type")
    if not isinstance(event_type, str) or event_type not in HANDLED_EVENT_TYPES:
        return None
    data = event.get("data")
    if not isinstance(data, dict):
        return None
    obj = data.get("object")
    if not isinstance(obj, dict):
        return None

    # Field extraction varies by event type. Map carefully.
    customer_id = ""
    customer_email = ""
    subscription_id = ""
    price_id = ""
    product_id = ""

    if event_type == "checkout.session.completed":
        # Session object: customer, customer_email (top-level for guest),
        # subscription (id), display_items[0].price/product
        customer_id = str(obj.get("customer") or "")
        customer_email = str(
            obj.get("customer_email")
            or (obj.get("customer_details") or {}).get("email")
            or "",
        )
        subscription_id = str(obj.get("subscription") or "")
        # Stripe Checkout doesn't always expand line_items; price_id is
        # often in metadata when the integration sets it
        meta = obj.get("metadata") or {}
        price_id = str(meta.get("price_id") or "")
        product_id = str(meta.get("product_id") or "")

    elif event_type in (
        "invoice.payment_succeeded", "invoice.payment_failed",
    ):
        # Invoice object: customer, customer_email, subscription,
        # lines.data[0].price.id, lines.data[0].price.product
        customer_id = str(obj.get("customer") or "")
        customer_email = str(obj.get("customer_email") or "")
        subscription_id = str(obj.get("subscription") or "")
        lines = (obj.get("lines") or {}).get("data") or []
        if lines:
            price = (lines[0].get("price") or {})
            price_id = str(price.get("id") or "")
            product_id = str(price.get("product") or "")

    elif event_type in (
        "customer.subscription.deleted", "customer.subscription.updated",
    ):
        # Subscription object: customer, items.data[0].price.id/product
        customer_id = str(obj.get("customer") or "")
        subscription_id = str(obj.get("id") or "")
        items = (obj.get("items") or {}).get("data") or []
        if items:
            price = (items[0].get("price") or {})
            price_id = str(price.get("id") or "")
            product_id = str(price.get("product") or "")

    return StripeNormalizedEvent(
        event_type=event_type,
        customer_id=customer_id,
        customer_email=customer_email,
        subscription_id=subscription_id,
        price_id=price_id,
        product_id=product_id,
        new_status=_new_status_for(event_type),
    )


def stripe_price_to_tier(
    *, price_id: str, price_tier_map: dict[str, str],
) -> str | None:
    """Pure: operator-supplied price_id → our tier name.

    The map lives in settings (one entry per active price you sell on
    Stripe). Returns None for unknown prices so the route logs +
    ack-ignores rather than guessing.
    """
    if not price_id:
        return None
    return price_tier_map.get(price_id)


__all__ = [
    "HANDLED_EVENT_TYPES",
    "StripeNormalizedEvent",
    "normalize_stripe_event",
    "stripe_price_to_tier",
]
