"""Observability — Sentry init (A7, eval doc 2026-05-20).

Why this exists: eval doc found Sentry free tier (5k events/mo) covers
a 100-sub product. Without it, the first refund-class bug is invisible
until customers tweet about it.

Defensive contract:
  - If ``sentry_dsn`` is empty → init is a silent no-op (returns False).
  - If ``sentry-sdk`` package isn't installed → log warning + no-op.
  - If both are present → init Sentry and return True.

This lets the same code run in dev (no DSN), production (DSN + sdk),
and CI (no sdk in the slim image) without conditional imports at call sites.
"""
from __future__ import annotations

import logging

log = logging.getLogger(__name__)


def init_sentry(
    *,
    dsn: str,
    environment: str = "production",
    traces_sample_rate: float = 0.1,
    component: str = "side-stream",
) -> bool:
    """Initialize Sentry if dsn + sentry-sdk are both available.

    Returns True if initialized (a single info log line), False if
    skipped silently (empty DSN) or skipped with warning (sdk missing).

    Intentionally NOT raising on missing dsn — silent no-op is the
    correct dev/test behavior. Operator pastes DSN in
    /srv/secrets/side-stream.env on ai-primary to enable.
    """
    if not dsn:
        return False
    try:
        import sentry_sdk  # noqa: PLC0415 — defensive optional dep
    except ImportError:
        log.warning(
            "observability.sentry_sdk_missing — SENTRY_DSN set but "
            "sentry-sdk is not installed; skipping init",
        )
        return False
    sentry_sdk.init(
        dsn=dsn,
        environment=environment,
        traces_sample_rate=traces_sample_rate,
        # Never auto-capture user emails / PII.
        send_default_pii=False,
    )
    log.info(
        "observability.sentry_initialized component=%s env=%s sample_rate=%.2f",
        component, environment, traces_sample_rate,
    )
    return True


__all__ = ["init_sentry"]
