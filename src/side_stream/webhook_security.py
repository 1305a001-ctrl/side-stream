"""Webhook signature verification — HMAC-SHA256 for Whop + Stripe.

Both providers send a header containing a signature derived from the
shared webhook secret + request body. Forgery is trivial without
verification — anyone could POST to /v1/webhooks/whop and upgrade
themselves to enterprise tier. These helpers gate the webhook routes
so only the real provider's payloads are honored.

Whop:
  Header:    X-Whop-Signature
  Algorithm: HMAC-SHA256(secret, body) → hex
  Compare:   header == computed (constant-time)

Stripe:
  Header:    Stripe-Signature: t=<unix_ts>,v1=<hex_sig>
  Signed:    f"{timestamp}.{body}"
  Algorithm: HMAC-SHA256(secret, signed_payload) → hex
  Compare:   v1 == computed (constant-time) AND ts within ±5 min tolerance

Pure helpers — no I/O. The route handler calls these with the raw
body bytes + the header string.
"""
from __future__ import annotations

import hashlib
import hmac
import logging
import time
from dataclasses import dataclass

log = logging.getLogger(__name__)


# Stripe's recommended tolerance: 5 minutes. Reject older signatures to
# limit replay-attack windows.
DEFAULT_STRIPE_TOLERANCE_SEC: int = 5 * 60


@dataclass(frozen=True)
class VerificationResult:
    """Outcome of a signature verification."""
    valid: bool
    reason: str  # 'ok' on success; failure reason otherwise


def _hmac_sha256_hex(*, secret: str, message: bytes) -> str:
    """Pure: hex-encoded HMAC-SHA256."""
    return hmac.new(
        key=secret.encode("utf-8"),
        msg=message,
        digestmod=hashlib.sha256,
    ).hexdigest()


def verify_whop_signature(
    *, body: bytes, signature_header: str, secret: str,
) -> VerificationResult:
    """Pure: verify Whop's X-Whop-Signature header.

    Returns VerificationResult(valid=True, reason='ok') on match.
    """
    if not secret:
        return VerificationResult(False, "secret_not_configured")
    if not signature_header:
        return VerificationResult(False, "header_missing")
    expected = _hmac_sha256_hex(secret=secret, message=body)
    # Constant-time compare — never use ==
    if hmac.compare_digest(signature_header.lower(), expected.lower()):
        return VerificationResult(True, "ok")
    return VerificationResult(False, "signature_mismatch")


def _parse_stripe_signature_header(header: str) -> tuple[int, str]:
    """Pure: 't=1700000000,v1=abc123,v0=...' → (timestamp, v1_signature).

    Returns (0, '') when the header is malformed.
    """
    if not header:
        return 0, ""
    ts = 0
    v1 = ""
    for part in header.split(","):
        if "=" not in part:
            continue
        k, _, v = part.partition("=")
        k = k.strip()
        v = v.strip()
        if k == "t":
            try:
                ts = int(v)
            except ValueError:
                ts = 0
        elif k == "v1":
            v1 = v
    return ts, v1


def verify_stripe_signature(
    *, body: bytes, signature_header: str, secret: str,
    now_unix: int | None = None,
    tolerance_sec: int = DEFAULT_STRIPE_TOLERANCE_SEC,
) -> VerificationResult:
    """Pure: verify Stripe's signature header per official docs.

    `now_unix` lets tests inject a deterministic clock; real callers
    pass None to use system time.
    """
    if not secret:
        return VerificationResult(False, "secret_not_configured")
    ts, v1 = _parse_stripe_signature_header(signature_header)
    if ts <= 0 or not v1:
        return VerificationResult(False, "header_malformed")
    if now_unix is None:
        now_unix = int(time.time())
    age = abs(now_unix - ts)
    if age > tolerance_sec:
        return VerificationResult(False, f"timestamp_out_of_tolerance:{age}s")
    signed_payload = f"{ts}.".encode() + body
    expected = _hmac_sha256_hex(secret=secret, message=signed_payload)
    if hmac.compare_digest(v1.lower(), expected.lower()):
        return VerificationResult(True, "ok")
    return VerificationResult(False, "signature_mismatch")


__all__ = [
    "DEFAULT_STRIPE_TOLERANCE_SEC",
    "VerificationResult",
    "verify_stripe_signature",
    "verify_whop_signature",
]
