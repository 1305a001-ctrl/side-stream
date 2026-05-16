"""Tests for webhook_security.py — Whop + Stripe HMAC verification."""
# ruff: noqa: S105, S106  — test fixtures use fake secrets, not real creds
from __future__ import annotations

import hashlib
import hmac
import os

os.environ.setdefault("POSTGRES_DSN", "postgresql://test:test@localhost:5432/test")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")

from side_stream.webhook_security import (  # noqa: E402
    _parse_stripe_signature_header,
    verify_stripe_signature,
    verify_whop_signature,
)


def _make_whop_sig(secret: str, body: bytes) -> str:
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def _make_stripe_sig(secret: str, ts: int, body: bytes) -> str:
    msg = f"{ts}.".encode() + body
    sig = hmac.new(secret.encode(), msg, hashlib.sha256).hexdigest()
    return f"t={ts},v1={sig}"


# ─── Whop ──────────────────────────────────────────────────────────


def test_verify_whop_valid_signature():
    secret = "whsec_test123"
    body = b'{"event_type":"subscription.created"}'
    sig = _make_whop_sig(secret, body)
    result = verify_whop_signature(body=body, signature_header=sig, secret=secret)
    assert result.valid is True
    assert result.reason == "ok"


def test_verify_whop_invalid_signature():
    body = b'{"foo":"bar"}'
    result = verify_whop_signature(
        body=body, signature_header="0" * 64, secret="whsec_test123",
    )
    assert result.valid is False
    assert result.reason == "signature_mismatch"


def test_verify_whop_missing_header():
    result = verify_whop_signature(
        body=b"{}", signature_header="", secret="whsec_test123",
    )
    assert result.valid is False
    assert result.reason == "header_missing"


def test_verify_whop_missing_secret():
    result = verify_whop_signature(
        body=b"{}", signature_header="abc", secret="",
    )
    assert result.valid is False
    assert result.reason == "secret_not_configured"


def test_verify_whop_constant_time_compare():
    """Ensure both cases work — confirms compare_digest pathway."""
    secret = "s"
    body = b"hello"
    sig = _make_whop_sig(secret, body)
    # Uppercase header should still match (case-insensitive)
    assert verify_whop_signature(
        body=body, signature_header=sig.upper(), secret=secret,
    ).valid is True


# ─── Stripe ────────────────────────────────────────────────────────


def test_parse_stripe_header_typical():
    header = "t=1700000000,v1=abc123"
    ts, sig = _parse_stripe_signature_header(header)
    assert ts == 1700000000
    assert sig == "abc123"


def test_parse_stripe_header_with_v0_legacy():
    header = "t=1700000000,v1=newsig,v0=oldsig"
    ts, sig = _parse_stripe_signature_header(header)
    assert ts == 1700000000
    assert sig == "newsig"


def test_parse_stripe_header_malformed_returns_zero():
    ts, sig = _parse_stripe_signature_header("garbage")
    assert ts == 0
    assert sig == ""


def test_parse_stripe_header_empty():
    ts, sig = _parse_stripe_signature_header("")
    assert ts == 0
    assert sig == ""


def test_verify_stripe_valid_signature():
    secret = "whsec_stripe_test"
    body = b'{"type":"checkout.session.completed"}'
    ts = 1_700_000_000
    header = _make_stripe_sig(secret, ts, body)
    result = verify_stripe_signature(
        body=body, signature_header=header,
        secret=secret, now_unix=ts + 10,
    )
    assert result.valid is True


def test_verify_stripe_replay_rejected():
    """Signature is valid but timestamp is too old → reject."""
    secret = "whsec_stripe_test"
    body = b'{"foo":"bar"}'
    old_ts = 1_700_000_000
    header = _make_stripe_sig(secret, old_ts, body)
    result = verify_stripe_signature(
        body=body, signature_header=header,
        secret=secret, now_unix=old_ts + 3600,   # 1 hour later
    )
    assert result.valid is False
    assert "timestamp_out_of_tolerance" in result.reason


def test_verify_stripe_signature_mismatch():
    secret = "whsec_stripe_test"
    body = b'{"a":1}'
    ts = 1_700_000_000
    bad_header = f"t={ts},v1=" + "0" * 64
    result = verify_stripe_signature(
        body=body, signature_header=bad_header,
        secret=secret, now_unix=ts + 10,
    )
    assert result.valid is False
    assert result.reason == "signature_mismatch"


def test_verify_stripe_malformed_header():
    result = verify_stripe_signature(
        body=b"{}", signature_header="not-a-valid-header",
        secret="s", now_unix=1700000000,
    )
    assert result.valid is False
    assert result.reason == "header_malformed"


def test_verify_stripe_missing_secret():
    result = verify_stripe_signature(
        body=b"{}", signature_header="t=1,v1=abc",
        secret="", now_unix=2,
    )
    assert result.valid is False
    assert result.reason == "secret_not_configured"


def test_verify_stripe_within_tolerance_boundary():
    """Exactly at the tolerance boundary should still pass."""
    secret = "s"
    body = b'{"x":1}'
    ts = 1_700_000_000
    header = _make_stripe_sig(secret, ts, body)
    result = verify_stripe_signature(
        body=body, signature_header=header,
        secret=secret, now_unix=ts + 300,   # exactly 5min
        tolerance_sec=300,
    )
    assert result.valid is True
