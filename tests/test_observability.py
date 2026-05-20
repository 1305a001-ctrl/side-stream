"""A7 — observability (Sentry init) tests."""
from __future__ import annotations

import os

os.environ.setdefault("POSTGRES_DSN", "postgresql://test:test@localhost:5432/test")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")

from side_stream.observability import init_sentry


def test_init_sentry_silent_noop_when_dsn_empty():
    """Empty DSN is the correct dev/test behavior — no logs, no errors."""
    assert init_sentry(dsn="") is False


def test_init_sentry_returns_false_when_sdk_missing(monkeypatch, caplog):
    """If sentry-sdk isn't installed but DSN is set, log warn + return False.

    Simulates the missing-dep case by patching the import in the module's
    runtime — sentry_sdk is imported INSIDE init_sentry, so we can swap
    sys.modules to trigger ImportError on the import attempt.
    """
    import builtins as _builtins
    real_import = _builtins.__import__

    def _fail_sentry(name, *args, **kwargs):
        if name == "sentry_sdk":
            raise ImportError("simulated missing sentry-sdk")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(_builtins, "__import__", _fail_sentry)

    import logging
    caplog.set_level(logging.WARNING)
    assert init_sentry(dsn="https://fake@sentry.io/123") is False
    assert any("sentry_sdk_missing" in r.message for r in caplog.records)
