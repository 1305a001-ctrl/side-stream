"""LLM signal validator — A5 (eval doc 2026-05-20 differentiator D5).

Pre-broadcast quality filter. Every Pro-tier signal POSTs to the local-llm
``/validate-signal`` endpoint (LIVE on ai-edge:8030 per memory) before being
broadcast. The LLM returns:

  {
    "looks_real": true | false,
    "risk_tag":   "impossible_price" | "stale_price" | "ok" | ...,
    "reason":     "..."
  }

If ``looks_real`` is False OR ``risk_tag`` is in the reject-tag set
(default {"impossible_price", "stale_price"}), the signal is dropped
and logged to ``signals:rejected:llm`` Redis stream for audit.

Failure mode: any HTTP / timeout / parse error returns None from
``llm_validate_signal``, which the pure helper treats as a NON-rejection
(fail-OPEN). The LLM is a quality filter, not a hard gate — the brand
promise of "we don't publish when it's not working" lives in A4
(calibration snapshot) and A1 (halt cascade), not here.

Cost: ~6.3s per /validate-signal call per smoke test in memory. Pro-tier
emit rate is single-digit per hour from chainlink_lag, so latency is
acceptable. If latency becomes a problem, raise timeout + add a thread
pool worker.
"""
from __future__ import annotations

import logging
from typing import Any

import httpx

log = logging.getLogger(__name__)


DEFAULT_REJECT_TAGS: frozenset[str] = frozenset({"impossible_price", "stale_price"})


# ─── Pure helpers ──────────────────────────────────────────────────────


def parse_reject_tags_csv(csv: str) -> set[str]:
    """Pure: parse the settings CSV into a tag set.

    Defensive: empty entries dropped, whitespace stripped.
    Defaults to DEFAULT_REJECT_TAGS when csv is empty.
    """
    tags = {t.strip() for t in (csv or "").split(",") if t.strip()}
    return tags or set(DEFAULT_REJECT_TAGS)


def should_reject_llm_validation(
    response: dict[str, Any] | None,
    *,
    reject_tags: set[str],
) -> tuple[bool, str]:
    """Pure: should this LLM response reject the signal?

    Returns ``(reject, reason)``. Cases:

      response is None        → (False, "no_response_unforced")  ← fail OPEN
                                 (LLM is unreachable; let signal through)
      looks_real is False     → (True,  "looks_real_false")
      risk_tag in reject_tags → (True,  "risk_tag:<tag>")
      otherwise               → (False, "ok")
    """
    if response is None:
        return False, "no_response_unforced"
    looks_real = response.get("looks_real")
    # Default looks_real=True when missing — fail-open inside a response
    # (the LLM gave us SOMETHING; the absence of an explicit veto is
    # not a veto).
    if looks_real is False:
        return True, "looks_real_false"
    risk_tag = response.get("risk_tag")
    if isinstance(risk_tag, str) and risk_tag in reject_tags:
        return True, f"risk_tag:{risk_tag}"
    return False, "ok"


# ─── I/O ───────────────────────────────────────────────────────────────


async def llm_validate_signal(
    http_client: httpx.AsyncClient,
    *,
    base_url: str,
    norm: dict[str, Any],
    context: str,
    timeout_sec: float,
) -> dict[str, Any] | None:
    """POST to ``<base_url>/validate-signal``.

    Returns the parsed JSON response, or None on any HTTP/timeout/parse
    error (fail-OPEN at the caller via should_reject_llm_validation).

    The signal payload sent to the LLM is the normalized `norm` dict
    (the same shape the broadcast formatter sees). ``context`` is a
    short hint like "chainlink_lag" — the LLM uses it for prompt
    framing.
    """
    if not base_url:
        return None
    try:
        resp = await http_client.post(
            f"{base_url.rstrip('/')}/validate-signal",
            json={"signal": norm, "context": context},
            timeout=timeout_sec,
        )
    except httpx.HTTPError as e:
        log.warning("llm_validator.http_error err=%s", e)
        return None
    if resp.status_code >= 400:
        log.warning("llm_validator.http_%d body=%s", resp.status_code, resp.text[:200])
        return None
    try:
        body = resp.json()
    except (ValueError, TypeError):
        log.warning("llm_validator.parse_failed body=%s", resp.text[:200])
        return None
    return body if isinstance(body, dict) else None


__all__ = [
    "DEFAULT_REJECT_TAGS",
    "llm_validate_signal",
    "parse_reject_tags_csv",
    "should_reject_llm_validation",
]
