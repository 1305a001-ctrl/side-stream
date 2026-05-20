"""LLM enrichment for outbound signals.

Optional sentiment tag attached to broadcasts — calls local-llm
/sentiment for the underlying asset. Used to give Pro+ subscribers a
contextual "bullish/bearish/neutral" alongside the raw signal.

Fail-OPEN: if local-llm is unreachable, returns None and the broadcast
proceeds without the tag. We never want LLM downtime to block signal
flow.
"""
from __future__ import annotations

import logging
from typing import Any

import httpx

from side_stream.settings import settings

log = logging.getLogger(__name__)


# Optional sentinel: when local_llm_base_url is empty (default), the
# enricher short-circuits to a no-op. Operator opts in via env.


async def fetch_sentiment(
    *, headline: str, asset: str, body: str = "",
) -> dict[str, Any] | None:
    """Returns {"label","confidence","rationale"} or None on any failure.

    Used by the broadcast pipeline to attach a directional tag to news-
    driven signals. Pure-async; safe to call from inside the pusher
    loop without blocking.
    """
    base = (settings.local_llm_base_url or "").rstrip("/")
    if not base:
        return None
    url = f"{base}/sentiment"
    try:
        async with httpx.AsyncClient(timeout=settings.local_llm_timeout_sec) as client:
            resp = await client.post(
                url,
                json={"headline": headline, "body": body, "asset": asset},
            )
    except httpx.HTTPError as e:
        log.debug("llm_sentiment.http_err err=%s", e)
        return None

    if resp.status_code != 200:
        log.debug("llm_sentiment.non_200 status=%d", resp.status_code)
        return None

    try:
        result = resp.json()
    except ValueError:
        return None

    if not result.get("ok", True):
        return None
    return {
        "label": str(result.get("label") or "neutral"),
        "confidence": float(result.get("confidence") or 0.0),
        "rationale": str(result.get("rationale") or ""),
    }


def format_sentiment_tag(sentiment: dict[str, Any] | None) -> str:
    """Pure: render a one-line sentiment tag for inclusion in a broadcast.

    Returns empty string when sentiment is None or low-confidence (so
    we never broadcast a confused signal).
    """
    if not sentiment:
        return ""
    label = sentiment.get("label", "neutral")
    conf = float(sentiment.get("confidence", 0.0))
    if conf < 0.5:
        return ""
    icon = {"bullish": "📈", "bearish": "📉", "neutral": "➖"}.get(label, "")
    rationale = sentiment.get("rationale", "")
    if rationale:
        return f"{icon} *{label.upper()}* ({conf:.0%}) — _{rationale}_"
    return f"{icon} *{label.upper()}* ({conf:.0%})"


__all__ = ["fetch_sentiment", "format_sentiment_tag"]
