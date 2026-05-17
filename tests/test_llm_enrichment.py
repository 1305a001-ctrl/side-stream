"""LLM enrichment pure-helper tests."""
from __future__ import annotations

from side_stream.llm_enrichment import format_sentiment_tag


def test_format_sentiment_tag_none_returns_empty():
    assert format_sentiment_tag(None) == ""


def test_format_sentiment_tag_low_confidence_suppressed():
    """Don't broadcast a guess — require ≥50% confidence."""
    assert format_sentiment_tag(
        {"label": "bullish", "confidence": 0.3, "rationale": "weak signal"},
    ) == ""


def test_format_sentiment_tag_bullish_with_rationale():
    out = format_sentiment_tag(
        {"label": "bullish", "confidence": 0.85, "rationale": "ETF inflows"},
    )
    assert "BULLISH" in out
    assert "85%" in out
    assert "ETF inflows" in out
    assert "📈" in out


def test_format_sentiment_tag_bearish():
    out = format_sentiment_tag(
        {"label": "bearish", "confidence": 0.7, "rationale": ""},
    )
    assert "BEARISH" in out
    assert "70%" in out
    assert "📉" in out


def test_format_sentiment_tag_neutral():
    out = format_sentiment_tag(
        {"label": "neutral", "confidence": 0.6, "rationale": "mixed"},
    )
    assert "NEUTRAL" in out
    assert "➖" in out


def test_format_sentiment_tag_at_50_percent_passes():
    """Exactly 50% should pass the gate (>=, not >)."""
    out = format_sentiment_tag(
        {"label": "bullish", "confidence": 0.5, "rationale": ""},
    )
    assert "BULLISH" in out
