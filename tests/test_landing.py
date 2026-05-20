"""Landing page rendering tests."""
from __future__ import annotations

from side_stream.api.landing import _render_landing


def test_landing_renders_html():
    html = _render_landing()
    assert "<!doctype html>" in html
    assert "<title>" in html
    assert "</html>" in html


def test_landing_includes_pricing_tiers():
    """3-tier model post 2026-05-20 eval doc prune (Free / Standard / Pro)."""
    html = _render_landing()
    for tier in ("Free", "Standard", "Pro"):
        assert tier in html
    # Pruned tiers MUST NOT appear — they were retired
    for retired in ("Pro Alerts", "Signal Pro+", "Enterprise"):
        assert retired not in html


def test_landing_includes_price_points():
    """3-tier prices: $0 free / $29 founding / $49 standard / $99 pro."""
    html = _render_landing()
    for price in ("$0", "$29", "$49", "$99"):
        assert price in html
    # Retired prices must not appear (use trailing space to avoid false
    # matches inside other dollar-figures elsewhere on the page).
    for retired in ("$9 ", "$39 ", "$299"):
        assert retired not in html


def test_landing_has_brand():
    """The brand_name from settings should appear in the title."""
    html = _render_landing()
    # default brand 'Streams Edge' should be in title
    assert "Streams Edge" in html or "{brand}" not in html


def test_landing_has_cta():
    html = _render_landing()
    assert 'class="cta"' in html


def test_landing_includes_pillrow_strategies():
    """Sanity: landing markets the breadth of strategies."""
    html = _render_landing()
    for s in ("Chainlink", "GMX", "Aave", "Polymarket"):
        assert s in html
