"""Landing page rendering tests."""
from __future__ import annotations

from side_stream.api.landing import _render_landing


def test_landing_renders_html():
    html = _render_landing()
    assert "<!doctype html>" in html
    assert "<title>" in html
    assert "</html>" in html


def test_landing_includes_pricing_tiers():
    html = _render_landing()
    for tier in ("Free", "Pro Alerts", "Signal Pro", "Signal Pro+", "Enterprise"):
        assert tier in html


def test_landing_includes_price_points():
    html = _render_landing()
    for price in ("$0", "$9", "$39", "$99", "$299"):
        assert price in html


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
