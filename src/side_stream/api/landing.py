"""Public marketing/landing page.

Single static HTML file rendered server-side. Pre-launch goal: convert
visitors → Free TG channel join → Standard ($29 founding / $49) → Pro ($99).

Refactored 2026-05-20 to the eval-doc 3-tier model (was 5 tiers):
the eval doc finding was that >3 tiers drops Whop conversion 15-20%.

Tone: confident, technical, not hype. Real numbers, real source
attribution. We're selling to traders who can sniff out marketing fluff.
"""
from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

from side_stream.settings import settings

router = APIRouter()


def _render_landing() -> str:
    """Pure: build the landing HTML using current settings (brand, channel URL)."""
    brand = settings.brand_name or "Streams Edge"
    free_url = (
        settings.public_channel_url
        or "https://t.me/streamsedge_free"
    )
    pro_url = (
        settings.signal_pro_group_url
        or "#signup"
    )

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>{brand} — sub-second crypto + equity signals</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="description" content="Sub-second crypto + equity signals from a multi-strategy fleet. Free tier on Telegram. Standard $49/mo or $29 founding (capped at 50).">
<style>
  :root {{ --bg:#0b0b10; --fg:#eaeaee; --mute:#9ea0ad; --accent:#6df0c8; --card:#15151c; --border:#22232c; }}
  * {{ box-sizing:border-box }}
  body {{ margin:0; font-family:-apple-system,system-ui,Segoe UI,Roboto,sans-serif; background:var(--bg); color:var(--fg); line-height:1.5 }}
  .container {{ max-width:1100px; margin:0 auto; padding:48px 24px }}
  nav {{ display:flex; justify-content:space-between; align-items:center; padding:24px 0 }}
  nav .brand {{ font-weight:700; font-size:1.2rem }}
  nav a {{ color:var(--mute); text-decoration:none; margin-left:24px }}
  nav a:hover {{ color:var(--fg) }}
  h1 {{ font-size:clamp(2rem, 4.5vw, 3.5rem); line-height:1.1; margin:48px 0 16px }}
  h1 .accent {{ color:var(--accent) }}
  h2 {{ font-size:1.8rem; margin:48px 0 16px }}
  h3 {{ font-size:1.2rem; margin:24px 0 8px }}
  p.lede {{ font-size:1.2rem; color:var(--mute); max-width:680px }}
  .pillrow {{ display:flex; flex-wrap:wrap; gap:10px; margin:24px 0 }}
  .pill {{ background:var(--card); border:1px solid var(--border); padding:6px 14px; border-radius:999px; font-size:0.85rem; color:var(--mute) }}
  .cta {{ display:inline-block; background:var(--accent); color:#0b0b10; padding:14px 28px; border-radius:8px; font-weight:600; text-decoration:none; margin:16px 8px 0 0 }}
  .cta.secondary {{ background:transparent; color:var(--fg); border:1px solid var(--border) }}
  .grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(280px,1fr)); gap:16px; margin:24px 0 }}
  .card {{ background:var(--card); border:1px solid var(--border); border-radius:12px; padding:24px }}
  .price {{ font-size:2rem; font-weight:700; margin:8px 0 4px }}
  .price small {{ font-size:0.9rem; color:var(--mute); font-weight:400 }}
  .features {{ list-style:none; padding:0; margin:16px 0; color:var(--mute) }}
  .features li {{ padding:6px 0 }}
  .features li::before {{ content:"→ "; color:var(--accent) }}
  .quietnote {{ font-size:0.85rem; color:var(--mute); margin:16px 0 }}
  footer {{ border-top:1px solid var(--border); margin-top:64px; padding-top:32px; color:var(--mute); font-size:0.9rem }}
  details {{ background:var(--card); border:1px solid var(--border); border-radius:8px; padding:16px 20px; margin:12px 0 }}
  details summary {{ cursor:pointer; font-weight:600 }}
  .stat-grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(160px,1fr)); gap:16px; margin:24px 0 }}
  .stat {{ background:var(--card); border:1px solid var(--border); border-radius:8px; padding:20px; text-align:center }}
  .stat .n {{ font-size:1.6rem; font-weight:700; color:var(--accent) }}
  .stat .l {{ font-size:0.85rem; color:var(--mute) }}
</style>
</head>
<body>
<div class="container">
  <nav>
    <div class="brand">{brand}</div>
    <div>
      <a href="#how">How it works</a>
      <a href="#pricing">Pricing</a>
      <a href="#faq">FAQ</a>
      <a href="{free_url}" class="cta" style="padding:8px 16px; margin:0">Free Telegram →</a>
    </div>
  </nav>

  <h1>Sub-second <span class="accent">crypto + equity</span> signals<br>from a 100-strategy fleet</h1>
  <p class="lede">
    Real-time alerts on liquidation cascades, off-market arbitrage,
    oracle-lag mispricings, and pre-settlement momentum on Polymarket.
    Free public channel for the headlines. Pro tiers for the full feed.
  </p>

  <div class="pillrow">
    <span class="pill">Chainlink Data Streams</span>
    <span class="pill">GMX V2 keeper feed</span>
    <span class="pill">Aave V3 liq scanner</span>
    <span class="pill">Polymarket cross-market</span>
    <span class="pill">After-hours equity arb</span>
  </div>

  <a href="{free_url}" class="cta">Join free Telegram</a>
  <a href="{pro_url}" class="cta secondary">See Pro tiers ↓</a>

  <h2 id="how">What you get</h2>

  <div class="stat-grid">
    <div class="stat"><div class="n">100+</div><div class="l">live strategies</div></div>
    <div class="stat"><div class="n">7 feeds</div><div class="l">Chainlink Data Streams</div></div>
    <div class="stat"><div class="n">3 venues</div><div class="l">Polymarket · GMX · Aave</div></div>
    <div class="stat"><div class="n">&lt;500ms</div><div class="l">signal-to-alert p95</div></div>
  </div>

  <div class="grid">
    <div class="card">
      <h3>Sub-second alerts</h3>
      <p class="quietnote">
        Chainlink Data Streams deliver oracle updates faster than the
        on-chain price tick. We turn that 0.5-2 sec lead into trigger
        events delivered to your Telegram / Discord / webhook within
        500ms of the feed update.
      </p>
    </div>
    <div class="card">
      <h3>Multi-venue coverage</h3>
      <p class="quietnote">
        Polymarket settlement-day momentum. Off-market-hours equity gaps
        (CME futures vs xStocks). GMX V2 liquidation triggers. Aave V3
        underwater positions. All from one stream.
      </p>
    </div>
    <div class="card">
      <h3>Strategy transparency</h3>
      <p class="quietnote">
        Every signal is tagged with the strategy that generated it.
        Every Pro card carries a 30-day Sharpe + PnL + closes footer.
        We pause publishing when Sharpe drops below 1.0 — quality
        is gated, not assumed.
      </p>
    </div>
  </div>

  <h2 id="live">Live quality (verified)</h2>

  <p class="quietnote" style="margin-bottom:16px">
    Refreshed every 60 seconds from our trading book. We pause publishing
    when Sharpe drops below 1.0 — open the book, every minute.
  </p>

  <div id="leaderboard" class="grid">
    <div class="card">
      <h3>Loading live data…</h3>
      <p class="quietnote">If this never loads, our API may be paused —
      every signal channel goes silent during maintenance.</p>
    </div>
  </div>

  <script>
  (async function loadQuality() {{
    try {{
      const r = await fetch('/v1/dashboard/quality');
      if (!r.ok) throw new Error('http ' + r.status);
      const j = await r.json();
      const wrap = document.getElementById('leaderboard');
      const snaps = j.snapshots || {{}};
      const slugs = Object.keys(snaps);
      if (slugs.length === 0) {{
        wrap.innerHTML = '<div class="card"><h3>Snapshots warming up</h3>'
          + '<p class="quietnote">Live data publishes every 60 seconds. '
          + 'Refresh in a moment.</p></div>';
        return;
      }}
      wrap.innerHTML = slugs.map(slug => {{
        const s = snaps[slug];
        const sharpe = s.sharpe == null ? '?' : Number(s.sharpe).toFixed(2);
        const pnl = (s.total_pnl_usd >= 0 ? '+$' : '-$') +
                    Math.abs(Number(s.total_pnl_usd)).toLocaleString('en-US', {{maximumFractionDigits:0}});
        const wr = (Number(s.win_rate) * 100).toFixed(1);
        return `<div class="card">
          <h3>${{slug}}</h3>
          <ul class="features">
            <li><strong>Sharpe (${{s.window_days}}d):</strong> ${{sharpe}}</li>
            <li><strong>PnL:</strong> ${{pnl}}</li>
            <li><strong>Closes:</strong> ${{s.n_closed}}</li>
            <li><strong>Win rate:</strong> ${{wr}}%</li>
          </ul>
        </div>`;
      }}).join('');
    }} catch (e) {{
      // Fail silent — landing should never look broken
      console.debug('quality load failed', e);
    }}
  }})();
  </script>

  <h2 id="pricing">Pricing</h2>

  <div class="grid">
    <div class="card">
      <h3>Free</h3>
      <div class="price">$0</div>
      <ul class="features">
        <li>Public Telegram channel</li>
        <li>5-min delayed top-3/day</li>
        <li>No custom triggers</li>
      </ul>
      <a href="{free_url}" class="cta secondary">Join free</a>
    </div>

    <div class="card">
      <h3>Standard</h3>
      <div class="price">$49 <small>/ month</small></div>
      <p class="quietnote" style="margin:4px 0 12px">
        <strong>$29/mo founding</strong> — first 50 subs only, closes permanently after.
      </p>
      <ul class="features">
        <li>Private TG group: every chainlink-lag emit, real-time</li>
        <li>Up to 100 custom price-cross triggers</li>
        <li>Webhook · Discord · Slack · email delivery</li>
        <li>Sub-second from Chainlink Data Streams</li>
        <li>30-day Sharpe + PnL footer on every signal card</li>
      </ul>
      <a href="{pro_url}" class="cta">Start Standard</a>
    </div>

    <div class="card">
      <h3>Pro</h3>
      <div class="price">$99 <small>/ month</small></div>
      <ul class="features">
        <li>Everything in Standard</li>
        <li>Up to 500 custom triggers</li>
        <li>GMX whale-liquidation alerts</li>
        <li>News-driven Polymarket triggers</li>
        <li>Weekly Sunday write-up: which signals won, which lost, why</li>
      </ul>
      <a href="{pro_url}" class="cta">Start Pro</a>
    </div>
  </div>

  <h2 id="faq">FAQ</h2>

  <details>
    <summary>Where do signals come from?</summary>
    <p>A multi-strategy fleet running on bare-metal hardware in our
    operator's home rack. Strategies consume Chainlink Data Streams,
    Polymarket Gamma/CLOB, GMX V2 subgraph, and Aave V3 reserves. Each
    strategy is open-sourced in our public GitHub orgs.</p>
  </details>

  <details>
    <summary>What's your track record?</summary>
    <p>Paper-mode track record is published on the Pro+ dashboard:
    Brier per strategy, Sharpe-to-date, signal-to-outcome scoring.
    We don't publish live PnL until at least 30 closed live trades
    per strategy — early data is noise.</p>
  </details>

  <details>
    <summary>How is this different from a Twitter alpha caller?</summary>
    <p>Three differences: (1) signals are generated by code with full
    audit trail, (2) latency is sub-second, not minutes, (3) we publish
    every signal including the misses — see Brier rollups.</p>
  </details>

  <details>
    <summary>Can I cancel anytime?</summary>
    <p>Yes. Cancel via Whop/Stripe dashboard, takes effect at end of
    current billing period.</p>
  </details>

  <footer>
    {brand} · operated by 1305a001 LLC ·
    <a href="{free_url}" style="color:var(--mute)">free channel</a> ·
    <a href="https://github.com/1305a001-ctrl" style="color:var(--mute)">github</a> ·
    not investment advice
  </footer>
</div>
</body>
</html>"""


@router.get("/", response_class=HTMLResponse)
async def landing() -> str:
    return _render_landing()


__all__ = ["router"]
