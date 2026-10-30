"""Play back one stock's spot on the Viz zone map, on any past session.

    uv run python tools/backtest_tba.py PLTR 2026-10-29 --out <dir>

Downloads daily bars live from Yahoo Finance, computes the same indicators the
pipeline publishes (daily EMA20, weekly EMA20 with a live weekly bar, ATR14,
tight range), classifies the zone with the export's own rules, and writes one
HTML page that draws the single dot on the zone map. Prints the reading as JSON.

The universe floors ($100M dollar volume, 4% ADR) are not applied: they choose
which names a tab shows, and here the name is given.
"""

from __future__ import annotations

import argparse
import html
import json
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd  # noqa: E402
import yfinance as yf  # noqa: E402

from src.indicators.create_technical_indicators import (  # noqa: E402
    compute_atr14, compute_ema_pair, compute_sma50_full, compute_tight_range,
    compute_weekly_ema, tight_range_config, weekly_ema_config,
)
from src.reporting.export_dashboard_data import (  # noqa: E402
    classify_zone, zone_distance, zone_map_config,
)

# The weekly EMA20 seeds on its first week. Over ~34 weeks the seed keeps ~3%
# weight, which visibly moves a volatile name's weekly distance, so read two
# years as the ETF Viz fetch does.
HISTORY_DAYS = 730

# docs/app.js ZONE_STATES labels, keyed by export_dashboard_data.ZONE_STATES ids.
STATE_LABELS = {
    'buy_area': 'In the buy area',
    'buy_stretched': 'Buy area, weekly stretched',
    'above': 'Above the buy area',
    'extended': 'Extended on both',
    'under_daily': 'Under the daily band',
    'weekly_broken': 'Weekly broken',
    'under_both': 'Under both',
}

FAMILY_CLASS = {'under': 'zm-under', 'band': 'zm-band', 'above': 'zm-above'}

# docs/app.js ZM_PAD / ZM_ASPECT / ZM_HEIGHT, at one fixed width.
WIDTH = 1000
PAD = {'l': 46, 'r': 14, 't': 40, 'b': 34}
HEIGHT = round(min(640, max(360, WIDTH * 0.62)))


def fetch_bars(ticker, session):
    """Daily OHLCV up to and including `session`, lower-case columns."""
    df = yf.download(ticker.replace('.', '-'), start=session - timedelta(days=HISTORY_DAYS),
                     end=session + timedelta(days=1), progress=False, threads=False)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0].lower() for c in df.columns]
    else:
        df = df.rename(columns=str.lower)
    df = df.dropna()
    return df[df.index.date <= session]


def zone_reading(ticker, bars, cfg):
    """The zone-map point for the last bar, as build_zone_map would publish it."""
    close = bars['close']
    ema10, ema20 = compute_ema_pair(close)
    ema20_w = compute_weekly_ema(close, **weekly_ema_config())
    atr14 = compute_atr14(bars['high'], bars['low'], close)
    # The pipeline's ADR% (calculate_technical_indicators), which the tight range reads.
    adr_pct = (bars['high'] / bars['low']).rolling(window=20, min_periods=10).mean() - 1
    tr = compute_tight_range(bars['low'], close, adr_pct, ema10, ema20,
                             compute_sma50_full(close), **tight_range_config())
    last = bars.index[-1]
    if pd.isna(ema20_w.iloc[-1]):
        raise SystemExit(f'{ticker}: fewer than 20 completed weeks before {last.date()}, '
                         'so there is no weekly EMA20 and no spot to plot.')
    d_daily = zone_distance(close.iloc[-1], ema20.iloc[-1], atr14.iloc[-1])
    d_weekly = zone_distance(close.iloc[-1], ema20_w.iloc[-1], atr14.iloc[-1])
    if d_daily is None or d_weekly is None:
        raise SystemExit(f'{ticker}: no daily distance on {last.date()}.')
    zone_daily, zone_weekly, state = classify_zone(d_daily, d_weekly, cfg)
    tight = bool(tr['tight_range'].iloc[-1])
    return {
        'ticker': ticker,
        'session': last.date().isoformat(),
        'close': round(float(close.iloc[-1]), 2),
        'ema20': round(float(ema20.iloc[-1]), 2),
        'ema20_w': round(float(ema20_w.iloc[-1]), 2),
        'atr14': round(float(atr14.iloc[-1]), 2),
        'd_daily': d_daily,
        'd_weekly': d_weekly,
        'zone_daily': zone_daily,
        'zone_weekly': zone_weekly,
        'state': state,
        'tight': tight,
        'tight_len': int(tr['tight_len'].iloc[-1]) if tight else None,
    }


def _f(v):
    return f'{v:.1f}'


def _signed(v):
    return f'{v:+.2f}'


def scatter_svg(t, cfg):
    """The zone map with one name on it: a port of zoneScatterSvg in docs/app.js."""
    x_range, y_range = cfg['x_range'], cfg['y_range']

    def X(v):
        return PAD['l'] + (v - x_range[0]) / (x_range[1] - x_range[0]) * (WIDTH - PAD['l'] - PAD['r'])

    def Y(v):
        return HEIGHT - PAD['b'] - (v - y_range[0]) / (y_range[1] - y_range[0]) * (HEIGHT - PAD['t'] - PAD['b'])

    xs = [X(v) for v in (x_range[0], cfg['band_low'], cfg['band_high'], x_range[1])]
    y_top, y_bot = Y(y_range[1]), Y(y_range[0])
    y_low, y_high = Y(cfg['weekly_low']), Y(cfg['weekly_high'])
    heads = {'under': 'Under the daily band', 'band': 'In the daily band', 'above': 'Above the daily band'}
    n_buy = 1 if t['state'] == 'buy_area' else 0

    svg = []
    for i, family in enumerate(FAMILY_CLASS):
        mid = (xs[i] + xs[i + 1]) / 2
        count = 1 if t['zone_daily'] == family else 0
        svg.append(f'<rect class="zm-bg {FAMILY_CLASS[family]}" x="{_f(xs[i])}" y="{_f(y_top)}" '
                   f'width="{_f(xs[i + 1] - xs[i])}" height="{_f(y_bot - y_top)}"/>')
        svg.append(f'<text class="zm-head" x="{_f(mid)}" y="{_f(y_top - 20)}">{heads[family]}</text>')
        svg.append(f'<text class="zm-axis" x="{_f(mid)}" y="{_f(y_top - 8)}">{count} of 1</text>')

    def rail(x1, y1, x2, y2, extra=''):
        return f'<line class="zm-rail{extra}" x1="{_f(x1)}" y1="{_f(y1)}" x2="{_f(x2)}" y2="{_f(y2)}"/>'

    svg.append(rail(xs[1], y_top, xs[1], y_bot) + rail(xs[2], y_top, xs[2], y_bot))
    svg.append(rail(xs[0], y_low, xs[3], y_low))
    svg.append(rail(xs[0], y_high, xs[3], y_high, ' zm-rail-dash'))
    svg.append(f'<text class="zm-axis zm-left" x="{_f(xs[0] + 4)}" y="{_f(y_low - 4)}">weekly EMA20</text>')
    svg.append(f'<text class="zm-axis zm-left" x="{_f(xs[0] + 4)}" y="{_f(y_high - 4)}">'
               f'weekly extended · &gt; +{cfg["weekly_high"]:g} ATR</text>')
    svg.append(f'<rect class="zm-box" x="{_f(xs[1])}" y="{_f(y_high)}" '
               f'width="{_f(xs[2] - xs[1])}" height="{_f(y_low - y_high)}"/>')
    tag = (f'<text class="zm-box-tag" x="{_f((xs[1] + xs[2]) / 2)}" y="{_f(y_high + 13)}">'
           f'Buy area · {n_buy}</text>')

    v = int(x_range[0])
    while v <= x_range[1]:
        svg.append(f'<text class="zm-axis" x="{_f(X(v))}" y="{_f(y_bot + 13)}">{f"+{v}" if v > 0 else v}</text>')
        v += 1
    v = int(y_range[0] // 2 * 2)
    while v <= y_range[1]:
        svg.append(f'<text class="zm-axis zm-right" x="{_f(xs[0] - 5)}" y="{_f(Y(v) + 3)}">'
                   f'{f"+{v}" if v > 0 else v}</text>')
        v += 2
    svg.append(f'<text class="zm-axis" x="{_f((xs[0] + xs[3]) / 2)}" y="{HEIGHT - 4}">'
               'daily: close vs EMA20, in ATR →</text>')
    svg.append(f'<text class="zm-axis" transform="translate(11 {_f((y_top + y_bot) / 2)}) rotate(-90)">'
               'weekly: close vs EMA20, in ATR →</text>')

    cx = min(x_range[1], max(x_range[0], t['d_daily']))
    cy = min(y_range[1], max(y_range[0], t['d_weekly']))
    px, py = X(cx), Y(cy)
    cls = FAMILY_CLASS[t['zone_daily']]
    if (cx, cy) != (t['d_daily'], t['d_weekly']):
        # Off-scale: a triangle on the nearest edge, pointing outward.
        s = 5
        if t['d_weekly'] > y_range[1]:
            pts = [(0, -s), (s, s), (-s, s)]
        elif t['d_weekly'] < y_range[0]:
            pts = [(0, s), (s, -s), (-s, -s)]
        elif t['d_daily'] > x_range[1]:
            pts = [(s, 0), (-s, s), (-s, -s)]
        else:
            pts = [(-s, 0), (s, s), (s, -s)]
        shape = (f'<polygon class="zm-dot {cls}" points="'
                 + ' '.join(f'{_f(px + dx)},{_f(py + dy)}' for dx, dy in pts) + '"/>')
    else:
        shape = f'<circle class="zm-dot {cls}" cx="{_f(px)}" cy="{_f(py)}" r="4"/>'
    r = 7 if t['tight'] else 5
    ring = f'<circle class="zm-ring" cx="{_f(px)}" cy="{_f(py)}" r="7"/>' if t['tight'] else ''

    # One label, beside the dot; on its left when the right side runs out of plot.
    label_w = len(t['ticker']) * 6.2 + 2
    lx = px + r + 2 if px + r + 2 + label_w <= xs[3] - 1 else px - r - 2 - label_w
    buy = ' zm-label-buy' if t['state'] == 'buy_area' else ''
    tip = (f'{t["ticker"]} · {STATE_LABELS[t["state"]]}\n'
           f'Daily {_signed(t["d_daily"])} ATR · Weekly {_signed(t["d_weekly"])} ATR'
           + (f'\nTight range {t["tight_len"]}d' if t['tight'] else ''))
    title = f'<title>{html.escape(tip)}</title>'
    label = f'<text class="zm-label{buy}" x="{_f(lx + 1)}" y="{_f(py + 3.5)}">{html.escape(t["ticker"])}</text>'
    return ''.join(svg) + f'<g class="zm-pt">{title}{ring}{shape}</g>' + tag + f'<g class="zm-pt">{title}{label}</g>'


# Tokens and zm-* rules from docs/style.css.
STYLE = """
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=Inter:wght@300;400&display=swap');
:root { --bg: #000000; --border2: #262626; --green: #00e676; --gdim: rgba(0, 230, 118, 0.09);
  --red: #ff3355; --rdim: rgba(255, 51, 85, 0.09); --amber: #ffb300; --adim2: rgba(255, 179, 0, 0.09);
  --tight: #b085f5; --text: #eeeeee; --text2: #888888; --text3: #555555;
  --mono: 'IBM Plex Mono', monospace; --display: 'Inter', sans-serif; }
body { margin: 0; padding: 16px; background: var(--bg); color: var(--text2); font-family: var(--mono); }
.zm-svg { display: block; font-family: var(--mono); max-width: 100%; height: auto; }
.zm-bg { stroke: none; }
.zm-bg.zm-under { fill: var(--rdim); }
.zm-bg.zm-band { fill: var(--gdim); }
.zm-bg.zm-above { fill: var(--adim2); }
.zm-dot { stroke: var(--bg); stroke-width: 1; }
.zm-dot.zm-under { fill: var(--red); }
.zm-dot.zm-band { fill: var(--green); }
.zm-dot.zm-above { fill: var(--amber); }
.zm-ring { fill: none; stroke: var(--tight); stroke-width: 1.5; }
.zm-rail { stroke: var(--border2); stroke-width: 1; }
.zm-rail.zm-rail-dash { stroke-dasharray: 5 4; }
.zm-box { fill: none; stroke: var(--green); stroke-width: 1.2; }
.zm-box-tag { fill: var(--green); font-size: 10px; text-anchor: middle; paint-order: stroke; stroke: var(--bg); stroke-width: 3px; stroke-linejoin: round; }
.zm-head { fill: var(--text2); font-family: var(--display); font-size: 10px; text-anchor: middle; }
.zm-axis { fill: var(--text3); font-size: 9px; text-anchor: middle; }
.zm-axis.zm-left { text-anchor: start; }
.zm-axis.zm-right { text-anchor: end; }
.zm-label { fill: var(--text2); font-size: 10px; paint-order: stroke; stroke: var(--bg); stroke-width: 3px; stroke-linejoin: round; }
.zm-label.zm-label-buy { fill: var(--text); }
.zm-foot { margin: 8px 0 0; font-size: 11px; }
"""


def render_page(t, cfg):
    foot = (f'{t["ticker"]} · {t["session"]} · {STATE_LABELS[t["state"]]} · '
            f'daily {_signed(t["d_daily"])} ATR · weekly {_signed(t["d_weekly"])} ATR'
            + (f' · tight range {t["tight_len"]}d' if t['tight'] else ''))
    return (f'<!doctype html><html><head><meta charset="utf-8">'
            f'<title>{t["ticker"]} {t["session"]} zone map</title><style>{STYLE}</style></head><body>'
            f'<svg class="zm-svg" viewBox="0 0 {WIDTH} {HEIGHT}" width="{WIDTH}" height="{HEIGHT}">'
            f'{scatter_svg(t, cfg)}</svg><p class="zm-foot">{html.escape(foot)}</p></body></html>')


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('ticker')
    parser.add_argument('date', type=date.fromisoformat, help='YYYY-MM-DD; a non-trading day reads the session before it')
    parser.add_argument('--out', type=Path, default=Path(tempfile.gettempdir()) / 'backtest_tba',
                        help='directory for the HTML page')
    args = parser.parse_args()
    ticker = args.ticker.upper()

    bars = fetch_bars(ticker, args.date)
    if bars.empty:
        raise SystemExit(f'{ticker}: Yahoo returned no bars on or before {args.date}.')
    cfg = zone_map_config()
    reading = zone_reading(ticker, bars, cfg)

    args.out.mkdir(parents=True, exist_ok=True)
    page = args.out / f'backtest_tba_{ticker}_{reading["session"]}.html'
    page.write_text(render_page(reading, cfg), encoding='utf-8')
    print(json.dumps({**reading, 'state_label': STATE_LABELS[reading['state']], 'page': str(page)}, indent=2))


if __name__ == '__main__':
    main()
