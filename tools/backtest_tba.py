"""Play back stocks' spots on the Viz zone map, on any past session.

    uv run python tools/backtest_tba.py PLTR SNOW MSFT 2026-10-10 --out <dir>

Downloads daily bars live from Yahoo Finance, computes the same indicators the
pipeline publishes (daily EMA20, weekly EMA20 with a live weekly bar, ATR14,
tight range), classifies each zone with the export's own rules, and writes one
HTML page that draws every name on one zone map. Prints the readings as JSON.

A name Yahoo cannot serve, or one with too little history, is skipped and
listed; the rest still plot.

The universe floors ($100M dollar volume, 4% ADR) are not applied: they choose
which names a tab shows, and here the names are given.
"""

from __future__ import annotations

import argparse
import html
import json
import math
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

# docs/app.js ZM_FONT, ZM_LABEL and ZM_LABEL_PASSES: the label placer.
FONT = {'axis': 9, 'label': 10}
LABEL = {'h': 11, 'reach': 150, 'step': 6, 'angles': 24}
LABEL_PASSES = [
    {'dots': True, 'far': False, 'cross': False, 'under': False},
    {'dots': False, 'far': False, 'cross': False, 'under': False},
    {'dots': True, 'far': True, 'cross': False, 'under': False},
    {'dots': False, 'far': True, 'cross': True, 'under': False},
    {'dots': False, 'far': True, 'cross': True, 'under': True},
]


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
        raise ValueError(f'fewer than 20 completed weeks before {last.date()}, '
                         'so there is no weekly EMA20 and no spot to plot')
    d_daily = zone_distance(close.iloc[-1], ema20.iloc[-1], atr14.iloc[-1])
    d_weekly = zone_distance(close.iloc[-1], ema20_w.iloc[-1], atr14.iloc[-1])
    if d_daily is None or d_weekly is None:
        raise ValueError(f'no daily distance on {last.date()}')
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


def _text_w(text, px):
    # The dashboard measures with a canvas; this is its own fallback estimate.
    return len(text) * px * 0.62


def _overlap(a, b):
    return a[0] < b[0] + b[2] and b[0] < a[0] + a[2] and a[1] < b[1] + b[3] and b[1] < a[1] + a[3]


def _seg_hits_box(s, b):
    """True when segment s (x1, y1, x2, y2) touches box b (Liang-Barsky)."""
    dx, dy = s[2] - s[0], s[3] - s[1]
    p = (-dx, dx, -dy, dy)
    q = (s[0] - b[0], b[0] + b[2] - s[0], s[1] - b[1], b[1] + b[3] - s[1])
    t0, t1 = 0.0, 1.0
    for pi, qi in zip(p, q):
        if pi == 0:
            if qi < 0:
                return False
            continue
        r = qi / pi
        if pi < 0:
            if r > t1:
                return False
            t0 = max(t0, r)
        else:
            if r < t0:
                return False
            t1 = min(t1, r)
    return True


def _segs_cross(a, b):
    """True when two segments cross at a point inside both."""
    def side(x1, y1, x2, y2, x, y):
        v = (x2 - x1) * (y - y1) - (y2 - y1) * (x - x1)
        return (v > 0) - (v < 0)
    return (side(*a, b[0], b[1]) * side(*a, b[2], b[3]) < 0
            and side(*b, a[0], a[1]) * side(*b, a[2], a[3]) < 0)


def _label_spots(q, w, h, reach):
    """The label spots for one dot, nearest first: eight beside it, then rings that need a leader."""
    gap = q['r'] + 2
    d = gap * 0.75
    x, y = q['x'], q['y']
    for sx, sy in ((x + gap, y - h / 2), (x - gap - w, y - h / 2),
                   (x - w / 2, y - gap - h), (x - w / 2, y + gap),
                   (x + d, y - d - h), (x - d - w, y - d - h),
                   (x + d, y + d), (x - d - w, y + d)):
        yield sx, sy, False
    rad = gap + 10
    while rad <= reach:
        for k in range(LABEL['angles']):
            a = 2 * math.pi * k / LABEL['angles']
            c, s = math.cos(a), math.sin(a)
            px, py = x + rad * c, y + rad * s
            # Put the near edge of the box on the ring, so the leader stays short.
            yield (px if c > 0.3 else px - w if c < -0.3 else px - w / 2,
                   py if s > 0.3 else py - h if s < -0.3 else py - h / 2, True)
        rad += LABEL['step']


def _leader(q, box):
    """The leader from the edge of the dot to the nearest point of the box."""
    tx = min(box[0] + box[2], max(box[0], q['x']))
    ty = min(box[1] + box[3], max(box[1], q['y']))
    length = math.hypot(tx - q['x'], ty - q['y']) or 1
    return (q['x'] + (tx - q['x']) / length * q['r'], q['y'] + (ty - q['y']) / length * q['r'], tx, ty)


def _place_labels(pts, plot, placed):
    """Every name gets a label that covers no other label, as zoneScatterSvg places them.

    Buy-area names place first, then tight names, then the rest in the order
    given (the dashboard orders that rest by the tab's row number, which this
    tool does not compute).
    """
    leaders = []
    dots = [((q['x'] - q['r'], q['y'] - q['r'], 2 * q['r'], 2 * q['r']), q) for q in pts]
    order = sorted(pts, key=lambda q: 0 if q['t']['state'] == 'buy_area' else 1 if q['t']['tight'] else 2)
    for q in order:
        w = _text_w(q['t']['ticker'], FONT['label']) + 2
        h = LABEL['h']

        def fits(sx, sy, lead, rules):
            box = (sx, sy, w, h)
            if sx < plot[0] or sx + w > plot[1] or sy < plot[2] or sy + h > plot[3]:
                return None
            if any(_overlap(box, o) for o in placed):
                return None
            if rules['dots'] and any(dq is not q and _overlap(box, d) for d, dq in dots):
                return None
            if not rules['under'] and any(_seg_hits_box(s, box) for s in leaders):
                return None
            line = _leader(q, box) if lead else None
            if line and ((not rules['under'] and any(_seg_hits_box(line, o) for o in placed))
                         or (not rules['cross'] and any(_segs_cross(line, s) for s in leaders))):
                return None
            return box, line

        hit = None
        for rules in LABEL_PASSES:
            reach = max(WIDTH, HEIGHT) if rules['far'] else LABEL['reach']
            hit = next((r for r in (fits(*spot, rules) for spot in _label_spots(q, w, h, reach)) if r), None)
            if hit:
                break
        if not hit:
            x = q['x'] - q['r'] - 2 - w if q['x'] + q['r'] + 2 + w > plot[1] else q['x'] + q['r'] + 2
            hit = ((x, q['y'] - h / 2, w, h), None)
        q['box'], q['lead'] = hit
        placed.append(q['box'])
        if q['lead']:
            leaders.append(q['lead'])


def _triangle(t, px, py, x_range, y_range, cls):
    """An off-scale name: a triangle on the nearest edge, pointing outward."""
    s = 5
    if t['d_weekly'] > y_range[1]:
        pts = [(0, -s), (s, s), (-s, s)]
    elif t['d_weekly'] < y_range[0]:
        pts = [(0, s), (s, -s), (-s, -s)]
    elif t['d_daily'] > x_range[1]:
        pts = [(s, 0), (-s, s), (-s, -s)]
    else:
        pts = [(-s, 0), (s, s), (s, -s)]
    return (f'<polygon class="zm-dot {cls}" points="'
            + ' '.join(f'{_f(px + dx)},{_f(py + dy)}' for dx, dy in pts) + '"/>')


def scatter_svg(readings, cfg):
    """The zone map with the given names on it: a port of zoneScatterSvg in docs/app.js."""
    x_range, y_range = cfg['x_range'], cfg['y_range']

    def X(v):
        return PAD['l'] + (v - x_range[0]) / (x_range[1] - x_range[0]) * (WIDTH - PAD['l'] - PAD['r'])

    def Y(v):
        return HEIGHT - PAD['b'] - (v - y_range[0]) / (y_range[1] - y_range[0]) * (HEIGHT - PAD['t'] - PAD['b'])

    xs = [X(v) for v in (x_range[0], cfg['band_low'], cfg['band_high'], x_range[1])]
    y_top, y_bot = Y(y_range[1]), Y(y_range[0])
    y_low, y_high = Y(cfg['weekly_low']), Y(cfg['weekly_high'])
    heads = {'under': 'Under the daily band', 'band': 'In the daily band', 'above': 'Above the daily band'}
    n_buy = sum(t['state'] == 'buy_area' for t in readings)

    svg = []
    for i, family in enumerate(FAMILY_CLASS):
        mid = (xs[i] + xs[i + 1]) / 2
        count = sum(t['zone_daily'] == family for t in readings)
        svg.append(f'<rect class="zm-bg {FAMILY_CLASS[family]}" x="{_f(xs[i])}" y="{_f(y_top)}" '
                   f'width="{_f(xs[i + 1] - xs[i])}" height="{_f(y_bot - y_top)}"/>')
        svg.append(f'<text class="zm-head" x="{_f(mid)}" y="{_f(y_top - 20)}">{heads[family]}</text>')
        svg.append(f'<text class="zm-axis" x="{_f(mid)}" y="{_f(y_top - 8)}">{count} of {len(readings)}</text>')

    def rail(x1, y1, x2, y2, extra=''):
        return f'<line class="zm-rail{extra}" x1="{_f(x1)}" y1="{_f(y1)}" x2="{_f(x2)}" y2="{_f(y2)}"/>'

    low_text = 'weekly EMA20'
    high_text = f'weekly extended · > +{cfg["weekly_high"]:g} ATR'
    tag_text = f'Buy area · {n_buy}'
    svg.append(rail(xs[1], y_top, xs[1], y_bot) + rail(xs[2], y_top, xs[2], y_bot))
    svg.append(rail(xs[0], y_low, xs[3], y_low))
    svg.append(rail(xs[0], y_high, xs[3], y_high, ' zm-rail-dash'))
    svg.append(f'<text class="zm-axis zm-left" x="{_f(xs[0] + 4)}" y="{_f(y_low - 4)}">{html.escape(low_text)}</text>')
    svg.append(f'<text class="zm-axis zm-left" x="{_f(xs[0] + 4)}" y="{_f(y_high - 4)}">{html.escape(high_text)}</text>')
    svg.append(f'<rect class="zm-box" x="{_f(xs[1])}" y="{_f(y_high)}" '
               f'width="{_f(xs[2] - xs[1])}" height="{_f(y_low - y_high)}"/>')
    # Drawn above the dots, so no dot hides it.
    tag = (f'<text class="zm-box-tag" x="{_f((xs[1] + xs[2]) / 2)}" y="{_f(y_high + 13)}">'
           f'{html.escape(tag_text)}</text>')

    v = math.ceil(x_range[0])
    while v <= x_range[1]:
        svg.append(f'<text class="zm-axis" x="{_f(X(v))}" y="{_f(y_bot + 13)}">{f"+{v}" if v > 0 else v}</text>')
        v += 1
    v = math.ceil(y_range[0] / 2) * 2
    while v <= y_range[1]:
        svg.append(f'<text class="zm-axis zm-right" x="{_f(xs[0] - 5)}" y="{_f(Y(v) + 3)}">'
                   f'{f"+{v}" if v > 0 else v}</text>')
        v += 2
    svg.append(f'<text class="zm-axis" x="{_f((xs[0] + xs[3]) / 2)}" y="{HEIGHT - 4}">'
               'daily: close vs EMA20, in ATR →</text>')
    svg.append(f'<text class="zm-axis" transform="translate(11 {_f((y_top + y_bot) / 2)}) rotate(-90)">'
               'weekly: close vs EMA20, in ATR →</text>')

    # Positions. `r` is the space the dot (and its tight ring) takes.
    pts = []
    for t in readings:
        cx = min(x_range[1], max(x_range[0], t['d_daily']))
        cy = min(y_range[1], max(y_range[0], t['d_weekly']))
        pts.append({'t': t, 'x': X(cx), 'y': Y(cy), 'r': 7 if t['tight'] else 5,
                    'off': (cx, cy) != (t['d_daily'], t['d_weekly'])})

    tag_w = _text_w(tag_text, FONT['label'])
    fixed_text = [
        ((xs[1] + xs[2]) / 2 - tag_w / 2, y_high + 4, tag_w, 11),
        (xs[0] + 4, y_low - 12, _text_w(low_text, FONT['axis']), 10),
        (xs[0] + 4, y_high - 12, _text_w(high_text, FONT['axis']), 10),
    ]
    _place_labels(pts, (xs[0] + 1, xs[3] - 1, y_top + 1, y_bot - 1), fixed_text)

    # Two layers: dots and leaders first, then every label above them.
    marks, labels = [], []
    for q in pts:
        t = q['t']
        cls = FAMILY_CLASS[t['zone_daily']]
        tip = (f'{t["ticker"]} · {t["session"]} · {STATE_LABELS[t["state"]]}\n'
               f'Daily {_signed(t["d_daily"])} ATR · Weekly {_signed(t["d_weekly"])} ATR'
               + (f'\nTight range {t["tight_len"]}d' if t['tight'] else ''))
        title = f'<title>{html.escape(tip)}</title>'
        shape = (_triangle(t, q['x'], q['y'], x_range, y_range, cls) if q['off']
                 else f'<circle class="zm-dot {cls}" cx="{_f(q["x"])}" cy="{_f(q["y"])}" r="4"/>')
        ring = f'<circle class="zm-ring" cx="{_f(q["x"])}" cy="{_f(q["y"])}" r="7"/>' if t['tight'] else ''
        lead = (f'<line class="zm-leader" x1="{_f(q["lead"][0])}" y1="{_f(q["lead"][1])}" '
                f'x2="{_f(q["lead"][2])}" y2="{_f(q["lead"][3])}"/>') if q['lead'] else ''
        buy = ' zm-label-buy' if t['state'] == 'buy_area' else ''
        box = q['box']
        label = (f'<text class="zm-label{buy}" x="{_f(box[0] + 1)}" y="{_f(box[1] + box[3] / 2 + 3.5)}">'
                 f'{html.escape(t["ticker"])}</text>')
        marks.append(f'<g class="zm-pt">{title}{lead}{ring}{shape}</g>')
        labels.append(f'<g class="zm-pt">{title}{label}</g>')
    return ''.join(svg) + ''.join(marks) + tag + ''.join(labels)


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
.zm-leader { stroke: var(--text3); stroke-width: 0.75; }
.zm-label { fill: var(--text2); font-size: 10px; paint-order: stroke; stroke: var(--bg); stroke-width: 3px; stroke-linejoin: round; }
.zm-label.zm-label-buy { fill: var(--text); }
.zm-pt:hover .zm-label { fill: var(--text); }
.zm-pt:hover .zm-leader { stroke: var(--text2); }
.zm-foot { margin: 8px 0 0; font-size: 11px; line-height: 1.6; }
"""


def render_page(readings, skipped, cfg, asked):
    lines = [f'{t["ticker"]} · {t["session"]} · {STATE_LABELS[t["state"]]} · '
             f'daily {_signed(t["d_daily"])} ATR · weekly {_signed(t["d_weekly"])} ATR'
             + (f' · tight range {t["tight_len"]}d' if t['tight'] else '') for t in readings]
    lines += [f'{s["ticker"]} · not plotted: {s["reason"]}' for s in skipped]
    foot = '<br>'.join(html.escape(line) for line in lines)
    title = f'{" ".join(t["ticker"] for t in readings)} {asked} zone map'
    return (f'<!doctype html><html><head><meta charset="utf-8">'
            f'<title>{html.escape(title)}</title><style>{STYLE}</style></head><body>'
            f'<svg class="zm-svg" viewBox="0 0 {WIDTH} {HEIGHT}" width="{WIDTH}" height="{HEIGHT}">'
            f'{scatter_svg(readings, cfg)}</svg><p class="zm-foot">{foot}</p></body></html>')


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('tickers', nargs='+')
    parser.add_argument('date', type=date.fromisoformat, help='YYYY-MM-DD; a non-trading day reads the session before it')
    parser.add_argument('--out', type=Path, default=Path(tempfile.gettempdir()) / 'backtest_tba',
                        help='directory for the HTML page')
    args = parser.parse_args()
    tickers = list(dict.fromkeys(t.upper() for t in args.tickers))

    cfg = zone_map_config()
    readings, skipped = [], []
    for ticker in tickers:
        bars = fetch_bars(ticker, args.date)
        try:
            if bars.empty:
                raise ValueError(f'Yahoo returned no bars on or before {args.date}')
            readings.append(zone_reading(ticker, bars, cfg))
        except ValueError as exc:
            skipped.append({'ticker': ticker, 'reason': str(exc)})
    if not readings:
        print(json.dumps({'readings': [], 'skipped': skipped}, indent=2))
        raise SystemExit('No ticker could be plotted.')

    args.out.mkdir(parents=True, exist_ok=True)
    # Three names at most: a long list would pass the Windows 260-character path limit.
    names = [t['ticker'] for t in readings]
    stem = '_'.join(names[:3]) + (f'_and_{len(names) - 3}_more' if len(names) > 3 else '')
    page = args.out / f'backtest_tba_{stem}_{args.date}.html'
    page.write_text(render_page(readings, skipped, cfg, args.date), encoding='utf-8')
    print(json.dumps({
        'readings': [{**t, 'state_label': STATE_LABELS[t['state']]} for t in readings],
        'skipped': skipped,
        'page': str(page),
    }, indent=2))


if __name__ == '__main__':
    main()
