"""Time-of-day normalisation for the screener's relative-volume figure.

TradingView's ``relative_volume_10d_calc`` is session-to-date volume divided by
the 10-day average **full-day** volume, with no time-of-day adjustment. That was
verified rather than assumed: across 59 liquid tickers the implied divisor
(``volume / rvol``) matched the true 10-day average daily volume to a median
error of 0.02%, while the 30-day average was off by 13.8%.

A fixed floor on that raw figure is therefore not one filter but a different
filter every hour. Clearing ``rvol >= 1.5`` demands roughly:

===========  ==============================
time (ET)    multiple of normal pace needed
===========  ==============================
09:35        16.3x
10:00         7.5x
10:30         4.8x
15:00         1.8x
===========  ==============================

So the leg is strictest exactly when a momentum trader most needs it, and it is
why a stock can run +14% on twice its normal participation all morning and never
be admitted. Measured on 2026-08-14, the raw leg admitted 20 rows out of 2,191.

Dividing the raw figure by the expected fraction below converts it into a
**pace**: 1.0 means the stock is trading at exactly its normal rate for this
hour, at any hour.

Relationship to ``ep_scan_common.calculate_rvol_at_time``
---------------------------------------------------------
That function answers the same question far more precisely, by comparing a
ticker's cumulative volume to its **own** history at the same clock time. It
costs one Alpaca request per ticker, which suits a scan of ~30 earnings
candidates and cannot work here: this dashboard re-reads ~2,000 screener rows
every few seconds and must stay at one request per poll. The curve below is the
affordable approximation — pure arithmetic on data the poll already returned.

The cost of that approximation is the assumption that every ticker shares the
market's intraday volume shape. The U-shape is robust across liquid names, but a
ticker with its own scheduled mid-morning catalyst will not match it. Treat the
result as a gate, not as a measurement.

The curve itself is the median cumulative share of regular-session volume,
measured across 36 liquid tickers over 5 sessions (180 ticker-days) at 5-minute
resolution. Re-derive it with ``tools`` equivalents of the yfinance 5m download;
resolution is 5 minutes through the first hour, where the curve is steepest and
the correction matters most, and 15 minutes thereafter.
"""

from __future__ import annotations

import math
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

import numpy as np

ET = ZoneInfo("America/New_York")

SESSION_OPEN_MIN = 9 * 60 + 30   # 09:30 ET
SESSION_CLOSE_MIN = 16 * 60      # 16:00 ET

# (minutes past midnight ET, median cumulative share of the session's volume)
CURVE = (
    (570, 0.0000),   # 09:30
    (575, 0.0908),   # 09:35
    (580, 0.1139),   # 09:40
    (585, 0.1341),   # 09:45
    (590, 0.1563),   # 09:50
    (595, 0.1727),   # 09:55
    (600, 0.1926),   # 10:00
    (605, 0.2149),   # 10:05
    (610, 0.2315),   # 10:10
    (615, 0.2507),   # 10:15
    (620, 0.2682),   # 10:20
    (625, 0.2829),   # 10:25
    (630, 0.2982),   # 10:30
    (645, 0.3351),   # 10:45
    (660, 0.3720),   # 11:00
    (675, 0.4041),   # 11:15
    (690, 0.4388),   # 11:30
    (705, 0.4715),   # 11:45
    (720, 0.5021),   # 12:00
    (735, 0.5290),   # 12:15
    (750, 0.5582),   # 12:30
    (765, 0.5827),   # 12:45
    (780, 0.6045),   # 13:00
    (795, 0.6269),   # 13:15
    (810, 0.6549),   # 13:30
    (825, 0.6783),   # 13:45
    (840, 0.7008),   # 14:00
    (855, 0.7273),   # 14:15
    (870, 0.7504),   # 14:30
    (885, 0.7838),   # 14:45
    (900, 0.8103),   # 15:00
    (915, 0.8386),   # 15:15
    (930, 0.8777),   # 15:30
    (945, 0.9161),   # 15:45
    (960, 1.0000),   # 16:00 — includes the closing cross
)

# The floor on the denominator. At 09:30:00 the true share is zero, so an
# unclamped divide makes every pace infinite and admits the whole universe on
# the opening print. Clamping to the 09:35 measurement understates pace for the
# first five minutes, which fails closed rather than open.
MIN_FRACTION = CURVE[1][1]

# Split once at import for `np.interp`, matching the anchor-point lookup in
# `themes/analyze_theme_strength.py`.
_CURVE_MINUTES = np.array([m for m, _ in CURVE], dtype=float)
_CURVE_FRACTIONS = np.array([f for _, f in CURVE], dtype=float)


def expected_fraction(now: Optional[datetime] = None) -> float:
    """Share of a normal session's volume traded by ``now``.

    Outside the regular session this returns a bound rather than an
    extrapolation: the floor before the open, a full session after the close.
    Both bounds stay explicit rather than leaning on `np.interp`'s edge
    behaviour, because they are deliberate choices about sessions this curve
    does not describe, not a side effect of interpolating.
    """
    moment = now or datetime.now(tz=ET)
    moment = moment.astimezone(ET) if moment.tzinfo else moment.replace(tzinfo=ET)
    minutes = moment.hour * 60 + moment.minute + moment.second / 60.0

    if minutes >= SESSION_CLOSE_MIN:
        return 1.0
    if minutes <= SESSION_OPEN_MIN:
        return MIN_FRACTION
    return float(max(MIN_FRACTION,
                     np.interp(minutes, _CURVE_MINUTES, _CURVE_FRACTIONS)))


def pace_divisor(fraction) -> float:
    """The clamped denominator every pace calculation divides by.

    Shared deliberately. The gate runs a vectorized pandas expression while
    callers and tests use the scalar `volume_pace`, and a clamp written twice
    is a clamp that drifts — the repo already learned this with
    `compute_inside_day`, which two modules now share for the same reason.
    """
    return max(float(fraction), MIN_FRACTION)


def volume_pace(rvol, fraction) -> float:
    """Convert a raw relative-volume reading into a time-of-day pace.

    1.0 is normal participation for this hour. A missing, non-finite or
    negative reading is 0.0, not 1.0: an unknown must not pass the gate as if
    it qualified. `apply_in_play` reproduces this rule with
    `to_numeric(...).fillna(0).clip(lower=0)` over a whole column;
    `tests/test_bidask_volume_curve.py` pins the two against each other.
    """
    try:
        raw = float(rvol)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(raw) or raw <= 0:
        return 0.0
    return raw / pace_divisor(fraction)
