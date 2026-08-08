"""
Nasdaq McClellan Oscillator / Summation Index (ratio-adjusted) and its RSI.

Pure functions over daily advance/decline counts — no I/O, no network. The
collector in `src/data_collection/compute_nasi.py` supplies the counts.

Formulas (StockCharts $NAMO / $NASI convention):
    RANA        = 1000 * (advances - declines) / (advances + declines)
    oscillator  = EMA19(RANA) - EMA39(RANA)          # recursive, adjust=False
    summation   = cumulative sum of oscillator
    rsi         = Wilder RSI(14) of the summation series

**Why RSI is the signal and the summation level is not.** The summation index is
a running total with no natural origin, so its absolute level depends entirely on
where the accumulation started. RSI reads day-over-day *changes*, and
diff(summation) == oscillator, so the RSI is invariant to that seed — it is
reproducible without agreeing on an epoch with anyone. `test_nasdaq_mcclellan.py`
pins this.

**Issue universe matters more than anything else here.** Nasdaq's own market
diary counts every listed security, ETFs and closed-end funds included. Excluding
them under-counts decliners in a selloff and biases the whole series upward: on
2026-07-30 an operating-companies-only universe read RSI 13.06 where StockCharts
read 8.85, while an all-issues universe read 9.97. Since the classic oversold
line is 10, that exclusion is the difference between seeing a signal and missing
it. `select_universe` in the collector is therefore deliberately permissive.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

RANA_SCALE = 1000.0
FAST_SPAN = 19
SLOW_SPAN = 39
RSI_PERIOD = 14


def ratio_adjusted_net_advances(advances: pd.Series, declines: pd.Series) -> pd.Series:
    """RANA = 1000 * net advances / total issues traded.

    Sessions where nothing traded (advances + declines == 0) yield NaN rather
    than 0 — a zero would read as a perfectly neutral day and drag the EMAs.
    """
    advances = advances.astype("float64")
    declines = declines.astype("float64")
    total = (advances + declines).replace(0, np.nan)
    return RANA_SCALE * (advances - declines) / total


def mcclellan_oscillator(rana: pd.Series) -> pd.Series:
    """19-day EMA of RANA less its 39-day EMA (recursive form)."""
    rana = rana.dropna()
    fast = rana.ewm(span=FAST_SPAN, adjust=False).mean()
    slow = rana.ewm(span=SLOW_SPAN, adjust=False).mean()
    return fast - slow


def summation_index(oscillator: pd.Series) -> pd.Series:
    """Running total of the oscillator.

    The starting value is arbitrary; only the shape carries meaning. Consumers
    that need a level comparable to a vendor's chart must anchor it themselves.
    """
    return oscillator.cumsum()


def wilder_rsi(series: pd.Series, period: int = RSI_PERIOD) -> pd.Series:
    """RSI using Wilder's smoothing — the convention StockCharts charts use."""
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss
    # The endpoints need no special-casing: an unbroken run of gains divides by
    # zero to +inf and lands on 100, an unbroken run of losses gives rs == 0 and
    # lands on 0. Only the all-flat case is 0/0 -> NaN, which is the honest
    # answer for an undefined RSI.
    #
    # Do NOT "pin" those ends with .where(avg_loss > 0, 100)-style guards. NaN
    # fails every comparison, so such a guard also rewrites the min_periods
    # warmup rows — they became 0.0, survived dropna(), satisfied `rsi <= 10`,
    # and rendered as fabricated oversold markers on the dashboard.
    return (100 - 100 / (1 + rs)).where(series.notna())


def compute_breadth_frame(advances: pd.Series, declines: pd.Series,
                          ma_window: int = 10) -> pd.DataFrame:
    """Full NASI panel from raw advance/decline counts.

    Returns columns: issues, rana, oscillator, summation, summation_ma, rsi.
    """
    rana = ratio_adjusted_net_advances(advances, declines)
    oscillator = mcclellan_oscillator(rana)
    summation = summation_index(oscillator)
    return pd.DataFrame({
        "issues": (advances + declines).reindex(summation.index),
        "rana": rana.reindex(summation.index),
        "oscillator": oscillator,
        "summation": summation,
        "summation_ma": summation.rolling(ma_window).mean(),
        "rsi": wilder_rsi(summation),
    })
