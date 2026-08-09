"""New-high / new-low badges.

The screener exposes no boolean "at a new high" flag — `new_high_52w` is
accepted but returns null — so the badge is computed by comparing the last price
against the period extremes the feed does populate.

Horizons are checked longest-first and the longest satisfied one wins: a stock
at a 52-week high is also at a 1-month high, and the 52-week fact is the
interesting one.
"""

from __future__ import annotations

from typing import Optional

# Longest first. Each entry is (label, high field, low field).
HORIZONS = (
    ("52W", "price_52_week_high", "price_52_week_low"),
    ("6M", "High.6M", "Low.6M"),
    ("3M", "High.3M", "Low.3M"),
    ("1M", "High.1M", "Low.1M"),
)


def extreme_badge(meta: dict) -> Optional[dict]:
    """Return the longest horizon whose extreme the last price has reached.

    Returns None when the price sits inside every horizon, or when the feed did
    not supply the fields (crypto, which carries only 24h rolling high/low).
    """
    close = _num(meta.get("close"))
    if close <= 0:
        return None

    for label, high_field, low_field in HORIZONS:
        high = _num(meta.get(high_field))
        if high > 0 and close >= high:
            return {"label": label, "direction": "high"}

    for label, high_field, low_field in HORIZONS:
        low = _num(meta.get(low_field))
        if low > 0 and close <= low:
            return {"label": label, "direction": "low"}

    return None


def _num(value) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0
