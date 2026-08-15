"""Universe filtering for the bid/ask dashboard.

Two distinct cuts, deliberately separated:

* **Liquidity floors** decide what is worth *polling*. Most are pushed
  server-side by `feed`; average dollar volume lands here because the screener
  library rejects column arithmetic.
* **The in-play gate** decides what is worth *displaying*. One request covers
  the whole universe in under half a second, so this gate exists for the
  reader's attention, not for throughput.
"""

from __future__ import annotations

from typing import Optional

import pandas as pd

from src.bidask.volume_curve import expected_fraction, pace_divisor


def apply_liquidity(df: pd.DataFrame, cfg) -> pd.DataFrame:
    """Apply the floors the feed could not express server-side.

    Average dollar volume is average share volume times price. The screener has
    no such column and the library cannot compose one, so it is derived here.
    """
    if df.empty:
        return df
    out = df.copy()
    if "avg_volume" in out.columns and out["avg_volume"].notna().any():
        out["avg_dollar_vol"] = out["avg_volume"].astype(float) * out["close"].astype(float)
        out = out[out["avg_dollar_vol"] >= cfg.min_avg_dollar_vol]
    else:
        # Crypto carries no average-volume field; the floor cannot be applied
        # and the column is surfaced as absent rather than silently zeroed.
        out["avg_dollar_vol"] = None
    return out


def apply_in_play(
    df: pd.DataFrame, cfg, *, session_fraction: Optional[float] = None
) -> pd.DataFrame:
    """Keep rows that are actually moving.

    The two legs are independent and either can be disabled by setting its
    config value to null. With both disabled the liquidity-filtered set passes
    through untouched.

    The volume leg floors a **pace**, not the screener's raw relative-volume
    figure. That figure divides session-to-date volume by a full-day average, so
    flooring it directly demands 16x normal participation at 09:35 and 1.8x at
    15:00 — strictest exactly when a momentum trader needs it loosest. Dividing
    by the expected share of the session's volume makes the floor mean one thing
    all day. See `src/bidask/volume_curve.py`.

    `session_fraction` is passed in rather than read from the clock here so the
    gate stays a pure function of its inputs; omitting it falls back to now.
    """
    if df.empty:
        return df
    pace_floor = cfg.in_play_min_volume_pace
    change_floor = cfg.in_play_min_change_pct
    if pace_floor is None and change_floor is None:
        return df

    keep = pd.Series(False, index=df.index)
    if pace_floor is not None and "rvol" in df.columns:
        fraction = expected_fraction() if session_fraction is None else session_fraction
        # Same rule as the scalar `volume_pace`, applied to a whole column: an
        # unknown or negative reading is 0, never a free pass. The clamped
        # divisor is shared rather than rewritten so the two cannot drift.
        raw = pd.to_numeric(df["rvol"], errors="coerce").fillna(0.0).clip(lower=0.0)
        keep |= (raw / pace_divisor(fraction)) >= pace_floor
    if change_floor is not None and "change_pct" in df.columns:
        keep |= pd.to_numeric(df["change_pct"], errors="coerce").abs().fillna(0) >= change_floor
    return df[keep]


def exclude_symbols(df: pd.DataFrame, excluded) -> pd.DataFrame:
    """Drop symbols whose tape pressure carries no information.

    Stablecoins are the motivating case: pegged at $1, so their observations are
    micro-oscillation around the peg being classified as directional flow. They
    also trade constantly, so they accumulate hits faster than anything real and
    float to the top of the column.
    """
    if df.empty or not excluded or "symbol" not in df.columns:
        return df
    return df[~df["symbol"].astype(str).str.upper().isin(excluded)]


def build_universe(
    df: pd.DataFrame,
    cfg,
    *,
    in_play: bool = True,
    market: str = "equity",
    session_fraction: Optional[float] = None,
) -> pd.DataFrame:
    out = apply_liquidity(df, cfg)
    if market == "crypto":
        out = exclude_symbols(out, cfg.crypto_exclude)
    if in_play:
        out = apply_in_play(out, cfg, session_fraction=session_fraction)
    return out
