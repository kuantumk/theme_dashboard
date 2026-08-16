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

from src.bidask.rvol_at_time import minutes_since_open, rvol_at_time, threshold_for


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
    df: pd.DataFrame,
    cfg,
    *,
    profiles: Optional[dict] = None,
    elapsed_minutes: Optional[float] = None,
) -> pd.DataFrame:
    """Keep rows that are actually moving.

    The two legs are independent. An empty `in_play_rvol_schedule` disables the
    volume leg and a null `in_play_min_change_pct` disables the change leg; with
    both off the liquidity-filtered set passes through untouched.

    The volume leg is Relative Volume at Time: this ticker's volume since the
    open over the mean of **its own** volume by the same time of day across
    recent sessions. The screener's `relative_volume_10d_calc` divides by a
    full-day average instead, so flooring it directly demands 16x normal
    participation at 09:35 and 1.8x at 15:00. See `src/bidask/rvol_at_time.py`.

    `profiles` and `elapsed_minutes` are passed in rather than read from the
    clock and the cache here, so the gate stays a pure function of its inputs.

    A ticker with no baseline yet — a fresh listing, a download miss, or the
    warm-up still running — scores 0 and is admitted only by the change leg.
    That is deliberate: an unknown must not clear a floor as if it qualified.
    """
    if df.empty:
        return df
    schedule = cfg.in_play_rvol_schedule
    change_floor = cfg.in_play_min_change_pct
    if not schedule and change_floor is None:
        return df

    keep = pd.Series(False, index=df.index)
    if schedule and "volume" in df.columns and "symbol" in df.columns:
        elapsed = (minutes_since_open() if elapsed_minutes is None else elapsed_minutes)
        floor = threshold_for(schedule, elapsed)
        if floor is not None:
            table = profiles or {}
            volumes = pd.to_numeric(df["volume"], errors="coerce")
            ratios = [
                rvol_at_time(volume, table.get(str(symbol)), elapsed)
                for symbol, volume in zip(df["symbol"], volumes)
            ]
            keep |= pd.Series(ratios, index=df.index) >= floor
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
    profiles: Optional[dict] = None,
    elapsed_minutes: Optional[float] = None,
) -> pd.DataFrame:
    out = apply_liquidity(df, cfg)
    if market == "crypto":
        out = exclude_symbols(out, cfg.crypto_exclude)
    if in_play:
        out = apply_in_play(out, cfg, profiles=profiles, elapsed_minutes=elapsed_minutes)
    return out
