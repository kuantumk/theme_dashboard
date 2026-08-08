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

import pandas as pd


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


def apply_in_play(df: pd.DataFrame, cfg) -> pd.DataFrame:
    """Keep rows that are actually moving.

    The two legs are independent and either can be disabled by setting its
    config value to null. With both disabled the liquidity-filtered set passes
    through untouched.
    """
    if df.empty:
        return df
    rvol_floor = cfg.in_play_min_rvol
    change_floor = cfg.in_play_min_change_pct
    if rvol_floor is None and change_floor is None:
        return df

    keep = pd.Series(False, index=df.index)
    if rvol_floor is not None and "rvol" in df.columns:
        keep |= pd.to_numeric(df["rvol"], errors="coerce").fillna(0) >= rvol_floor
    if change_floor is not None and "change_pct" in df.columns:
        keep |= pd.to_numeric(df["change_pct"], errors="coerce").abs().fillna(0) >= change_floor
    return df[keep]


def build_universe(df: pd.DataFrame, cfg, *, in_play: bool = True) -> pd.DataFrame:
    out = apply_liquidity(df, cfg)
    if in_play:
        out = apply_in_play(out, cfg)
    return out
