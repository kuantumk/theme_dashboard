"""Shared coiled-theme setup scoring.

The scan looks for former leaders that have gone quiet near key moving
averages before momentum/RS/VARS fully confirm again.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


MIN_AVG_DOLLAR_VOL = 10_000_000
MIN_VOL_SMA50 = 500_000
MIN_PRICE = 2.0
MIN_ADR_PCT = 0.035
MIN_COILED_THEME_SCORE = 90.0
MAX_DIST_SMA50_PCT = 0.25
MAX_VOL_DRY_10_50 = 1.00

COILED_OUTPUT_COLUMNS = [
    "range_pct",
    "range10_pct",
    "range20_pct",
    "range_contraction_10_20",
    "vol_dry_10_50",
    "dist_sma50_pct",
    "close_vs_252h",
    "nr7",
    "nr20",
    "coiled_theme_score",
    "coiled_flags",
    "coiled_is_candidate",
]


def _series(df: pd.DataFrame, name: str, default=np.nan) -> pd.Series:
    if name in df.columns:
        return pd.to_numeric(df[name], errors="coerce")
    return pd.Series(default, index=df.index, dtype="float64")


def _bool_series(df: pd.DataFrame, name: str) -> pd.Series:
    if name not in df.columns:
        return pd.Series(False, index=df.index, dtype="bool")
    return df[name].fillna(False).astype(bool)


def _ensure_setup_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    high = _series(out, "high")
    low = _series(out, "low")
    close = _series(out, "close")
    volume = _series(out, "volume")
    sma50 = _series(out, "sma50")
    max252 = _series(out, "max252")
    vol_sma50 = _series(out, "vol_sma50")

    if "range_pct" not in out.columns:
        out["range_pct"] = (high - low) / close
    if "range10_pct" not in out.columns:
        out["range10_pct"] = (high.rolling(10, min_periods=5).max() - low.rolling(10, min_periods=5).min()) / close
    if "range20_pct" not in out.columns:
        out["range20_pct"] = (high.rolling(20, min_periods=10).max() - low.rolling(20, min_periods=10).min()) / close
    if "range_contraction_10_20" not in out.columns:
        out["range_contraction_10_20"] = _series(out, "range10_pct") / _series(out, "range20_pct")
    if "vol_dry_10_50" not in out.columns:
        out["vol_dry_10_50"] = volume.rolling(10, min_periods=5).mean() / vol_sma50
    if "dist_sma50_pct" not in out.columns:
        out["dist_sma50_pct"] = (close / sma50) - 1
    if "close_vs_252h" not in out.columns:
        out["close_vs_252h"] = close / max252
    if "nr7" not in out.columns:
        range_pct = _series(out, "range_pct")
        out["nr7"] = range_pct <= range_pct.rolling(7, min_periods=7).min()
    if "nr20" not in out.columns:
        range_pct = _series(out, "range_pct")
        out["nr20"] = range_pct <= range_pct.rolling(20, min_periods=20).min()

    return out


def _score_from_conditions(df: pd.DataFrame) -> pd.Series:
    inside = _bool_series(df, "inside_day")
    tight = _bool_series(df, "tight_day")
    nr7 = _bool_series(df, "nr7")
    nr20 = _bool_series(df, "nr20")
    close_to_ma = _bool_series(df, "close_to_ma")

    vol_dry = _series(df, "vol_dry_10_50")
    dist50 = _series(df, "dist_sma50_pct")
    range_contract = _series(df, "range_contraction_10_20")
    high_low_252 = _series(df, "max252") / _series(df, "min252")
    perf_6mo = _series(df, "perf_6mo")
    perf_12mo = _series(df, "perf_12mo")
    avg_dollar_vol = _series(df, "avg_dollar_vol")
    adr_pct = _series(df, "adr_pct")
    rs = _series(df, "rs_sts_pct")
    vars_raw = _series(df, "vars")
    perf_1mo = _series(df, "perf_1mo")

    score = pd.Series(0.0, index=df.index)

    compression = pd.Series(0.0, index=df.index)
    compression += np.where(inside & tight, 30.0, 0.0)
    compression += np.where((inside ^ tight), 22.0, 0.0)
    compression += np.where(nr20, 8.0, np.where(nr7, 5.0, 0.0))
    compression += np.where(range_contract <= 0.75, 5.0, 0.0)
    score += compression.clip(upper=35.0)

    dry = pd.Series(0.0, index=df.index)
    dry += np.where(vol_dry <= 0.70, 20.0, 0.0)
    dry += np.where((vol_dry > 0.70) & (vol_dry <= 0.90), 16.0, 0.0)
    dry += np.where((vol_dry > 0.90) & (vol_dry <= MAX_VOL_DRY_10_50), 12.0, 0.0)
    score += dry.clip(upper=20.0)

    prior = pd.Series(0.0, index=df.index)
    prior += np.where(high_low_252 >= 5.0, 20.0, 0.0)
    prior += np.where((high_low_252 >= 2.0) & (high_low_252 < 5.0), 12.0, 0.0)
    prior += np.where(perf_12mo >= 1.0, 15.0, 0.0)
    prior += np.where(perf_6mo >= 1.0, 14.0, 0.0)
    prior += np.where((perf_6mo >= 0.50) & (perf_6mo < 1.0), 10.0, 0.0)
    score += prior.clip(upper=22.0)

    base = pd.Series(0.0, index=df.index)
    base += np.where(dist50.abs() <= 0.05, 15.0, 0.0)
    base += np.where((dist50.abs() > 0.05) & (dist50.abs() <= 0.12), 10.0, 0.0)
    base += np.where(close_to_ma, 5.0, 0.0)
    score += base.clip(upper=18.0)

    liquidity = pd.Series(0.0, index=df.index)
    liquidity += np.where(avg_dollar_vol >= MIN_AVG_DOLLAR_VOL, 5.0, 0.0)
    liquidity += np.where(adr_pct >= MIN_ADR_PCT, 5.0, 0.0)
    score += liquidity

    blind_spot = pd.Series(0.0, index=df.index)
    blind_spot += np.where(perf_1mo < 0.10, 3.0, 0.0)
    blind_spot += np.where(rs < 70.0, 3.0, 0.0)
    blind_spot += np.where(vars_raw < 2.0, 2.0, 0.0)
    blind_spot += np.where(vars_raw < 0.0, 2.0, 0.0)
    score += blind_spot.clip(upper=10.0)

    return score.clip(lower=0.0, upper=100.0)


def _flag_row(row: pd.Series) -> str:
    flags: list[str] = []

    if bool(row.get("inside_day", False)):
        flags.append("inside")
    if bool(row.get("tight_day", False)):
        flags.append("tight")
    if bool(row.get("nr20", False)):
        flags.append("NR20")
    elif bool(row.get("nr7", False)):
        flags.append("NR7")

    vol_dry = row.get("vol_dry_10_50")
    if pd.notna(vol_dry):
        flags.append(f"volDry={float(vol_dry):.2f}")

    dist50 = row.get("dist_sma50_pct")
    if pd.notna(dist50):
        flags.append(f"50SMA={float(dist50) * 100:+.1f}%")

    close_vs_high = row.get("close_vs_252h")
    if pd.notna(close_vs_high):
        flags.append(f"252H={float(close_vs_high):.2f}x")

    high_low_252 = row.get("max252")
    min252 = row.get("min252")
    if pd.notna(high_low_252) and pd.notna(min252) and float(min252) > 0:
        flags.append(f"prior={float(high_low_252) / float(min252):.1f}x")

    blind = []
    if pd.notna(row.get("perf_1mo")) and float(row.get("perf_1mo")) < 0.10:
        blind.append("1m")
    if pd.notna(row.get("rs_sts_pct")) and float(row.get("rs_sts_pct")) < 70:
        blind.append("RS")
    if pd.notna(row.get("vars")) and float(row.get("vars")) < 2:
        blind.append("VARS")
    if blind:
        flags.append("blind=" + "/".join(blind))

    return "; ".join(flags[:8])


def add_coiled_theme_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Return ``df`` with reusable coiled-theme columns and candidate score."""
    out = _ensure_setup_columns(df)

    close = _series(out, "close")
    vol_sma50 = _series(out, "vol_sma50")
    avg_dollar_vol = _series(out, "avg_dollar_vol")
    adr_pct = _series(out, "adr_pct")
    dist50 = _series(out, "dist_sma50_pct")
    vol_dry = _series(out, "vol_dry_10_50")
    high_low_252 = _series(out, "max252") / _series(out, "min252")
    perf_6mo = _series(out, "perf_6mo")
    perf_12mo = _series(out, "perf_12mo")

    inside = _bool_series(out, "inside_day")
    tight = _bool_series(out, "tight_day")
    nr20 = _bool_series(out, "nr20")
    close_to_ma = _bool_series(out, "close_to_ma")

    liquidity = (
        (close >= MIN_PRICE)
        & (vol_sma50 >= MIN_VOL_SMA50)
        & (avg_dollar_vol >= MIN_AVG_DOLLAR_VOL)
        & (adr_pct >= MIN_ADR_PCT)
    )
    prior_leadership = (
        (high_low_252 >= 2.0)
        | (perf_6mo >= 0.50)
        | (perf_12mo >= 1.0)
    )
    constructive_base = (
        (dist50 >= -0.15)
        & (dist50 <= 0.20)
        & (close_to_ma | (dist50.abs() <= 0.12))
    )
    compression = (inside & tight) | (tight & nr20)
    dry_enough = vol_dry <= MAX_VOL_DRY_10_50
    not_extended = dist50 <= MAX_DIST_SMA50_PCT

    out["coiled_theme_score"] = _score_from_conditions(out)
    out["coiled_is_candidate"] = (
        liquidity
        & prior_leadership
        & constructive_base
        & compression
        & dry_enough
        & not_extended
        & (out["coiled_theme_score"] >= MIN_COILED_THEME_SCORE)
    ).fillna(False)
    out["coiled_flags"] = out.apply(_flag_row, axis=1)

    return out


def copy_coiled_columns(source: pd.DataFrame, target: pd.DataFrame) -> None:
    """Copy coiled columns onto ``target`` in-place when a caller needs mutation."""
    for col in COILED_OUTPUT_COLUMNS:
        if col in source.columns:
            target[col] = source[col]


def coiled_filter(df: pd.DataFrame) -> pd.Series:
    enriched = add_coiled_theme_metrics(df)
    return enriched["coiled_is_candidate"].fillna(False)
