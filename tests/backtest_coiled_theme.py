"""Backtest the coiled-theme screener on historical Yahoo daily data.

This is a research harness, not a unit test. It intentionally writes its
report under artifacts/ so production dashboard data stays untouched.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.parse
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np
import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.screening.coiled_theme import add_coiled_theme_metrics

ARTIFACT_DIR = PROJECT_ROOT / "artifacts" / "coiled_theme_backtest"
CACHE_DIR = ARTIFACT_DIR / "ohlcv"
REPORT_PATH = ARTIFACT_DIR / "coiled_theme_backtest.md"
warnings.filterwarnings("ignore", message="Unverified HTTPS request")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--download-start", default="2024-01-01")
    parser.add_argument("--start", default="2025-01-01")
    parser.add_argument("--end", default="2025-12-31")
    parser.add_argument("--max-workers", type=int, default=12)
    parser.add_argument("--limit", type=int, default=0, help="Optional ticker limit for smoke runs.")
    parser.add_argument("--refresh", action="store_true")
    return parser.parse_args()


def yahoo_symbol(ticker: str) -> str:
    return ticker.replace(".", "-").strip().upper()


def cache_path(ticker: str) -> Path:
    safe = ticker.replace("/", "_").replace("\\", "_").replace(".", "-").upper()
    return CACHE_DIR / f"{safe}.csv"


def load_tickers(limit: int = 0) -> list[str]:
    mapping = json.loads((PROJECT_ROOT / "data" / "ticker_themes.json").read_text(encoding="utf-8"))
    tickers = sorted(t for t in mapping if t.isascii() and not any(ch in t for ch in ("^", " ", "/")))
    priority = ["RGTI", "QBTS", "IONQ", "QUBT", "ARQQ", "LAES"]
    ordered = priority + [t for t in tickers if t not in priority]
    if limit:
        ordered = ordered[:limit]
    return ordered


def fetch_chart(ticker: str, start: str, end: str, refresh: bool = False) -> pd.DataFrame | None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = cache_path(ticker)
    if path.exists() and not refresh:
        try:
            df = pd.read_csv(path, parse_dates=["date"]).set_index("date")
            if not df.empty:
                return df
        except Exception:
            pass

    p1 = int(pd.Timestamp(start, tz="UTC").timestamp())
    p2 = int((pd.Timestamp(end, tz="UTC") + pd.Timedelta(days=1)).timestamp())
    sym = urllib.parse.quote(yahoo_symbol(ticker), safe="")
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?period1={p1}&period2={p2}&interval=1d&events=history"
    try:
        response = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"}, verify=False)
        response.raise_for_status()
        result = (response.json().get("chart") or {}).get("result")
        if not result:
            return None
        item = result[0]
        quote = item["indicators"]["quote"][0]
        index = (
            pd.to_datetime(item["timestamp"], unit="s")
            .tz_localize("UTC")
            .tz_convert("America/New_York")
            .tz_localize(None)
            .normalize()
        )
        df = pd.DataFrame(
            {k: quote[k] for k in ["open", "high", "low", "close", "volume"]},
            index=index,
        ).dropna()
        if df.empty:
            return None
        df.index.name = "date"
        df.to_csv(path)
        return df
    except Exception:
        return None


def add_indicators(df: pd.DataFrame, spy: pd.DataFrame | None = None) -> pd.DataFrame:
    out = df.copy()
    high_low = out["high"] - out["low"]
    high_prev = (out["high"] - out["close"].shift(1)).abs()
    low_prev = (out["low"] - out["close"].shift(1)).abs()
    tr = pd.concat([high_low, high_prev, low_prev], axis=1).max(axis=1)
    out["atr14"] = tr.rolling(14, min_periods=1).mean()
    out["ema10"] = out["close"].ewm(span=10, adjust=False).mean()
    out["ema20"] = out["close"].ewm(span=20, adjust=False).mean()
    for window in [25, 30, 50, 100, 200]:
        out[f"sma{window}"] = out["close"].rolling(window, min_periods=max(window // 2, 1)).mean()
    for window in [30, 60, 90, 120, 150, 252]:
        out[f"min{window}"] = out["low"].rolling(window, min_periods=max(window // 2, 1)).min()
        out[f"max{window}"] = out["high"].rolling(window, min_periods=max(window // 2, 1)).max()
    out["vol_sma40"] = out["volume"].rolling(40, min_periods=20).mean()
    out["vol_sma50"] = out["volume"].rolling(50, min_periods=25).mean()
    out["vol_sma252"] = out["volume"].rolling(252, min_periods=126).mean()
    out["avg_dollar_vol"] = (out["volume"] * out["close"]).rolling(20, min_periods=10).mean()
    out["adr_pct"] = (out["high"] / out["low"]).rolling(20, min_periods=10).mean() - 1
    out["inside_day"] = (out["high"] < out["high"].shift(1)) & (out["low"] > out["low"].shift(1))
    out["tight_day"] = (out["close"] - out["open"]).abs() / out["close"] < 0.2 * out["adr_pct"]
    out["close_to_ma"] = (
        ((out["close"] - out["ema10"]).abs() < 0.5 * out["atr14"])
        | ((out["close"] - out["ema20"]).abs() < 0.5 * out["atr14"])
        | ((out["close"] - out["sma50"]).abs() < 0.5 * out["atr14"])
    )
    out["range_pct"] = (out["high"] - out["low"]) / out["close"]
    out["range10_pct"] = (out["high"].rolling(10, min_periods=5).max() - out["low"].rolling(10, min_periods=5).min()) / out["close"]
    out["range20_pct"] = (out["high"].rolling(20, min_periods=10).max() - out["low"].rolling(20, min_periods=10).min()) / out["close"]
    out["range_contraction_10_20"] = out["range10_pct"] / out["range20_pct"]
    out["vol_dry_10_50"] = out["volume"].rolling(10, min_periods=5).mean() / out["vol_sma50"]
    out["dist_sma50_pct"] = out["close"] / out["sma50"] - 1
    out["close_vs_252h"] = out["close"] / out["max252"]
    out["nr7"] = out["range_pct"] <= out["range_pct"].rolling(7, min_periods=7).min()
    out["nr20"] = out["range_pct"] <= out["range_pct"].rolling(20, min_periods=20).min()
    for name, periods in [("1mo", 21), ("3mo", 63), ("6mo", 126), ("12mo", 252)]:
        out[f"perf_{name}"] = out["close"] / out["close"].shift(periods) - 1

    if spy is not None:
        spy_norm = ((spy["close"] - spy["close"].shift(1)) / spy["atr14"]).rolling(100, min_periods=1).sum()
        ticker_norm = ((out["close"] - out["close"].shift(1)) / out["atr14"]).rolling(100, min_periods=1).sum()
        out["vars"] = ticker_norm - spy_norm.reindex(out.index)
        out["vars_20ema"] = out["vars"].ewm(span=20, adjust=False, min_periods=1).mean()
        rel = out["close"] / spy["close"].reindex(out.index)
        rs_vals = []
        for i in range(len(rel)):
            window = rel.iloc[max(0, i - 26): i + 1].dropna()
            if len(window) <= 1:
                rs_vals.append(np.nan)
            else:
                rs_vals.append((window < window.iloc[-1]).sum() / (len(window) - 1) * 100)
        out["rs_sts_pct"] = rs_vals
    return out


def forward_return(df: pd.DataFrame, date: pd.Timestamp, days: int) -> float | None:
    if date not in df.index:
        return None
    loc = df.index.get_loc(date)
    if not isinstance(loc, int) or loc + days >= len(df):
        return None
    start = df["close"].iloc[loc]
    end = df["close"].iloc[loc + days]
    if not start or pd.isna(start) or pd.isna(end):
        return None
    return float(end / start - 1)


def existing_screener_flags(row: pd.Series) -> dict[str, bool]:
    return {
        "momentum_136": bool(
            row["vol_sma50"] >= 750_000
            and row["avg_dollar_vol"] >= 15_000_000
            and row["adr_pct"] >= 0.04
            and (row["perf_1mo"] >= 0.25 or row["perf_3mo"] >= 0.50 or row["perf_6mo"] >= 1.00)
        ),
        "vars": bool(
            row["avg_dollar_vol"] >= 40_000_000
            and row["vol_sma50"] >= 1_000_000
            and row["close"] > 2
            and row["adr_pct"] >= 0.033
            and row["vars"] > 2.0
            and row["vars_20ema"] > 1.0
        ),
    }


def main():
    args = parse_args()
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)

    tickers = load_tickers(args.limit)
    all_symbols = ["SPY"] + [t for t in tickers if t != "SPY"]
    print(f"Downloading/loading {len(all_symbols)} symbols...")
    raw = {}
    with ThreadPoolExecutor(max_workers=args.max_workers) as pool:
        futures = {pool.submit(fetch_chart, t, args.download_start, args.end, args.refresh): t for t in all_symbols}
        for i, fut in enumerate(as_completed(futures), 1):
            ticker = futures[fut]
            df = fut.result()
            if df is not None and len(df) >= 80:
                raw[ticker] = df
            if i % 100 == 0:
                print(f"  {i}/{len(all_symbols)} fetched; usable={len(raw)}")

    if "SPY" not in raw:
        raise RuntimeError("SPY data is required for VARS/RS_STS")

    spy = add_indicators(raw["SPY"])
    data = {t: add_indicators(df, spy=spy) for t, df in raw.items() if t != "SPY"}

    themes = json.loads((PROJECT_ROOT / "data" / "ticker_themes.json").read_text(encoding="utf-8"))
    dates = pd.bdate_range(args.start, args.end)
    rows = []
    baseline_rows = []
    daily_counts = []
    rgti_case = None
    quantum_snapshots = {}

    for date in dates:
        master_rows = []
        for ticker, df in data.items():
            if date in df.index:
                row = df.loc[date].copy()
                row["ticker"] = ticker
                row["date"] = date.strftime("%Y-%m-%d")
                master_rows.append(row)
        if not master_rows:
            continue

        master = pd.DataFrame(master_rows)
        master = add_coiled_theme_metrics(master)
        candidates = master[master["coiled_is_candidate"]].copy()
        daily_counts.append((date, len(candidates)))

        baseline = master[
            (master["close"] >= 2)
            & (master["avg_dollar_vol"] >= 10_000_000)
            & (master["vol_sma50"] >= 500_000)
            & (master["adr_pct"] >= 0.035)
        ].copy()

        for _, row in candidates.iterrows():
            ticker = row["ticker"]
            out = row[["date", "ticker", "coiled_theme_score", "coiled_flags", "rs_sts_pct", "vars", "perf_1mo"]].to_dict()
            out["themes"] = themes.get(ticker, [])
            out["momentum_136"] = existing_screener_flags(row)["momentum_136"]
            out["vars_pass"] = existing_screener_flags(row)["vars"]
            for days in [5, 10, 20]:
                out[f"fwd_{days}d"] = forward_return(data[ticker], date, days)
            rows.append(out)

        for _, row in baseline.iterrows():
            ticker = row["ticker"]
            item = {"date": date.strftime("%Y-%m-%d"), "ticker": ticker}
            for days in [5, 10, 20]:
                item[f"fwd_{days}d"] = forward_return(data[ticker], date, days)
            baseline_rows.append(item)

        if date.strftime("%Y-%m-%d") in {"2025-09-08", "2025-09-09"}:
            q = candidates[candidates["ticker"].isin(["RGTI", "QBTS", "IONQ", "QUBT", "ARQQ", "LAES"])].copy()
            quantum_snapshots[date.strftime("%Y-%m-%d")] = q.sort_values("coiled_theme_score", ascending=False)
            rgti = q[q["ticker"] == "RGTI"]
            if not rgti.empty and rgti_case is None:
                rgti_case = rgti.iloc[0].to_dict()

    results = pd.DataFrame(rows)
    baseline = pd.DataFrame(baseline_rows)
    counts = pd.Series([c for _, c in daily_counts], index=[d for d, _ in daily_counts])
    quantum_visible = any(not frame.empty for frame in quantum_snapshots.values())

    lines = [
        "# Coiled Theme Backtest",
        "",
        f"Universe: {len(data)} usable tagged tickers from Yahoo daily data.",
        f"Window: {args.start} to {args.end}; indicators built from {args.download_start}.",
        "",
        "## Acceptance Checks",
        f"- RGTI on 2025-09-08: {'PASS' if rgti_case and str(rgti_case.get('date')) == '2025-09-08' else 'FAIL'}",
        f"- Quantum Computing visible by 2025-09-08/09: {'PASS' if quantum_visible else 'FAIL'}",
        f"- Median daily alert count: {counts.median():.1f} names ({'PASS' if 5 <= counts.median() <= 15 else 'CHECK'})",
    ]

    for days in [5, 10, 20]:
        c_mean = results[f"fwd_{days}d"].dropna().mean()
        b_mean = baseline[f"fwd_{days}d"].dropna().mean()
        c_med = results[f"fwd_{days}d"].dropna().median()
        b_med = baseline[f"fwd_{days}d"].dropna().median()
        lines.append(
            f"- T+{days}: coiled mean {c_mean:.2%} / median {c_med:.2%}; "
            f"baseline mean {b_mean:.2%} / median {b_med:.2%} "
            f"({'PASS' if c_mean > b_mean else 'CHECK'})"
        )

    lines.extend(["", "## RGTI Case"])
    if rgti_case:
        flags = existing_screener_flags(pd.Series(rgti_case))
        lines.append(
            f"- {rgti_case['date']} RGTI score {rgti_case['coiled_theme_score']:.1f}; "
            f"RS {rgti_case['rs_sts_pct']:.1f}; VARS {rgti_case['vars']:.2f}; "
            f"1M {rgti_case['perf_1mo']:.1%}; flags: {rgti_case['coiled_flags']}"
        )
        lines.append(f"- Existing blind spot: momentum_136={flags['momentum_136']}, vars={flags['vars']}")
    else:
        lines.append("- RGTI was not found in the target setup window.")

    lines.extend(["", "## Quantum Snapshots"])
    for date, frame in quantum_snapshots.items():
        lines.append(f"### {date}")
        if frame.empty:
            lines.append("- No quantum candidates.")
            continue
        for _, row in frame.iterrows():
            lines.append(f"- {row['ticker']}: score {row['coiled_theme_score']:.1f}; {row['coiled_flags']}")

    lines.extend(["", "## Alert Count Distribution"])
    lines.append(counts.describe(percentiles=[0.25, 0.5, 0.75, 0.9]).to_string())

    lines.extend(["", "## Top 20 Forward T+20 Results"])
    if not results.empty:
        top = results.dropna(subset=["fwd_20d"]).sort_values("fwd_20d", ascending=False).head(20)
        for _, row in top.iterrows():
            lines.append(f"- {row['date']} {row['ticker']}: {row['fwd_20d']:.1%}; score {row['coiled_theme_score']:.1f}; {row['coiled_flags']}")

    REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    results.to_csv(ARTIFACT_DIR / "coiled_theme_candidates.csv", index=False)
    baseline.to_csv(ARTIFACT_DIR / "coiled_theme_baseline.csv", index=False)
    print(REPORT_PATH)


if __name__ == "__main__":
    main()
