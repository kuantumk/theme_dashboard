"""
Compute the Nasdaq McClellan Summation Index ($NASI equivalent) and its RSI(14).

There is no free feed for $NASI — the exchanges do not publish advance/decline
data at all, so every vendor computes its own from a private issue list. We
therefore compute ours, from the Nasdaq-listed universe in the ticker file the
workflow already refreshes daily.

Advance/decline counts are cached in `data/nasdaq_ad_history.json` so that the
long history is paid for once. Each run refreshes only a trailing window, which
also lets late-reported sessions correct themselves.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from config.settings import DATA_DIR, DOCS_DATA_DIR  # noqa: E402
from src.indicators.nasdaq_mcclellan import compute_breadth_frame  # noqa: E402

AD_HISTORY_PATH = DATA_DIR / "nasdaq_ad_history.json"
OUTPUT_PATH = DOCS_DATA_DIR / "nasi.json"

# Calibrated against StockCharts $NASI on 2026-07-30 / 2026-08-07. Including
# ETFs is what closes the gap (RSI 13.06 -> 9.97 vs their 8.85); the volume
# floor only trims never-traded shells. See nasdaq_mcclellan module docstring.
MIN_AVG_VOLUME = 50_000
AVG_VOLUME_WINDOW = 20
# Calendar days, not sessions — these bound a yfinance date range.
REFRESH_DAYS = 90
# Extra calendar days downloaded before the refresh window so the rolling volume
# average is fully warm at its first accepted session. Without this the same date
# gets different advance/decline counts on a refresh than it got on the backfill,
# because its liquidity gate was computed from a partial average.
VOLUME_WARMUP_DAYS = 45
# Trading sessions, not calendar days: ~18 months, matching the span of the
# reference $NASI chart. The point of this panel is that RSI(14) troughs recur
# only about once a year, so a window short enough to contain a single trough
# shows a squiggle rather than a pattern. Bounded by what the committed A/D
# cache holds (3.2 years), so widening it needs no backfill.
EXPORT_SESSIONS = 378
BATCH_SIZE = 150


def select_universe(ticker_file: Path = None) -> list[str]:
    """Nasdaq-listed, non-test securities — ETFs and funds deliberately included.

    This is intentionally *not* `stock_utils.get_tickers_from_nasdaq()`: that
    filter drops ETFs, warrants, units and rights because the screeners must not
    trade them. Breadth is the opposite problem — it wants every issue the
    exchange counts in its own advance/decline tally.
    """
    ticker_file = ticker_file or (DATA_DIR / "tickers_from_nasdaq.txt")
    frame = pd.read_csv(ticker_file, sep="|")
    frame = frame[frame["Symbol"].notna()]
    listed = frame[(frame["Listing Exchange"] == "Q") & (frame["Test Issue"] == "N")]
    return sorted(listed["Symbol"].astype(str).unique())


def _load_ad_history() -> pd.DataFrame:
    if not AD_HISTORY_PATH.exists():
        return pd.DataFrame(columns=["advances", "declines"], dtype="int64")
    raw = json.loads(AD_HISTORY_PATH.read_text())
    frame = pd.DataFrame.from_dict(raw, orient="index")
    frame.index = pd.to_datetime(frame.index)
    return frame.sort_index()


def _save_ad_history(frame: pd.DataFrame) -> None:
    payload = {
        d.strftime("%Y-%m-%d"): {"advances": int(r.advances), "declines": int(r.declines)}
        for d, r in frame.sort_index().iterrows()
    }
    AD_HISTORY_PATH.write_text(json.dumps(payload, separators=(",", ":")))


def count_advances_declines(tickers: list[str], start: str, end: str = None,
                            batch_size: int = BATCH_SIZE) -> pd.DataFrame:
    """Download the universe and count advancing vs declining issues per session.

    An issue counts only when it has both a prior and a current close and clears
    the volume floor, mirroring a market diary's "issues traded" denominator.
    Unchanged closes count as neither, per the McClellan definition.
    """
    closes: dict[str, pd.Series] = {}
    volumes: dict[str, pd.Series] = {}
    for i in range(0, len(tickers), batch_size):
        chunk = tickers[i:i + batch_size]
        try:
            data = yf.download(chunk, start=start, end=end, progress=False,
                               auto_adjust=False, group_by="ticker", threads=True)
        except Exception as exc:  # noqa: BLE001 - a failed batch must not abort the run
            print(f"  batch {i} failed: {exc}")
            continue
        for ticker in chunk:
            try:
                frame = data[ticker].dropna(subset=["Close"])
            except Exception:  # noqa: BLE001 - symbol absent from the response
                continue
            if frame.empty:
                continue
            closes[ticker] = frame["Close"]
            volumes[ticker] = frame["Volume"]

    if not closes:
        raise RuntimeError("no price data returned for the Nasdaq breadth universe")

    close_panel = pd.DataFrame(closes).sort_index()
    volume_panel = pd.DataFrame(volumes).sort_index().reindex_like(close_panel)

    liquid = volume_panel.rolling(AVG_VOLUME_WINDOW, min_periods=1).mean() >= MIN_AVG_VOLUME
    change = close_panel.where(liquid).diff()
    counts = pd.DataFrame({
        "advances": (change > 0).sum(axis=1),
        "declines": (change < 0).sum(axis=1),
    })
    # The first row has no prior close, and thin sessions (holidays, partial
    # feeds) carry no usable signal.
    return counts[counts.sum(axis=1) >= 100]


def build_payload(counts: pd.DataFrame, export_sessions: int = EXPORT_SESSIONS) -> dict:
    panel = compute_breadth_frame(counts["advances"], counts["declines"]).dropna(subset=["rsi"])
    if panel.empty:
        raise RuntimeError("breadth panel is empty after warmup")
    recent = panel.tail(export_sessions)
    latest = recent.iloc[-1]
    # No separate "change" field: the summation's day-over-day change *is* the
    # oscillator, so publishing both would be two names for one number.
    return {
        "updated": datetime.now().isoformat(timespec="seconds"),
        "current": {
            "date": recent.index[-1].strftime("%Y-%m-%d"),
            "summation": round(float(latest.summation), 2),
            "summation_ma": (None if pd.isna(latest.summation_ma)
                             else round(float(latest.summation_ma), 2)),
            "oscillator": round(float(latest.oscillator), 2),
            "rsi": round(float(latest.rsi), 2),
            "issues": int(latest.issues),
        },
        "history": [
            {
                "date": idx.strftime("%Y-%m-%d"),
                "summation": round(float(row.summation), 2),
                "summation_ma": (None if pd.isna(row.summation_ma)
                                 else round(float(row.summation_ma), 2)),
                "rsi": round(float(row.rsi), 2),
            }
            for idx, row in recent.iterrows()
        ],
    }


def summary_line(payload: dict, out_path) -> str:
    """One-line run summary.

    Split out of `main()` so it is reachable from tests. Inlined in `main()` it
    was the one code path no test touched, and a stale key reference there
    raised KeyError on every run *after* nasi.json had already been written —
    a clean run that looked like a failed one in the workflow log.
    """
    current = payload["current"]
    return (f"NASI {current['summation']:.2f} (osc {current['oscillator']:+.2f})  "
            f"RSI(14) {current['rsi']:.2f}  issues {current['issues']}  -> {out_path}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Compute Nasdaq McClellan Summation Index + RSI")
    parser.add_argument("--backfill", action="store_true",
                        help="rebuild advance/decline history from scratch (slow)")
    parser.add_argument("--start", default="2023-06-01", help="backfill start date")
    parser.add_argument("--out", type=Path, default=OUTPUT_PATH)
    args = parser.parse_args()

    tickers = select_universe()
    history = _load_ad_history()
    print(f"Nasdaq breadth universe: {len(tickers)} listed issues")

    if args.backfill or history.empty:
        accept_from = pd.Timestamp(args.start)
        print(f"backfilling advance/decline history from {args.start} (this takes a few minutes)")
    else:
        # Re-download a trailing window so late-reported sessions can correct
        # themselves. Only sessions at or after `accept_from` are kept — the
        # earlier padding exists solely to warm the rolling volume average.
        accept_from = history.index.max() - pd.Timedelta(days=REFRESH_DAYS)
        print(f"refreshing advance/decline history from {accept_from.date()}")

    download_start = (accept_from - pd.Timedelta(days=VOLUME_WARMUP_DAYS)).strftime("%Y-%m-%d")
    fresh = count_advances_declines(tickers, start=download_start)
    fresh = fresh[fresh.index >= accept_from]
    if fresh.empty:
        raise RuntimeError(f"no sessions counted at or after {accept_from.date()}")
    print(f"  counted {len(fresh)} sessions, latest {fresh.index[-1].date()}")

    # Fresh counts win for any overlapping session.
    history = fresh.combine_first(history).sort_index()
    history = history.astype("int64")
    _save_ad_history(history)

    payload = build_payload(history)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(payload, separators=(",", ":")))

    print(summary_line(payload, args.out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
