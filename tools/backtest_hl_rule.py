"""Back-test the Highs / Lows hit rule against the 2026-08-07 source screenshot.

This is the U3 gate from ``docs/plans/2026-08-07-001-feat-highs-lows-tab-plan.md``.

The adopted rule (R7) is a *gated variant* of a candidate that was falsified in
its ungated form: "minutes with last price at the running high of day" scored a
negative rank correlation at every tolerance. The adopted form adds a move
threshold in front of it and samples at poll cadence rather than per minute.
That combination is what explains TWLO (closed +24.9% but faded off its high,
so it stopped accruing at 76 hits) where the ungated version could not -- but
until this harness runs, that explanation is a story, not a measurement.

The rule, replayed per poll:

    qualifies_high(t) = (last(t) / prev_close - 1) >= move_threshold
                        AND last(t) >= running_high(t) * (1 - tolerance)

    hits = count of polls where qualifies_high holds

Ground truth is the per-ticker hit count read off the screenshot, captured
2026-08-07 between 13:17 and 15:29 ET. Since capture time is only bracketed,
the harness sweeps cutoffs; Spearman is rank-based and largely cutoff-
insensitive, which is why rank correlation is the bar rather than absolute error.

    uv run python tools/backtest_hl_rule.py

Exit 0 when the adopted rule clears the PASS_BAR, 1 otherwise.
"""

from __future__ import annotations

import sys
import warnings
from datetime import datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import yfinance as yf

warnings.filterwarnings("ignore")

ET = ZoneInfo("America/New_York")
SESSION = "2026-08-07"
PREV_SESSION = "2026-08-06"

# Spearman bar the adopted rule must clear. Set above the 0.39 that got
# "minutes above an N-day high" rejected, and below the 0.84 that raw % change
# reaches without capturing persistence at all -- a rule that merely tracks
# magnitude would be indistinguishable from the % change baseline and would not
# justify the machinery.
PASS_BAR = 0.60

MOVE_THRESHOLD = 0.03
DEFAULT_TOLERANCE = 0.0015
DEFAULT_POLL_SECONDS = 90

# Per-ticker hit counts read off the source screenshot's highs column.
OBSERVED: dict[str, int] = {
    "FIVN": 164, "WOLF": 142, "TNDM": 141, "CRSR": 136, "SPCX": 122,
    "HALO": 109, "HNGE": 108, "TSEM": 100, "LUNR": 95, "OKLO": 93,
    "RDW": 92, "ROAD": 90, "NVTS": 85, "IONQ": 82, "FLY": 76,
    "TWLO": 76, "BTG": 73, "PLTR": 66, "AXON": 65, "VSH": 62,
    "UMAC": 60, "QBTS": 56, "MXL": 56, "UBER": 54, "RKLB": 54,
    "PATH": 53, "GTLB": 50, "CBRS": 49, "HPQ": 43, "MDB": 43,
    "MCHP": 42, "RGTI": 42, "IOT": 40, "KRMN": 39, "ZETA": 38,
    "OC": 37, "RCAT": 37, "PL": 36, "HONA": 36, "SWKS": 35,
    "KTOS": 35, "SMCI": 32, "AVAV": 30, "GFS": 28, "MTSI": 28,
    "QRVO": 27, "CRDO": 26, "RMBS": 26, "QCOM": 26, "ACHR": 23,
    "MPWR": 22, "SAP": 21,
}

CUTOFFS = ["13:30", "14:00", "14:30", "15:00", "15:59"]


def load_bars() -> tuple[dict[str, pd.DataFrame], dict[str, float]]:
    """Fetch 1-minute RTH bars for the session, plus each ticker's prior close."""
    tickers = sorted(OBSERVED)
    print(f"  fetching 1-minute bars for {len(tickers)} tickers ({SESSION})...")
    minute = yf.download(
        tickers, period="5d", interval="1m", prepost=False,
        progress=False, auto_adjust=False, group_by="ticker", threads=True,
    )
    daily = yf.download(
        tickers, start="2026-07-25", end="2026-08-08",
        progress=False, auto_adjust=False, group_by="ticker", threads=True,
    )

    bars: dict[str, pd.DataFrame] = {}
    prev_close: dict[str, float] = {}
    for t in tickers:
        try:
            m = minute[t].dropna()
            d = daily[t].dropna(how="all")
        except (KeyError, TypeError):
            continue
        if m.empty or d.empty:
            continue
        m = m[m.index.tz_convert(ET).strftime("%Y-%m-%d") == SESSION]
        if len(m) < 60:
            continue
        prior = d[d.index.strftime("%Y-%m-%d") <= PREV_SESSION]
        if prior.empty:
            continue
        bars[t] = m
        prev_close[t] = float(prior["Close"].iloc[-1])
    return bars, prev_close


def simulate(
    frame: pd.DataFrame, prev_close: float, cutoff: str,
    tolerance: float, poll_seconds: int,
) -> int:
    """Replay R7 over one ticker's session and return its hit count."""
    df = frame[frame.index.tz_convert(ET).strftime("%H:%M") <= cutoff]
    if df.empty:
        return 0
    running_high = df["High"].cummax()
    last = df["Close"]
    idx = df.index

    # Poll instants every `poll_seconds` from the first bar; each poll reads the
    # most recent bar at or before it, which is how a live scanner sees the tape.
    start, end = idx[0], idx[-1]
    polls = pd.date_range(start, end, freq=f"{poll_seconds}s")
    pos = idx.searchsorted(polls, side="right") - 1
    pos = pos[pos >= 0]

    chg = (last.iloc[pos].to_numpy() / prev_close) - 1.0
    at_high = last.iloc[pos].to_numpy() >= running_high.iloc[pos].to_numpy() * (1 - tolerance)
    return int(np.sum((chg >= MOVE_THRESHOLD) & at_high))


def score(
    bars: dict[str, pd.DataFrame], prev_close: dict[str, float],
    cutoff: str, tolerance: float, poll_seconds: int,
) -> tuple[float, int, float]:
    """Return (spearman, n, mean_hits) for one parameter set."""
    pred, obs = {}, {}
    for t, frame in bars.items():
        pred[t] = simulate(frame, prev_close[t], cutoff, tolerance, poll_seconds)
        obs[t] = OBSERVED[t]
    if len(pred) < 10:
        return float("nan"), len(pred), float("nan")
    p, o = pd.Series(pred), pd.Series(obs)
    return float(p.corr(o, method="spearman")), len(p), float(p.mean())


def main() -> int:
    print("=" * 72)
    print("Highs / Lows hit-rule back-test  (U3 gate)")
    print("=" * 72)
    bars, prev_close = load_bars()
    print(f"  usable tickers: {len(bars)} / {len(OBSERVED)}\n")
    if len(bars) < 20:
        print("  FAIL - too few tickers resolved to score the rule.")
        print("  yfinance only serves ~7 days of 1-minute history; if the")
        print("  session has aged out, this harness needs cached bars.")
        return 1

    print("  Adopted rule (R7): move >= 3% AND last within tolerance of day high")
    print(f"  {'cutoff':>8} {'tol':>8} {'poll':>6} {'n':>4} {'mean hits':>10} {'spearman':>10}")
    best = (-2.0, None)
    for cutoff in CUTOFFS:
        rho, n, mean_hits = score(bars, prev_close, cutoff, DEFAULT_TOLERANCE, DEFAULT_POLL_SECONDS)
        print(f"  {cutoff:>8} {DEFAULT_TOLERANCE:>8.4f} {DEFAULT_POLL_SECONDS:>5}s {n:>4} {mean_hits:>10.1f} {rho:>10.3f}")
        if rho > best[0]:
            best = (rho, cutoff)

    print("\n  Tolerance sensitivity (cutoff 14:00) - the parameter U3 flagged")
    print("  as degenerate at both ends:")
    print(f"  {'tol':>8} {'mean hits':>10} {'spearman':>10}")
    for tol in (0.0, 0.0005, 0.001, 0.0015, 0.003, 0.005, 0.01, 0.02):
        rho, _, mean_hits = score(bars, prev_close, "14:00", tol, DEFAULT_POLL_SECONDS)
        print(f"  {tol:>8.4f} {mean_hits:>10.1f} {rho:>10.3f}")

    print("\n  Baselines for comparison:")
    gate_only, obs_s = {}, pd.Series(OBSERVED)
    for t, frame in bars.items():
        df = frame[frame.index.tz_convert(ET).strftime("%H:%M") <= "14:00"]
        polls = pd.date_range(df.index[0], df.index[-1], freq=f"{DEFAULT_POLL_SECONDS}s")
        pos = df.index.searchsorted(polls, side="right") - 1
        pos = pos[pos >= 0]
        chg = (df["Close"].iloc[pos].to_numpy() / prev_close[t]) - 1.0
        gate_only[t] = int(np.sum(chg >= MOVE_THRESHOLD))
    g = pd.Series(gate_only)
    print(f"    move-gate only, no at-high leg : {g.corr(obs_s[g.index], method='spearman'):.3f}")

    rho, cutoff = best
    print("\n" + "-" * 72)
    print(f"  Best Spearman {rho:.3f} at cutoff {cutoff} (bar: {PASS_BAR:.2f})")
    if np.isnan(rho) or rho < PASS_BAR:
        print("  VERDICT: FAIL - the adopted rule does not reproduce the source's")
        print("  hit ordering. Stop before U4: the reconstruction needs rework,")
        print("  not the implementation.")
        return 1
    print("  VERDICT: PASS - the adopted rule reproduces the source's hit")
    print("  ordering. U3's gate clears; U4 onward may proceed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
