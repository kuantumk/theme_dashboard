"""Live-tape probe for the Highs / Lows scan mechanism.

The U3 back-test (``tools/backtest_hl_rule.py``) replays *yfinance minute bars*,
which are a reconstruction of the tape rather than the tape itself. This probe
tests the mechanism against a genuinely live feed by polling the TradingView
screener on a repeating interval and accumulating hits exactly as the live loop
would.

It defaults to crypto because crypto trades 24/7, so the mechanism can be
exercised outside US market hours. Pass ``--market equity`` during US market
hours to run the same probe against the real target universe.

What this probe DOES establish:
  * the session cookie authenticates a *repeated* live poll, not just one call
  * quotes actually change between polls (the feed is live, not a cached page)
  * the accumulator increments sanely and hit counts stay bounded by cycles

What it does NOT establish: whether our rule reproduces the source tool's hit
numbers. There is no ground-truth hit count for BTC to compare against; that
question belongs to the back-test against the 2026-08-07 screenshot.

    uv run python tools/live_probe_hl.py --polls 12 --interval 15
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from collections import defaultdict
from datetime import datetime

from dotenv import load_dotenv
from tradingview_screener import Query, col, screeners

MAJORS = ["BTC", "ETH", "XRP", "SOL", "BNB", "DOGE", "ADA", "TRX", "LINK", "AVAX"]

# "At the extreme" tolerance, as a fraction. Same parameter the equity rule uses.
TOLERANCE = 0.0015
MOVE_THRESHOLD_PCT = 3.0


def cookie_jar() -> dict:
    load_dotenv()
    sid = os.environ.get("TRADINGVIEW_SESSIONID", "").strip()
    if not sid:
        print("TRADINGVIEW_SESSIONID not set in .env")
        sys.exit(1)
    jar = {"sessionid": sid}
    sign = (
        os.environ.get("TRADINGVIEW_SESSIONID_SIGN", "").strip()
        or os.environ.get("TRADINGVIEW_SESSION_SIGN", "").strip()
    )
    if sign:
        jar["sessionid_sign"] = sign
    return jar


def scan_crypto(jar: dict, universe: str = "majors"):
    """Live crypto rows on a single liquid venue.

    ``majors`` is the stable control set; ``movers`` selects coins actually
    pressing their 24h high, which is what exercises the hit-increment path.
    Majors on a quiet session sit 0.2-1.2% below their 24h high and correctly
    never qualify.
    """
    q = (
        screeners.crypto()
        .select("base_currency", "close", "24h_close_change|5", "high", "low",
                "update_mode", "last_bar_update_time")
    )
    if universe == "movers":
        q = q.where(col("exchange") == "BINANCE", col("24h_close_change|5") >= 5)
    else:
        q = q.where(col("base_currency").isin(MAJORS), col("exchange") == "BINANCE")
    _, df = q.order_by("24h_vol|5", ascending=False).limit(60).get_scanner_data(cookies=jar)
    if df.empty:
        return df
    # One row per coin: the highest-volume pair for each base currency.
    df = df.drop_duplicates(subset="base_currency", keep="first")
    return df.rename(columns={"base_currency": "sym", "24h_close_change|5": "change"})


def scan_equity(jar: dict):
    _, df = (
        Query()
        .set_markets("america")
        .select("name", "close", "change", "high", "low", "Value.Traded",
                "update_mode", "last_bar_update_time")
        .where(col("close") >= 5, col("Value.Traded") >= 80_000_000)
        .order_by("change", ascending=False)
        .limit(300)
        .get_scanner_data(cookies=jar)
    )
    return df.rename(columns={"name": "sym"})


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--market", choices=["crypto", "equity"], default="crypto")
    ap.add_argument("--universe", choices=["majors", "movers"], default="majors")
    ap.add_argument("--polls", type=int, default=12)
    ap.add_argument("--interval", type=int, default=15)
    args = ap.parse_args()

    jar = cookie_jar()
    scan = (
        (lambda j: scan_crypto(j, args.universe))
        if args.market == "crypto"
        else scan_equity
    )

    hits_at_high: dict[str, int] = defaultdict(int)
    hits_new_high: dict[str, int] = defaultdict(int)
    # Session-relative extremes, tracked from the first poll. For crypto the
    # vendor `high`/`low` are 24h rolling, which is not the analogue of an
    # intraday session high -- a coin can sit 5% under a high set 20 hours ago
    # and never qualify. Anchoring the running max/min at probe start makes the
    # crypto probe test the same shape the equity session does.
    run_hi: dict[str, float] = {}
    run_lo: dict[str, float] = {}
    sess_new_high: dict[str, int] = defaultdict(int)
    sess_new_low: dict[str, int] = defaultdict(int)
    prev_close: dict[str, float] = {}
    prev_high: dict[str, float] = {}
    moved_counts: list[int] = []
    modes: set[str] = set()

    print("=" * 74)
    print(f"Live tape probe - market={args.market}  polls={args.polls}  every {args.interval}s")
    print("=" * 74)

    for i in range(1, args.polls + 1):
        t0 = time.time()
        try:
            df = scan(jar)
        except Exception as exc:  # noqa: BLE001 - diagnostic probe
            print(f"  poll {i:2d}  scan failed: {type(exc).__name__}")
            time.sleep(max(0, args.interval - (time.time() - t0)))
            continue
        if df.empty:
            print(f"  poll {i:2d}  no rows")
            time.sleep(max(0, args.interval - (time.time() - t0)))
            continue

        modes.update(df["update_mode"].dropna().unique().tolist())
        moved = 0
        for _, r in df.iterrows():
            sym, close, high = r["sym"], float(r["close"]), float(r["high"])
            if sym in prev_close and close != prev_close[sym]:
                moved += 1
            # Rule leg A: last price sitting at the running extreme.
            if close >= high * (1 - TOLERANCE):
                hits_at_high[sym] += 1
            # Rule leg B: the extreme itself advanced since the last poll (an
            # event, not a state) - only observable on a live tape.
            if sym in prev_high and high > prev_high[sym]:
                hits_new_high[sym] += 1
            # Session-relative: did this scan set a new extreme since probe start?
            if sym not in run_hi:
                run_hi[sym], run_lo[sym] = close, close
            elif close > run_hi[sym]:
                run_hi[sym] = close
                sess_new_high[sym] += 1
            elif close < run_lo[sym]:
                run_lo[sym] = close
                sess_new_low[sym] += 1
            prev_close[sym], prev_high[sym] = close, high
        if i > 1:
            moved_counts.append(moved)
        stamp = datetime.now().strftime("%H:%M:%S")
        print(f"  poll {i:2d}  {stamp}  rows={len(df):3d}  quotes_changed={moved:3d}"
              + ("" if i > 1 else "  (baseline)"))
        if i < args.polls:
            time.sleep(max(0, args.interval - (time.time() - t0)))

    print("\n" + "-" * 74)
    print(f"  update_mode observed : {sorted(modes)}")
    if moved_counts:
        tracked = len(prev_close)
        avg = sum(moved_counts) / len(moved_counts)
        print(f"  symbols tracked      : {tracked}")
        print(f"  avg quotes changed   : {avg:.1f} per poll ({avg / max(tracked,1):.0%} of symbols)")
    scans = args.polls - 1  # the first poll seeds the extremes, it cannot score
    print(f"\n  Session-relative extremes, anchored at probe start ({scans} scoring scans)")
    print(f"  {'symbol':>8} {'new-high hits':>14} {'new-low hits':>13} {'hi %of scans':>13}")
    order = sorted(run_hi, key=lambda s: -(sess_new_high[s] + sess_new_low[s]))
    for sym in order[:12]:
        pct = sess_new_high[sym] / scans if scans else 0
        print(f"  {sym:>8} {sess_new_high[sym]:>14d} {sess_new_low[sym]:>13d} {pct:>12.0%}")
    tot_hi = sum(sess_new_high.values())
    tot_lo = sum(sess_new_low.values())
    print(f"\n  totals: {tot_hi} new-high hits, {tot_lo} new-low hits across "
          f"{len(run_hi)} symbols x {scans} scans")
    if run_hi:
        best = max(sess_new_high.values()) if sess_new_high else 0
        print(f"  busiest symbol scored {best}/{scans} scans "
              f"({best / scans if scans else 0:.0%}) -- compare with the source's "
              f"WOLF at 142/238 (60%)")

    live = bool(moved_counts) and sum(moved_counts) > 0
    streaming = any(not m.startswith("delayed") for m in modes)
    print("\n" + "-" * 74)
    print(f"  cookie authenticates repeated polls : {'YES' if modes else 'NO'}")
    print(f"  feed reports streaming              : {'YES' if streaming else 'NO'}")
    print(f"  quotes change between polls         : {'YES' if live else 'NO'}")
    print(f"  hits bounded by cycles ({args.polls})        : "
          f"{'YES' if all(v <= args.polls for v in hits_at_high.values()) else 'NO'}")
    return 0 if (live and streaming) else 1


if __name__ == "__main__":
    sys.exit(main())
