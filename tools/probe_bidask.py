"""Live bid/ask pressure probe for the Highs / Lows hit hypothesis.

Hypothesis under test: a "hit" is the last trade printing at the ask (aggressive
buying) or at the bid (aggressive selling). Unlike new-high events -- which
measurement showed fire on only 5-45% of scans and cannot produce the source
screenshot's 60% for WOLF -- trading at the ask is a *persistent state* that can
plausibly hold across most scans of a strongly bid tape.

Classification per scan, strict first then midpoint fallback:

    last >= ask  -> ask hit  (buyer initiated)
    last <= bid  -> bid hit  (seller initiated)
    otherwise    -> inside the spread; midpoint decides, ties unclassified

Crypto is used because it quotes 24/7 and TradingView returns live bid/ask for
it. Equity bid/ask fields exist on the screener but return null out of session,
so the equity form of this probe must be run during US market hours.

    uv run python tools/probe_bidask.py --polls 24 --interval 10
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from collections import defaultdict
from datetime import datetime

from dotenv import load_dotenv
from tradingview_screener import col, screeners

MAJORS = ["BTC", "ETH", "XRP", "SOL", "BNB", "DOGE", "ADA", "TRX", "LINK", "AVAX"]


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


def scan(jar: dict):
    _, df = (
        screeners.crypto()
        .select("base_currency", "close", "bid", "ask", "24h_close_change|5",
                "update_mode")
        .where(col("base_currency").isin(MAJORS), col("exchange") == "BINANCE")
        .order_by("24h_vol|5", ascending=False)
        .limit(60)
        .get_scanner_data(cookies=jar)
    )
    if df.empty:
        return df
    return df.drop_duplicates(subset="base_currency", keep="first")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--polls", type=int, default=24)
    ap.add_argument("--interval", type=int, default=10)
    args = ap.parse_args()

    jar = cookie_jar()
    ask_hits: dict[str, int] = defaultdict(int)
    bid_hits: dict[str, int] = defaultdict(int)
    inside: dict[str, int] = defaultdict(int)
    scored: dict[str, int] = defaultdict(int)
    spreads: list[float] = []
    quote_missing = 0
    # First/last print per symbol, so we can test whether ask-hit rate carries
    # information beyond simple price direction over the window. Every earlier
    # candidate metric collapsed into a proxy for % change; this is that check.
    first_px: dict[str, float] = {}
    last_px: dict[str, float] = {}

    print("=" * 76)
    print(f"Bid/ask pressure probe - {args.polls} scans every {args.interval}s")
    print("=" * 76)

    for i in range(1, args.polls + 1):
        t0 = time.time()
        try:
            df = scan(jar)
        except Exception as exc:  # noqa: BLE001 - diagnostic probe
            print(f"  scan {i:2d}  failed: {type(exc).__name__}")
            time.sleep(max(0, args.interval - (time.time() - t0)))
            continue

        at_ask = at_bid = mid = 0
        for _, r in df.iterrows():
            sym = r["base_currency"]
            last, bid, ask = r["close"], r["bid"], r["ask"]
            if bid is None or ask is None or last is None or ask <= bid:
                quote_missing += 1
                continue
            scored[sym] += 1
            spreads.append((ask - bid) / last * 100)
            first_px.setdefault(sym, last)
            last_px[sym] = last
            if last >= ask:
                ask_hits[sym] += 1
                at_ask += 1
            elif last <= bid:
                bid_hits[sym] += 1
                at_bid += 1
            else:
                midpoint = (ask + bid) / 2
                if last > midpoint:
                    ask_hits[sym] += 1
                    at_ask += 1
                elif last < midpoint:
                    bid_hits[sym] += 1
                    at_bid += 1
                else:
                    inside[sym] += 1
                    mid += 1
        stamp = datetime.now().strftime("%H:%M:%S")
        print(f"  scan {i:2d}  {stamp}  ask-side={at_ask:3d}  bid-side={at_bid:3d}  unclassified={mid:2d}")
        if i < args.polls:
            time.sleep(max(0, args.interval - (time.time() - t0)))

    print("\n" + "-" * 76)
    if spreads:
        s = sorted(spreads)
        print(f"  median spread: {s[len(s)//2]:.4f}%   quotes missing: {quote_missing}")
    print(f"\n  {'symbol':>8} {'scans':>6} {'ask hits':>9} {'bid hits':>9} {'ask %':>7} {'net':>6}")
    for sym in sorted(scored, key=lambda x: -(ask_hits[x] - bid_hits[x])):
        n = scored[sym]
        pct = ask_hits[sym] / n if n else 0
        print(f"  {sym:>8} {n:>6d} {ask_hits[sym]:>9d} {bid_hits[sym]:>9d} "
              f"{pct:>6.0%} {ask_hits[sym]-bid_hits[sym]:>6d}")

    tot_a = sum(ask_hits.values())
    tot_b = sum(bid_hits.values())
    tot = tot_a + tot_b
    print(f"\n  tape-wide: {tot_a} ask-side vs {tot_b} bid-side "
          f"({tot_a / tot:.0%} buy pressure)" if tot else "\n  no classified scans")
    if scored:
        rates = [ask_hits[s] / scored[s] for s in scored if scored[s]]
        print(f"  per-symbol ask-hit rate: min {min(rates):.0%}  max {max(rates):.0%}")
        print("  (the source screenshot needs ~60% for WOLF, 9% for SAP -- a")
        print("   mechanism must be able to span that range)")

    # Independence check: if ask-hit rate merely tracks price direction over the
    # window, it carries no information that % change does not already provide.
    syms = [s for s in scored if s in first_px and first_px[s]]
    if len(syms) >= 5:
        import pandas as pd

        rate = pd.Series({s: ask_hits[s] / scored[s] for s in syms})
        move = pd.Series({s: last_px[s] / first_px[s] - 1 for s in syms})
        rho = rate.corr(move, method="spearman")
        print(f"\n  corr(ask-hit rate, price move over window): spearman {rho:.3f}")
        print("  low correlation => the measure is not a % change proxy;")
        print("  high correlation => it is, and adds nothing over price.")
        print(f"\n  {'symbol':>8} {'ask %':>7} {'move %':>8}")
        for s in sorted(syms, key=lambda x: -rate[x]):
            print(f"  {s:>8} {rate[s]:>6.0%} {move[s] * 100:>7.2f}%")
    return 0


if __name__ == "__main__":
    sys.exit(main())
