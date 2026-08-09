"""Fetch intraday OHLC bars from TradingView using an existing session cookie.

The screener endpoint (`tradingview-screener`) returns current snapshot rows
only -- it has no bar history. TradingView serves history over a separate
websocket channel, which is what this module speaks.

Auth reuses the `sessionid` cookie already in `.env`; it never performs a
programmatic login (see R3 in the Highs / Lows plan -- automated signin invites
CAPTCHA and account flagging).

    uv run --with websocket-client python tools/tv_history.py WOLF --bars 400

Protocol notes, since it is undocumented and easy to get wrong:
  * every frame is length-prefixed as ``~m~<len>~m~<payload>``
  * the server sends ``~h~`` heartbeats that must be echoed or it disconnects
  * bars arrive inside ``timescale_update`` messages, keyed by series id
"""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import string
import sys

import pandas as pd
import requests

WS_URL = "wss://data.tradingview.com/socket.io/websocket?from=chart%2F"
ORIGIN = "https://www.tradingview.com"


def _session_id() -> tuple[str, str]:
    from dotenv import load_dotenv

    load_dotenv()
    sid = os.environ.get("TRADINGVIEW_SESSIONID", "").strip()
    sign = (
        os.environ.get("TRADINGVIEW_SESSIONID_SIGN", "").strip()
        or os.environ.get("TRADINGVIEW_SESSION_SIGN", "").strip()
    )
    if not sid:
        print("TRADINGVIEW_SESSIONID not set in .env")
        sys.exit(1)
    return sid, sign


def auth_token(sid: str, sign: str) -> str:
    """Exchange the session cookie for the websocket auth token.

    Returns the literal 'unauthorized_user_token' if the cookie does not yield
    one, which still streams delayed data -- the caller should treat that as a
    degraded result rather than a hard failure.
    """
    cookies = {"sessionid": sid}
    if sign:
        cookies["sessionid_sign"] = sign
    try:
        r = requests.get(
            ORIGIN + "/",
            cookies=cookies,
            headers={"User-Agent": "Mozilla/5.0", "Accept-Language": "en-US,en;q=0.9"},
            timeout=20,
        )
        m = re.search(r'"auth_token":"([^"]+)"', r.text)
        if m:
            return m.group(1)
    except Exception as exc:  # noqa: BLE001 - never surface cookie-bearing detail
        print(f"  auth token lookup failed: {type(exc).__name__}")
    return "unauthorized_user_token"


def _rand(prefix: str) -> str:
    return prefix + "".join(random.choices(string.ascii_lowercase, k=12))


def _frame(payload: str) -> str:
    return f"~m~{len(payload)}~m~{payload}"


def _msg(func: str, args: list) -> str:
    return _frame(json.dumps({"m": func, "p": args}, separators=(",", ":")))


def fetch_bars(symbol: str, interval: str = "1", bars: int = 400) -> pd.DataFrame:
    import websocket  # provided via --with websocket-client

    sid, sign = _session_id()
    token = auth_token(sid, sign)
    print(f"  auth token: {'authenticated' if token != 'unauthorized_user_token' else 'ANONYMOUS (delayed)'}")

    cookie = f"sessionid={sid}" + (f"; sessionid_sign={sign}" if sign else "")
    ws = websocket.create_connection(
        WS_URL,
        header=[f"Origin: {ORIGIN}", "User-Agent: Mozilla/5.0", f"Cookie: {cookie}"],
        timeout=20,
    )

    chart = _rand("cs_")
    ws.send(_msg("set_auth_token", [token]))
    ws.send(_msg("chart_create_session", [chart, ""]))
    ws.send(_msg("resolve_symbol", [
        chart, "sds_sym_1",
        '={"symbol":"' + symbol + '","adjustment":"splits","session":"regular"}',
    ]))
    ws.send(_msg("create_series", [chart, "sds_1", "s1", "sds_sym_1", interval, bars, ""]))

    collected: dict[int, list] = {}
    raw = ""
    for _ in range(400):
        try:
            raw = ws.recv()
        except Exception:
            break
        if not raw:
            break
        # Heartbeats must be echoed verbatim or the server drops the socket.
        if "~h~" in raw:
            ws.send(raw)
            continue
        for part in re.split(r"~m~\d+~m~", raw):
            if not part.startswith("{"):
                continue
            try:
                obj = json.loads(part)
            except json.JSONDecodeError:
                continue
            if obj.get("m") not in ("timescale_update", "du"):
                if obj.get("m") == "critical_error":
                    print(f"  server error: {obj.get('p')}")
                    ws.close()
                    return pd.DataFrame()
                continue
            for blob in obj.get("p", []):
                if not isinstance(blob, dict):
                    continue
                for _key, val in blob.items():
                    if not isinstance(val, dict) or "s" not in val:
                        continue
                    for row in val["s"]:
                        v = row.get("v")
                        if v and len(v) >= 5:
                            collected[int(v[0])] = v
        if collected and "series_completed" in raw:
            break
    ws.close()

    if not collected:
        return pd.DataFrame()
    df = pd.DataFrame(
        [[k] + v[1:6] for k, v in sorted(collected.items())],
        columns=["ts", "open", "high", "low", "close", "volume"],
    )
    df["time"] = pd.to_datetime(df["ts"], unit="s", utc=True).dt.tz_convert("America/New_York")
    return df.set_index("time").drop(columns="ts")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("symbol", help="bare ticker or EXCHANGE:TICKER")
    ap.add_argument("--interval", default="1")
    ap.add_argument("--bars", type=int, default=400)
    ap.add_argument("--date", default=None, help="filter to one YYYY-MM-DD session")
    ap.add_argument("--until", default=None, help="drop bars after HH:MM ET")
    args = ap.parse_args()

    sym = args.symbol if ":" in args.symbol else f"NASDAQ:{args.symbol}"
    df = fetch_bars(sym, args.interval, args.bars)
    if df.empty:
        print("  no bars returned")
        return 1
    if args.date:
        df = df[df.index.strftime("%Y-%m-%d") == args.date]
    if args.until:
        df = df[df.index.strftime("%H:%M") <= args.until]
    if df.empty:
        print("  no bars after filtering")
        return 1

    hh = int((df["high"] > df["high"].shift(1)).sum())
    ll = int((df["low"] < df["low"].shift(1)).sum())
    print(f"\n  {sym}  {args.interval}m bars: {len(df)}")
    print(f"  window: {df.index[0]:%Y-%m-%d %H:%M} -> {df.index[-1]:%H:%M} ET")
    print(f"  higher highs (high > prev high) : {hh}")
    print(f"  lower lows   (low  < prev low)  : {ll}")
    print(f"  net (hh - ll)                   : {hh - ll}")
    print(f"\n  first 3:\n{df.head(3).to_string()}")
    print(f"\n  last 3:\n{df.tail(3).to_string()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
