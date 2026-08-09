"""Verify the TradingView screener feed entitlement for the Highs / Lows tab.

This is the U2 stop-condition gate from
``docs/plans/2026-08-07-001-feat-highs-lows-tab-plan.md``: the whole live path
assumes an authenticated TradingView Premium session lifts the screener's
default 15-minute delay. Nothing downstream should be built until that is
confirmed against a live session.

The screener self-describes its entitlement in the ``update_mode`` field:

    delayed_streaming_900   -> 15-minute delayed (900s). The unauthenticated
                               baseline, confirmed 2026-08-08.
    streaming / realtime    -> real-time, what a Premium cookie should yield.

Run this **during US market hours** (9:30 AM - 4:00 PM ET). Outside them the
feed is not streaming for anyone and the check cannot distinguish entitlements.

Setup: put your TradingView session cookie in ``.env`` as

    TRADINGVIEW_SESSIONID=<value>
    TRADINGVIEW_SESSIONID_SIGN=<value>   # optional, see below

Copy them from your browser's devtools (Application -> Cookies ->
https://www.tradingview.com). TradingView sets both ``sessionid`` and
``sessionid_sign``; some endpoints validate the pair, so if ``sessionid``
alone comes back unauthenticated, add the sign cookie too.

Never paste either into a chat, a log, or a CI secret: together they are a full
account session token, revoked only by logging out of TradingView.

    uv run python tools/verify_hl_feed.py

Exit 0 when the authenticated feed is real-time, 1 otherwise.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from tradingview_screener import Query, col

ET = ZoneInfo("America/New_York")

# Universe floors fitted from the 2026-08-07 screenshot; see the plan's
# Reverse-Engineering Findings. Only used here to shape a realistic query.
MIN_PRICE = 5
MIN_TRADED_VALUE = 80_000_000

SELECT_FIELDS = [
    "name", "close", "change", "volume", "Value.Traded",
    "high", "low", "industry", "update_mode", "last_bar_update_time",
]

DELAYED_PREFIX = "delayed"


def scan(cookies: dict | None) -> tuple[int, str, int | None]:
    """Return (row_count, update_mode, last_bar_update_time) for one scan."""
    query = (
        Query()
        .set_markets("america")
        .select(*SELECT_FIELDS)
        .where(col("close") >= MIN_PRICE, col("Value.Traded") >= MIN_TRADED_VALUE)
        .limit(500)
    )
    # NOTE: the library rejects column arithmetic (col('close') * col('volume')
    # raises TypeError), which is why the floor uses the precomputed
    # Value.Traded field rather than composing one.
    _, df = query.get_scanner_data(cookies=cookies) if cookies else query.get_scanner_data()
    if df.empty:
        return 0, "", None
    modes = df["update_mode"].dropna().unique().tolist()
    mode = modes[0] if len(modes) == 1 else f"mixed:{modes}"
    bar_time = df["last_bar_update_time"].dropna().max()
    return len(df), str(mode), int(bar_time) if bar_time is not None else None


def describe(label: str, rows: int, mode: str, bar_time: int | None) -> None:
    print(f"\n  {label}")
    print(f"    rows returned : {rows}")
    print(f"    update_mode   : {mode or '(none)'}")
    if bar_time:
        stamp = datetime.fromtimestamp(bar_time, tz=timezone.utc).astimezone(ET)
        lag = (datetime.now(tz=timezone.utc) - datetime.fromtimestamp(bar_time, tz=timezone.utc))
        print(f"    newest bar    : {stamp:%Y-%m-%d %H:%M:%S %Z}  ({lag.total_seconds():.0f}s ago)")


def main() -> int:
    load_dotenv()
    cookie = os.environ.get("TRADINGVIEW_SESSIONID", "").strip()
    # Accept either spelling; the cookie itself is named `sessionid_sign`, but
    # `TRADINGVIEW_SESSION_SIGN` is the more natural env-var reading.
    cookie_sign = (
        os.environ.get("TRADINGVIEW_SESSIONID_SIGN", "").strip()
        or os.environ.get("TRADINGVIEW_SESSION_SIGN", "").strip()
    )
    jar = {"sessionid": cookie}
    if cookie_sign:
        jar["sessionid_sign"] = cookie_sign

    now_et = datetime.now(tz=ET)
    in_session = (
        now_et.weekday() < 5
        and (now_et.hour, now_et.minute) >= (9, 30)
        and now_et.hour < 16
    )

    print("=" * 68)
    print("TradingView feed entitlement check")
    print("=" * 68)
    print(f"  now: {now_et:%Y-%m-%d %H:%M:%S %Z}")
    if not in_session:
        print("\n  NOTE: outside US market hours (Mon-Fri 09:30-16:00 ET).")
        print("  update_mode describes the ENTITLEMENT, not live tick flow, so a")
        print("  PASS here is conclusive. Only a FAIL is ambiguous out of session.")

    try:
        base_rows, base_mode, base_time = scan(None)
    except Exception as exc:  # noqa: BLE001 - diagnostic script
        print(f"\n  unauthenticated scan failed: {type(exc).__name__}: {exc}")
        return 1
    describe("unauthenticated (baseline)", base_rows, base_mode, base_time)

    if not cookie:
        print("\n  TRADINGVIEW_SESSIONID is not set in .env - nothing to compare.")
        print("  Add it and re-run during market hours.")
        return 1

    sent = "sessionid + sessionid_sign" if cookie_sign else "sessionid only"
    try:
        auth_rows, auth_mode, auth_time = scan(jar)
    except Exception as exc:  # noqa: BLE001 - diagnostic script
        # Deliberately not printing the exception body: HTTP client errors can
        # embed the request cookie jar.
        print(f"\n  authenticated scan failed: {type(exc).__name__}")
        return 1
    describe(f"authenticated (Premium cookie: {sent})", auth_rows, auth_mode, auth_time)

    print("\n" + "-" * 68)
    if auth_rows == 0:
        print("  VERDICT: FAIL - authenticated scan returned no rows.")
        print("  The cookie is likely expired. Re-copy it from the browser.")
        return 1
    if auth_mode.startswith(DELAYED_PREFIX):
        print(f"  VERDICT: FAIL - authenticated feed still reports '{auth_mode}'.")
        if in_session:
            print("  The Premium subscription does not lift the delay on this")
            print("  endpoint. Stop: KD-2 and the live path need rethinking")
            print("  before U3-U8 are built. See the plan's A4 and Q-block.")
        else:
            print("  Inconclusive outside market hours - re-run in-session.")
        return 1
    if auth_mode == base_mode:
        print(f"  VERDICT: INCONCLUSIVE - both scans report '{auth_mode}'.")
        print("  The cookie changed nothing. Verify it is the live sessionid,")
        if not cookie_sign:
            print("  and try adding TRADINGVIEW_SESSIONID_SIGN as well - some")
            print("  endpoints validate the cookie pair rather than sessionid alone.")
        return 1

    print(f"  VERDICT: PASS - authenticated feed reports '{auth_mode}'")
    print(f"  (baseline was '{base_mode}').")
    print("  Real-time entitlement confirmed. U2's stop condition clears.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
