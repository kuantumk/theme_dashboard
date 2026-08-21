"""TradingView screener client for the bid/ask dashboard.

One request per poll returns the entire filtered universe — verified at ~2,300
US tickers in 0.37s / 545KB — so there is no per-ticker fan-out and no batching.

Two library constraints shape this module:

* Column arithmetic is unsupported (`col('a') * col('b')` raises TypeError), so
  average *dollar* volume cannot be filtered server-side. It is computed from
  average share volume and price after the fetch.
* `Query().set_markets('crypto')` returns zero rows. The default query carries a
  hardcoded stocks-only type filter, so crypto must use the dedicated builder.

⛔ A field the scanner does not publish returns null for every row rather than
erroring, and a server-side floor on it then matches nothing at all.
`Value.Traded` — session-to-date traded value — is the case that bit. It served
values until 2026-08-20 and by 2026-08-21 it was gone from the scanner's
3,771-field metainfo (which lists every other column selected here) and null on
every row. Measured that morning at 09:34 ET with the market open and
`close`/`volume` both live: `Value.Traded >= $1M` matched 0 of 13,661 rows,
while the average-volume leg alone matched 2,806. The universe was therefore
empty on every poll and the equity tab went dark for a whole session with no
error anywhere — the same silent shape `tvquote.py` documents for `bid`/`ask`.
Today's traded value is now derived from `close * volume` and floored after the
fetch. Before pushing any new floor server-side, check the field is in metainfo.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import pandas as pd
from tradingview_screener import Query, col, screeners

from src.bidask.config import cookie_jar

# Feed reports its own entitlement here: `delayed_streaming_900` unauthenticated
# (900s = the documented 15-minute delay), `streaming` with a valid cookie pair.
DELAYED_PREFIX = "delayed"

EQUITY_COLUMNS = [
    "name", "close", "bid", "ask", "change", "volume",
    "relative_volume_10d_calc", "market_cap_basic", "sector", "industry",
    "High.1M", "Low.1M", "High.3M", "Low.3M", "High.6M", "Low.6M",
    "price_52_week_high", "price_52_week_low",
    "update_mode", "last_bar_update_time", "current_session",
]

# `current_session` reports whether the market is actually trading, which is a
# different question from `update_mode` (whether our *feed* is real-time). A
# real-time entitlement on a closed market is still a closed market.
SESSION_LABELS = {
    # `market` is what the feed actually sends while the regular session runs —
    # verified live, 2026-08-12. `regular` was assumed and never observed; an
    # unmapped value falls through to the raw string, which then fails the UI's
    # equality test and renders an open market in the "delayed" style.
    "market": "market open",
    "regular": "market open",
    "extended": "extended hours",
    "pre_market": "pre-market",
    "premarket": "pre-market",
    "post_market": "after hours",
    "postmarket": "after hours",
    "out_of_session": "market closed",
    "holiday": "holiday",
}

CRYPTO_COLUMNS = [
    "base_currency", "close", "bid", "ask", "24h_close_change|5", "volume",
    "high", "low", "update_mode", "last_bar_update_time",
]

CRYPTO_EXCHANGE = "BINANCE"


@dataclass
class Payload:
    """One poll's result."""

    rows: pd.DataFrame
    feed: str = ""
    error: str = ""
    matched: int = 0
    market_status: str = ""
    extra: dict = field(default_factory=dict)

    @property
    def market_open(self) -> bool:
        return self.market_status in ("market open", "extended hours",
                                      "pre-market", "after hours")

    @property
    def delayed(self) -> bool:
        """True when the feed is not serving real-time data.

        Unrecognised values count as delayed: mislabelling a delayed banner is a
        cosmetic error, while presenting 15-minute-old prices as live is not.
        """
        return not self.feed or self.feed.startswith(DELAYED_PREFIX)

    @property
    def ok(self) -> bool:
        return not self.error and not self.rows.empty


def _feed_mode(df: pd.DataFrame) -> str:
    if "update_mode" not in df.columns:
        return ""
    modes = df["update_mode"].dropna().unique().tolist()
    if not modes:
        return ""
    return str(modes[0]) if len(modes) == 1 else "mixed"


def _market_status(df: pd.DataFrame) -> str:
    """Human-readable trading state, from the feed's own session field."""
    if "current_session" not in df.columns:
        return ""
    values = df["current_session"].dropna().unique().tolist()
    if not values:
        return ""
    raw = str(values[0]).strip().lower()
    return SESSION_LABELS.get(raw, raw.replace("_", " "))


def _traded_value(df: pd.DataFrame) -> pd.Series:
    """Price x volume, coerced so the product is always numeric.

    Both legs are coerced because the multiplication is on the poll path and
    sits OUTSIDE the fetcher's try block. Nulls alone are safe — pandas yields
    NaN, which fails the floor and drops the row, which is the intent. A
    *string* column is not: `"21" * 3` is `"212121"`, and comparing that Series
    against the floor raises TypeError. That exception escapes `fetch_equities`
    and `poll_once` to the poll loop's generic handler, which prints a type
    name, backs off, and never writes the state file — so the page freezes on
    the last good poll while the console scrolls one line. Coercion turns the
    same vendor surprise into an empty board with an honest feed pill.

    `errors="coerce"` maps anything unparseable to NaN, so the documented
    fail-closed rule holds for every dtype the vendor can return, not just the
    float64 it returns today. Matches `universe.py`'s existing idiom.
    """
    price = pd.to_numeric(df["close"], errors="coerce")
    volume = pd.to_numeric(df["volume"], errors="coerce")
    return price * volume


def _bare_ticker(value: str) -> str:
    """Strip the exchange prefix the screener returns (`NASDAQ:WOLF` -> `WOLF`)."""
    text = str(value)
    return text.split(":", 1)[1] if ":" in text else text


def fetch_equities(cfg, limit: int = 3000) -> Payload:
    """Fetch the liquidity-filtered US equity universe in one request."""
    try:
        matched, df = (
            Query()
            .set_markets("america")
            .select(*EQUITY_COLUMNS, cfg.avg_volume_field)
            .where(col(cfg.avg_volume_field) >= cfg.min_avg_volume)
            # `limit` now truncates before the traded-value floor rather than
            # after it, so the order decides what is lost when the universe
            # outgrows the cap (2,807 of 3,000 on 2026-08-21). Session-to-date
            # share volume is the closest live stand-in for the `Value.Traded`
            # ordering it replaces: it drops the least active names TODAY,
            # rather than names that are merely usually quiet — which is where
            # a gapper on 20x its normal volume would sit.
            .order_by("volume", ascending=False)
            .limit(limit)
            .get_scanner_data(cookies=cookie_jar())
        )
    except Exception as exc:  # noqa: BLE001 — never surface cookie-bearing detail
        # Deliberately not including str(exc): HTTP client errors can embed the
        # request URL and cookie jar, and this payload's shape is shared with
        # what the UI renders.
        return Payload(rows=pd.DataFrame(), error=type(exc).__name__)

    # Read the feed mode and the session BEFORE any row is filtered out. Both
    # describe the response, not the rows that survive it, and the UI renders a
    # missing one as an assertion rather than as an absence.
    #
    # Be precise about how much this covers, because it is less than it looks.
    # Both helpers read their value off the ROWS, so a response that carried
    # none has no reading to preserve and returns "" whenever it is called.
    # Hoisting them therefore fixes only the case where rows arrived and OUR
    # floor removed them all — not the zero-row case that actually went dark.
    # What covers that one is the wording in `web/app.js`: an empty `feed`
    # renders as "feed unknown" rather than "delayed feed", so an absent
    # reading stops accusing a streaming vendor of a stale one.
    feed, status = _feed_mode(df), _market_status(df)
    if df.empty:
        return Payload(rows=df, feed=feed, matched=matched, market_status=status)

    # Guarded like the query above, because this block now does work the query
    # used to do. `Value.Traded` was a `.where()` clause, so a bad column came
    # back inside the try as a visible `feed error` pill. Reading `df["close"]`
    # here instead raises KeyError if the vendor withdraws THAT column the way
    # it just withdrew `Value.Traded` — and nothing catches it: not this
    # function, not `poll_once`, only the poll loop's own handler, which prints
    # a type name and backs off without ever calling `write_state`. Every field
    # on the page then freezes at its last value, `generated_at` included, and
    # both markets lose the cycle because one write covers the whole loop. That
    # is worse than the bug this file was opened to fix: wrong-but-visible
    # pills at least said something was wrong.
    try:
        df = df.copy()
        df["symbol"] = df["ticker"].map(_bare_ticker) if "ticker" in df.columns else df["name"]
        df["avg_volume"] = df[cfg.avg_volume_field]
        df["change_pct"] = df["change"]
        df["rvol"] = df.get("relative_volume_10d_calc")
        # Session-to-date traded value, derived rather than read — see the
        # module docstring. Outside the regular session this carries the
        # previous session's figure, which is the right liquidity proxy while
        # today's does not exist yet.
        df["dollar_vol"] = _traded_value(df)
        # The floor the query can no longer push server-side. A NaN price or
        # volume fails the comparison, so an unknown never clears a floor as
        # though it had qualified — the same fail-closed rule the classifier's
        # quote guards obey.
        df = df[df["dollar_vol"] >= cfg.min_today_dollar_vol]
    except Exception as exc:  # noqa: BLE001 — a dead column must not freeze the page
        return Payload(rows=pd.DataFrame(), feed=feed, error=type(exc).__name__,
                       matched=matched, market_status=status)
    return Payload(rows=df, feed=feed, matched=matched, market_status=status)


def fetch_crypto(cfg, limit: int = 200) -> Payload:
    """Fetch live crypto rows. Works outside US market hours."""
    try:
        matched, df = (
            screeners.crypto()
            .select(*CRYPTO_COLUMNS)
            .where(col("exchange") == CRYPTO_EXCHANGE)
            .order_by("24h_vol|5", ascending=False)
            .limit(limit)
            .get_scanner_data(cookies=cookie_jar())
        )
    except Exception as exc:  # noqa: BLE001
        return Payload(rows=pd.DataFrame(), error=type(exc).__name__)

    # Read once, before any filtering, for the reason `fetch_equities` gives.
    # Crypto's status is a constant rather than a reading, and withholding a
    # constant because a response came back empty is the same asymmetry.
    feed = _feed_mode(df)
    if df.empty:
        return Payload(rows=df, feed=feed, matched=matched, market_status="24/7")

    # Some venue rows carry a null base_currency; without this they surface as a
    # literal "nan" ticker in the UI.
    df = df[df["base_currency"].notna()]
    if df.empty:
        return Payload(rows=df, feed=feed, matched=matched, market_status="24/7")
    df = df.drop_duplicates(subset="base_currency", keep="first").copy()
    df["symbol"] = df["base_currency"]
    df["change_pct"] = df["24h_close_change|5"]
    # Crypto has no session-scoped average-volume field and no auction windows,
    # so the equity liquidity gate does not apply. `high`/`low` are 24h rolling
    # rather than session extremes — see A3 in the plan.
    df["avg_volume"] = None
    df["rvol"] = None
    # Crypto `volume` is 24h rolling rather than session-to-date — there is no
    # session to date from. Labelled as 24h in the UI so it is not read as the
    # same quantity the equity tab shows.
    df["dollar_vol"] = _traded_value(df)
    return Payload(rows=df, feed=feed, matched=matched, market_status="24/7")


def fetch(market: str, cfg, limit: Optional[int] = None) -> Payload:
    if market == "crypto":
        return fetch_crypto(cfg, limit or 200)
    return fetch_equities(cfg, limit or 3000)
