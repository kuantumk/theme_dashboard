"""TradingView screener client for the bid/ask dashboard.

One request per poll returns the entire filtered universe — verified at ~2,300
US tickers in 0.37s / 545KB — so there is no per-ticker fan-out and no batching.

Two library constraints shape this module:

* Column arithmetic is unsupported (`col('a') * col('b')` raises TypeError), so
  average *dollar* volume cannot be filtered server-side. It is computed from
  average share volume and price after the fetch.
* `Query().set_markets('crypto')` returns zero rows. The default query carries a
  hardcoded stocks-only type filter, so crypto must use the dedicated builder.
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
    "name", "close", "bid", "ask", "change", "volume", "Value.Traded",
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
            .where(
                col("Value.Traded") >= cfg.min_today_dollar_vol,
                col(cfg.avg_volume_field) >= cfg.min_avg_volume,
            )
            .order_by("Value.Traded", ascending=False)
            .limit(limit)
            .get_scanner_data(cookies=cookie_jar())
        )
    except Exception as exc:  # noqa: BLE001 — never surface cookie-bearing detail
        # Deliberately not including str(exc): HTTP client errors can embed the
        # request URL and cookie jar, and this payload's shape is shared with
        # what the UI renders.
        return Payload(rows=pd.DataFrame(), error=type(exc).__name__)

    if df.empty:
        return Payload(rows=df, matched=matched)

    df = df.copy()
    df["symbol"] = df["ticker"].map(_bare_ticker) if "ticker" in df.columns else df["name"]
    df["avg_volume"] = df[cfg.avg_volume_field]
    df["change_pct"] = df["change"]
    df["rvol"] = df.get("relative_volume_10d_calc")
    # Session-to-date traded value. `Value.Traded` is the feed's own figure;
    # fall back to price x volume when it is absent.
    df["dollar_vol"] = df["Value.Traded"].fillna(df["close"] * df["volume"])
    return Payload(rows=df, feed=_feed_mode(df), matched=matched,
                   market_status=_market_status(df))


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

    if df.empty:
        return Payload(rows=df, matched=matched)

    # Some venue rows carry a null base_currency; without this they surface as a
    # literal "nan" ticker in the UI.
    df = df[df["base_currency"].notna()]
    if df.empty:
        return Payload(rows=df, matched=matched)
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
    df["dollar_vol"] = df["close"] * df["volume"]
    return Payload(rows=df, feed=_feed_mode(df), matched=matched,
                   market_status="24/7")


def fetch(market: str, cfg, limit: Optional[int] = None) -> Payload:
    if market == "crypto":
        return fetch_crypto(cfg, limit or 200)
    return fetch_equities(cfg, limit or 3000)
