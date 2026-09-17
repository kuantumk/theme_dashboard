"""TradingView chart websocket — the only source of extended-hours volume.

`src/bidask/rvol_at_time.py` needs each ticker's own 04:00-anchored volume
history to compute Relative Volume at Time before the opening bell. Nothing
else in reach supplies it:

* **yfinance returns the bars with zero volume on every one.** With
  `prepost=True` it serves 66 pre-market and 48 after-hours 5-minute bars per
  session and every one carries 0. `CLAUDE.md` already records this for the EP
  scan; it was confirmed again on 2026-09-17 across six probe symbols and 15
  sessions. A baseline built from those is a zero denominator, not a quiet
  ticker, and every ratio against it is infinite.
* **No screener column answers it either.** Every relative-volume field the
  `america` scanner serves replays the *previous* completed session during
  pre-market — across 300 symbols over a 201-second gap, pre-market volume rose
  for 262 of them while `relative_volume_10d_calc`, `relative_volume_intraday|5`
  and every `relative_volume|N` variant changed for **0 of 300**.

What does work is the service TradingView's own chart uses: the same
`data.tradingview.com` socket the quote stream already speaks, opened with
`?from=chart/`, authenticated with the same `sessionid`-minted JWT. Measured
2026-09-17: 2,500 five-minute bars, roughly eleven extended sessions, carrying
**real** extended-hours volume — 53 pre-market bars and 239,052 shares for
GNRC that morning against 200-5,724 on each of the ten prior sessions.

⛔ **Do not go looking for the study.** `create_study` for Relative Volume at
Time is refused with `Study not allowed in this connection` on both the `data`
and `prodata` hosts — it is Pine-based and Pine studies are blocked on this
connection tier, while a built-in `Volume@tv-basicstudies` study returns values
normally. The refusal is about the study class, not the route or the
credentials. The bars are the way in, and the division is one line of numpy.

⛔ **`"session": "extended"` is what moves the first bar to 04:00.** Without it
the series starts at 09:30 and the whole module answers the wrong question with
no error anywhere. Symbols need an exchange prefix (`NYSE:GNRC`); the socket
resolves nothing from a bare ticker.

Auth and framing are imported from `src/bidask/tvquote.py` rather than
restated. Two copies of a `~m~<len>~m~` parser would drift, and the quote
socket's version already carries the reason it slices by declared length
instead of matching braces.

Cost
----
One series request per symbol, so this belongs in the once-per-session baseline
warm-up, never in the poll loop. Workers each own a connection and drain their
own slice, so throughput scales with `workers` rather than with the round trip.
"""

from __future__ import annotations

import json
import random
import string
import threading
import time
from typing import Iterable, Optional

import websocket

from src.bidask.tvquote import HEADERS, ORIGIN, QuoteAuthError, auth_token, encode, iter_frames

SOCKET_URL = "wss://data.tradingview.com/socket.io/websocket?from=chart%2F"

# "extended" is load-bearing — see the module docstring. "regular" silently
# gives a 09:30-anchored series, which is the question this module replaced.
SESSION_KIND = "extended"

# 5-minute bars, matching `rvol_at_time.BAR_MINUTES`. An extended day holds 192
# slots, so 2,500 covers roughly thirteen sessions of a continuously-traded
# name and comfortably more of a quiet one, whose empty windows return no bar.
DEFAULT_INTERVAL = "5"
DEFAULT_BAR_COUNT = 2500

# Exchanges tried, in order, for a caller that passes a bare ticker. Only the
# resolve round-trip is repeated on a miss, not the series request, so a NYSE
# name costs one extra frame rather than a second download.
DEFAULT_EXCHANGES = ("NASDAQ", "NYSE", "AMEX")

# Socket read timeout, and the wall-clock budget one symbol may take before the
# worker gives up and moves on. A single unresolvable ticker must not stall a
# 1,900-symbol warm-up.
READ_TIMEOUT = 2.0
SYMBOL_TIMEOUT = 20.0
CONNECT_TIMEOUT = 20.0

# ⛔ Two ways a series ends with no bars, and they must not be conflated.
#
# A wrong exchange sends `symbol_error` ("invalid symbol") and then
# `series_error` ("resolve error"), and **never sends `series_completed`** —
# measured 2026-09-17 against NASDAQ:GNRC and NASDAQ:NOTAREALSYM. So waiting
# for a clean completion to decide a symbol is absent costs the full
# `SYMBOL_TIMEOUT` on every fallback candidate. `symbol_error` is that answer,
# it arrives in under a second, and it means "try the next exchange".
#
# `series_error` always *follows* `symbol_error` in that case, so returning on
# the first one means any `series_error` reaching this list arrived on its own
# — a different cause, and one a fresh connection may well fix.
NOT_LISTED_METHODS = ("symbol_error",)
REFUSAL_METHODS = ("series_error", "critical_error", "protocol_error")

# ⛔ Concurrent sockets. **Do not raise this to speed up the warm-up** — there
# is a ceiling and crossing it loses symbols rather than slowing down. Measured
# 2026-09-17 over one 60-symbol list: 6 workers returned 60 of 60 on both runs,
# 8 returned 57, and 12 returned 35. A lost symbol has no baseline, scores 0
# and is excluded, so the board simply thins with nothing on screen to say why.
# The retry below recovers a refusal, but it cannot recover throughput that was
# never there. 6 workers covers ~1,900 tickers in 3-5 minutes, against the
# ~1.7 minutes of the yfinance warm-up it replaces; launch before the bell.
DEFAULT_WORKERS = 6


class _SocketRefused(RuntimeError):
    """The socket declined or went quiet — not an answer about the symbol."""


def _rand(prefix: str) -> str:
    return prefix + "".join(random.choices(string.ascii_lowercase, k=12))


def qualified_symbols(symbol: str) -> list[str]:
    """Exchange-qualified candidates to try for one caller-supplied symbol.

    An `EXCHANGE:TICKER` string is used as given. A bare ticker gets the
    `DEFAULT_EXCHANGES` chain, because the caller's own universe row may carry
    only the symbol while the socket resolves nothing without an exchange.
    """
    symbol = (symbol or "").strip().upper()
    if not symbol:
        return []
    if ":" in symbol:
        return [symbol]
    return [f"{exchange}:{symbol}" for exchange in DEFAULT_EXCHANGES]


def absorb_bars(message: dict, collected: dict) -> None:
    """Fold one `timescale_update` / `du` message into `collected`.

    Bars arrive under the series id as `{"s": [{"i": n, "v": [ts, o, h, l, c,
    volume]}, ...]}`. Keying by the bar's unix timestamp rather than appending
    is what makes the forming bar safe: the socket resends it on every update,
    and the newest value simply replaces the older one.
    """
    if not isinstance(message, dict):
        return
    if message.get("m") not in ("timescale_update", "du"):
        return
    params = message.get("p")
    if not isinstance(params, list):
        return
    for blob in params:
        if not isinstance(blob, dict):
            continue
        for value in blob.values():
            if not isinstance(value, dict):
                continue
            rows = value.get("s")
            if not isinstance(rows, list):
                continue
            for row in rows:
                if not isinstance(row, dict):
                    continue
                bar = row.get("v")
                if isinstance(bar, list) and len(bar) >= 6:
                    try:
                        collected[int(bar[0])] = bar
                    except (TypeError, ValueError):
                        continue


def bars_to_frame(collected: dict):
    """Reduce collected bars to an ET-indexed OHLCV frame.

    Column names match yfinance's capitalisation so `rvol_at_time.build_profiles`
    reads `Volume` from either source without a translation layer.
    """
    import pandas as pd

    if not collected:
        return pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])
    stamps = sorted(collected)
    frame = pd.DataFrame(
        [[float(v) if v is not None else float("nan") for v in collected[s][1:6]]
         for s in stamps],
        columns=["Open", "High", "Low", "Close", "Volume"],
    )
    index = pd.to_datetime(stamps, unit="s", utc=True).tz_convert("America/New_York")
    frame.index = index
    return frame


class _Connection:
    """One authenticated chart socket, drained one symbol at a time."""

    def __init__(self, token: str, *, interval: str, bars: int):
        self._interval = interval
        self._bars = bars
        self._socket = websocket.create_connection(
            SOCKET_URL,
            header=[f"{k}: {v}" for k, v in HEADERS.items()],
            origin=ORIGIN,
            timeout=CONNECT_TIMEOUT,
        )
        self._socket.send(encode("set_auth_token", [token]))
        self._socket.settimeout(READ_TIMEOUT)

    def close(self) -> None:
        try:
            self._socket.close()
        except Exception:  # noqa: BLE001 — already tearing down
            pass

    def series(self, symbol: str) -> tuple[dict, str]:
        """Collected bars for one exchange-qualified symbol, and how it ended.

        The reason matters. `completed` and `symbol_error` are answers *about
        the symbol* — try the next exchange. Anything else is the socket
        refusing or going quiet, which is a transport problem and deserves a
        fresh connection. Collapsing the two is what makes a concurrency limit
        look like a delisted ticker and thins the board with no visible cause.
        """
        chart = _rand("cs_")
        spec = {"symbol": symbol, "adjustment": "splits", "session": SESSION_KIND}
        self._socket.send(encode("chart_create_session", [chart, ""]))
        self._socket.send(encode("resolve_symbol",
                                 [chart, "sym_1", "=" + json.dumps(spec, separators=(",", ":"))]))
        self._socket.send(encode("create_series",
                                 [chart, "sds_1", "s1", "sym_1", self._interval, self._bars, ""]))

        collected: dict = {}
        reason = "timeout"
        deadline = time.time() + SYMBOL_TIMEOUT
        done = False
        while not done and time.time() < deadline:
            try:
                raw = self._socket.recv()
            except websocket.WebSocketTimeoutException:
                continue
            if not raw:
                continue
            for payload in iter_frames(raw):
                if payload.startswith("~h~"):
                    # Echo verbatim. The server drops a connection that stops
                    # answering, and the drop looks like an empty history here.
                    self._socket.send(f"~m~{len(payload)}~m~{payload}")
                    continue
                try:
                    message = json.loads(payload)
                except (json.JSONDecodeError, TypeError):
                    continue
                if not isinstance(message, dict):
                    continue
                method = message.get("m")
                if method in NOT_LISTED_METHODS or method in REFUSAL_METHODS:
                    collected, reason, done = {}, str(method), True
                    break
                absorb_bars(message, collected)
                if method == "series_completed":
                    reason, done = "completed", True
                    break
        try:
            self._socket.send(encode("chart_delete_session", [chart]))
        except Exception:  # noqa: BLE001 — the socket is going away anyway
            pass
        return collected, reason


def fetch_bars(symbols: Iterable[str], *, interval: str = DEFAULT_INTERVAL,
               bars: int = DEFAULT_BAR_COUNT, workers: int = DEFAULT_WORKERS,
               token: Optional[str] = None) -> dict:
    """Extended-session 5-minute bars, keyed by the caller's own symbol string.

    The key is whatever was passed in — bare or exchange-qualified — so the
    caller's profile table keeps one naming convention end to end. A symbol
    that resolves on no exchange is simply absent from the result; callers
    fail closed on a missing baseline rather than guessing one.

    Raises `QuoteAuthError` when the session cookie is missing or rejected. An
    empty return, by contrast, means the socket answered and had nothing — the
    two must stay distinguishable, or a dead credential reads as a dead market.
    """
    wanted = [s for s in dict.fromkeys(str(s) for s in symbols if s) if s]
    if not wanted:
        return {}
    token = token or auth_token()

    collected: dict = {}
    lock = threading.Lock()
    queue = list(wanted)
    cursor = [0]

    def worker() -> None:
        connection = None
        while True:
            with lock:
                if cursor[0] >= len(queue):
                    break
                symbol = queue[cursor[0]]
                cursor[0] += 1
            for attempt in range(2):
                try:
                    if connection is None:
                        connection = _Connection(token, interval=interval, bars=bars)
                    frame = _resolve_one(connection, symbol)
                except QuoteAuthError:
                    raise
                except Exception:  # noqa: BLE001 — one dropped socket, not the warm-up
                    if connection is not None:
                        connection.close()
                    connection = None
                    if attempt == 0:
                        continue
                    frame = None
                break
            if frame is not None and not frame.empty:
                with lock:
                    collected[symbol] = frame
        if connection is not None:
            connection.close()

    threads = [threading.Thread(target=worker, daemon=True, name=f"tvbars-{i}")
               for i in range(max(1, min(workers, len(wanted))))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return collected


def _resolve_one(connection: "_Connection", symbol: str):
    """Try each exchange candidate until one returns bars.

    Returns the frame, or raises `_SocketRefused` when the socket refused or
    went quiet rather than answering that the symbol is not there. The caller
    reconnects on that; falling through to the next exchange instead would
    charge a transport failure to the symbol and drop it silently.
    """
    for candidate in qualified_symbols(symbol):
        rows, reason = connection.series(candidate)
        if rows:
            return bars_to_frame(rows)
        if reason not in ("completed",) + NOT_LISTED_METHODS:
            raise _SocketRefused(reason)
    return None
