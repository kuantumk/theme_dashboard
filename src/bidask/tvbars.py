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

Auth and framing are imported from `src/bidask/tvsocket.py` rather than
restated. Two copies of a `~m~<len>~m~` parser would drift, and the shared
version already carries the reason it slices by declared length instead of
matching braces.

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

from src.bidask.tvsocket import HEADERS, ORIGIN, QuoteAuthError, auth_token, encode, iter_frames

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
# never there.
#
# Batching is the lever that DOES scale, because the limit is on connections
# rather than on concurrent series — see `batch_for`. Measured 2026-09-18,
# 3 connections x 8 series matched 6 x 8 exactly (5.92s against 5.98s on 60
# symbols), so past a handful of sockets the account, not the pool, is the
# constraint. 6 is kept because it is the figure with two completeness runs
# behind it.
DEFAULT_WORKERS = 6

# ⛔ Series opened on ONE connection and drained together, rather than one at a
# time. The socket serves several at once and the shipped code did not use
# that. Measured 2026-09-18 on 60 real screener symbols, 6 connections, all
# returning 60 of 60:
#
#     bars    batch    s/symbol
#     2,496       1    0.106 - 0.131   (the sequential drain this replaced)
#     2,496       8    0.087
#     2,496      16    0.203           <-- WORSE than 8
#       768       8    0.075
#       768      16    0.053
#       768      32    0.040
#
# ⛔ There is no single best batch size, and the 16-at-2,496 row is why. The
# account saturates near 27,000 bars/s, so once a batch has that much in flight
# a bigger one queues instead of parallelising. Below the ceiling the opposite
# holds: the bytes stop mattering and a fixed per-series cost dominates, which
# only a wider batch amortises — 768 bars is barely faster than 2,496 at batch
# 8, and three times faster at batch 32.
#
# So the rule holds bars-in-flight roughly constant rather than naming a batch.
# 20,000 reproduces the measured best at the cold size (8) and stays inside the
# measured-good range at the incremental one.
BARS_IN_FLIGHT = 20_000
MAX_BATCH = 32

# How long the drain tolerates complete silence before giving up on whatever
# has not answered. Applies to the BATCH, not to each symbol: every series is
# in flight at once, so the budget is a silence timeout rather than a sum. A
# symbol that never answers costs this once, not once per batch mate.
BATCH_IDLE_TIMEOUT = SYMBOL_TIMEOUT


def batch_for(bars: int) -> int:
    """Series to keep in flight on one connection for a request of `bars`.

    See `BARS_IN_FLIGHT`. Inversely proportional to the payload because the two
    regimes have opposite constraints, and capped because nothing above 32 has
    been measured for completeness.
    """
    return max(1, min(MAX_BATCH, BARS_IN_FLIGHT // max(1, int(bars))))


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


def _open_socket(token: str):
    """One authenticated chart socket.

    Separated from `_Connection` so the drain logic can be exercised against a
    replayed socket. Before that seam existed, the routing, fallback and
    refusal paths had no test coverage at all.
    """
    sock = websocket.create_connection(
        SOCKET_URL,
        header=[f"{k}: {v}" for k, v in HEADERS.items()],
        origin=ORIGIN,
        timeout=CONNECT_TIMEOUT,
    )
    sock.send(encode("set_auth_token", [token]))
    sock.settimeout(READ_TIMEOUT)
    return sock


class _Connection:
    """One authenticated chart socket, carrying several series at once."""

    def __init__(self, token: str, *, interval: str, bars: int):
        self._interval = interval
        self._bars = bars
        self._socket = _open_socket(token)

    def close(self) -> None:
        try:
            self._socket.close()
        except Exception:  # noqa: BLE001 — already tearing down
            pass

    def series_batch(self, symbols: list) -> list:
        """Bars and an end reason for each symbol, in the order given.

        Every series is opened before any is drained, which is the whole gain:
        the socket works on all of them at once. Positional results rather than
        a dict keyed by symbol, because two caller keys can resolve to the same
        exchange-qualified string and a dict would silently merge them.

        The reason matters as much as the bars. `completed` and `symbol_error`
        are answers *about the symbol* — try the next exchange. Anything else
        is the socket refusing or going quiet, which is a transport problem and
        deserves a fresh connection. Collapsing the two is what makes a
        concurrency limit look like a delisted ticker and thins the board with
        no visible cause.
        """
        charts = []
        for symbol in symbols:
            chart = _rand("cs_")
            charts.append(chart)
            spec = {"symbol": symbol, "adjustment": "splits",
                    "session": SESSION_KIND}
            self._socket.send(encode("chart_create_session", [chart, ""]))
            self._socket.send(encode(
                "resolve_symbol",
                [chart, "sym_1", "=" + json.dumps(spec, separators=(",", ":"))]))
            self._socket.send(encode(
                "create_series",
                [chart, "sds_1", "s1", "sym_1", self._interval, self._bars, ""]))

        collected = {chart: {} for chart in charts}
        reasons = {chart: "timeout" for chart in charts}
        done = set()
        # ⛔ A silence budget, not a per-symbol one. The series run
        # concurrently, so summing a timeout per symbol would let one dead
        # ticker hold a batch of 32 for ten minutes. Any frame at all — for any
        # chart in the batch — proves the socket is working and resets it.
        last_progress = time.time()
        while len(done) < len(charts):
            if time.time() - last_progress > BATCH_IDLE_TIMEOUT:
                break
            try:
                raw = self._socket.recv()
            except websocket.WebSocketTimeoutException:
                continue
            if not raw:
                continue
            last_progress = time.time()
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
                params = message.get("p")
                chart = (params[0] if isinstance(params, list) and params
                         and isinstance(params[0], str) else None)
                method = message.get("m")
                # ⛔ The FIRST answer about a chart is the answer. A wrong
                # exchange sends `symbol_error` and then `series_error` for the
                # same chart — measured live 2026-09-18 — and `series_error`
                # alone means the socket refused, which discards the chunk and
                # reconnects. Letting the follow-up overwrite the reason made
                # one bare ticker take every symbol batched with it down. The
                # sequential drain returned on the first frame and never met
                # this; a batch keeps reading, so it has to ignore the echo.
                if chart in done:
                    continue
                if method in NOT_LISTED_METHODS or method in REFUSAL_METHODS:
                    if chart not in collected:
                        # A refusal naming no chart in this batch is about the
                        # connection, not about any one symbol. Charging it to
                        # a ticker would drop that ticker for the session.
                        raise _SocketRefused(str(method))
                    collected[chart] = {}
                    reasons[chart] = str(method)
                    done.add(chart)
                    continue
                if chart in collected:
                    absorb_bars(message, collected[chart])
                    if method == "series_completed":
                        reasons[chart] = "completed"
                        done.add(chart)

        for chart in charts:
            try:
                self._socket.send(encode("chart_delete_session", [chart]))
            except Exception:  # noqa: BLE001 — the socket is going away anyway
                break
        return [(collected[chart], reasons[chart]) for chart in charts]


def fetch_bars(symbols: Iterable[str], *, interval: str = DEFAULT_INTERVAL,
               bars: int = DEFAULT_BAR_COUNT, workers: int = DEFAULT_WORKERS,
               token: Optional[str] = None,
               batch: Optional[int] = None) -> dict:
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
    size = batch_for(bars) if batch is None else max(1, int(batch))

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
                chunk = queue[cursor[0]:cursor[0] + size]
                cursor[0] += len(chunk)
            for attempt in range(2):
                try:
                    if connection is None:
                        connection = _Connection(token, interval=interval, bars=bars)
                    frames = _resolve_batch(connection, chunk)
                except QuoteAuthError:
                    raise
                except Exception:  # noqa: BLE001 — one dropped socket, not the warm-up
                    if connection is not None:
                        connection.close()
                    connection = None
                    if attempt == 0:
                        continue
                    frames = {}
                break
            if frames:
                with lock:
                    for key, frame in frames.items():
                        if frame is not None and not frame.empty:
                            collected[key] = frame
        if connection is not None:
            connection.close()

    # One worker per chunk at most: spinning up six sockets for forty symbols
    # costs six handshakes to save nothing.
    pool = max(1, min(workers, -(-len(wanted) // size)))
    threads = [threading.Thread(target=worker, daemon=True, name=f"tvbars-{i}")
               for i in range(pool)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return collected


def _resolve_batch(connection: "_Connection", symbols: list) -> dict:
    """Resolve a chunk, advancing each symbol's exchange chain as needed.

    Returns `{caller symbol: frame}` for everything that answered, or raises
    `_SocketRefused` when the socket refused rather than answering about a
    symbol. The caller reconnects on that; treating it as "not listed here"
    instead would charge a transport failure to the ticker and drop it for the
    session.

    ⛔ Each round asks only for the candidates still outstanding. A bare ticker
    listed on NYSE costs one extra resolve round for itself, not a re-run of
    the whole chunk, and its batch mates are already finished by then.
    """
    chains = {symbol: qualified_symbols(symbol) for symbol in symbols}
    step = {symbol: 0 for symbol in symbols if chains[symbol]}
    out: dict = {}
    while step:
        keys = list(step)
        results = connection.series_batch([chains[k][step[k]] for k in keys])
        following = {}
        for key, (rows, reason) in zip(keys, results):
            if rows:
                out[key] = bars_to_frame(rows)
                continue
            if reason not in ("completed",) + NOT_LISTED_METHODS:
                raise _SocketRefused(reason)
            nxt = step[key] + 1
            if nxt < len(chains[key]):
                following[key] = nxt
        step = following
    return out
