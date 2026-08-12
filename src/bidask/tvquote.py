"""TradingView quote websocket — the only source of US equity bid/ask.

`src/bidask/feed.py` talks to the *screener* REST service, which is excellent at
selecting a universe and useless for quotes: the `america` scanner publishes
3,771 fields and **not one of them is bid or ask**. Ask it for `bid` anyway and
it does not error — it returns `null` for every row, which reads exactly like a
missing data entitlement. It is not. Crypto works only because the *crypto*
scanner genuinely does expose `bid`/`ask` in its metainfo.

Quotes for US equities come from the service TradingView's own web and desktop
apps use: a websocket at `data.tradingview.com`, authenticated with a short-
lived JWT that `https://www.tradingview.com/quote_token/` mints from the same
`sessionid` cookie the screener already uses. It serves `lp` (last price), `bid`,
`ask`, sizes, and cumulative `volume` together, per symbol, pushed on change.

Taking last price and volume from here too — rather than from the screener — is
deliberate. The classifier compares a trade price against the quote prevailing
at that trade, so both legs must come from one clock. Mixing a screener `close`
with a socket quote would add a cross-source skew on top of the poll-interval
skew `classify.py` already documents as its dominant error term.

Threading: one background thread owns every socket operation. `sync()` and
`snapshot()` are called from the poll loop and only touch guarded state, so no
frame is ever written by two threads at once.
"""

from __future__ import annotations

import json
import random
import re
import string
import threading
import time
from dataclasses import dataclass, field
from typing import Iterable, Optional

import requests
import websocket

from src.bidask.config import cookie_jar

TOKEN_URL = "https://www.tradingview.com/quote_token/"
SOCKET_URL = "wss://data.tradingview.com/socket.io/websocket?from=screener%2F"
ORIGIN = "https://www.tradingview.com"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Origin": ORIGIN,
    "Referer": "https://www.tradingview.com/",
}

# Fields requested per symbol. `lp`/`volume` ride along with the quote so the
# classifier's price and quote legs share a source — see the module docstring.
QUOTE_FIELDS = (
    "lp", "bid", "ask", "bid_size", "ask_size", "volume",
    "update_mode", "current_session",
)

# One frame per this many symbols. The socket accepts a long symbol list, but a
# bounded batch keeps any single frame small enough to never straddle a write.
SUBSCRIBE_BATCH = 50

# Socket read timeout. Doubles as the subscription-diff tick: the reader applies
# pending add/remove sets whenever a read wakes it, so a universe change lands
# within this long rather than waiting for the next quote to arrive.
READ_TIMEOUT = 1.0

# A quote older than this is not a quote. The socket pushes on change, so a
# healthy but quiet symbol looks identical to a dead connection from the
# consumer's side — this is what separates them.
STALE_AFTER = 30.0

# Reconnect backoff, in seconds. An undocumented endpoint under a personal
# account is not something to reconnect against in a tight loop.
BACKOFF = (1, 2, 5, 10, 30)

_FRAME_HEAD = re.compile(r"~m~(\d+)~m~")
_HEARTBEAT = re.compile(r"^~h~\d+$")


class QuoteAuthError(RuntimeError):
    """The socket could not be authenticated.

    Carried to the UI verbatim, unlike every other failure — the message is
    written here and contains no request detail, so there is no cookie to leak.
    A bare `HTTPError` on the token endpoint is the single most likely thing a
    user will hit, and it says nothing about the cause.
    """


def iter_frames(raw: str) -> Iterable[str]:
    """Split TradingView's `~m~<len>~m~<payload>` framing.

    The declared length is used to slice, rather than matching braces: quote
    payloads nest objects, so a non-greedy `\\{.*?\\}` pattern splits them in the
    wrong place and silently drops the tail of every message.
    """
    pos = 0
    while pos < len(raw):
        head = _FRAME_HEAD.match(raw, pos)
        if not head:
            return
        length = int(head.group(1))
        start = head.end()
        yield raw[start:start + length]
        pos = start + length


def encode(method: str, params: list) -> str:
    payload = json.dumps({"m": method, "p": params}, separators=(",", ":"))
    return f"~m~{len(payload)}~m~{payload}"


@dataclass
class Quote:
    """One symbol's latest pushed values."""

    bid: Optional[float] = None
    ask: Optional[float] = None
    last: Optional[float] = None
    volume: Optional[float] = None
    bid_size: Optional[float] = None
    ask_size: Optional[float] = None
    update_mode: str = ""
    current_session: str = ""
    updated_at: float = field(default_factory=time.time)

    @property
    def two_sided(self) -> bool:
        return isinstance(self.bid, (int, float)) and isinstance(self.ask, (int, float))

    def fresh(self, now: Optional[float] = None) -> bool:
        return ((now or time.time()) - self.updated_at) <= STALE_AFTER


class QuoteStream:
    """Live bid/ask for a changing set of exchange-qualified tickers.

    Ticker strings are the screener's own `EXCHANGE:SYMBOL` form (`NASDAQ:AAPL`),
    not the bare symbol — the socket resolves nothing without the exchange.
    """

    def __init__(self, *, connect_timeout: float = 20.0):
        self._connect_timeout = connect_timeout
        self._lock = threading.Lock()
        self._quotes: dict[str, Quote] = {}
        self._desired: set[str] = set()
        self._subscribed: set[str] = set()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._connected = False
        self._error = ""
        self._last_message_at = 0.0

    # ── lifecycle ────────────────────────────────────────────────

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name="bidask-quotes")
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    # ── poll-loop API ────────────────────────────────────────────

    def sync(self, tickers: Iterable[str]) -> None:
        """Declare the symbol set to track. The reader applies the diff."""
        wanted = {t for t in tickers if isinstance(t, str) and ":" in t}
        with self._lock:
            self._desired = wanted
            # Forget quotes for symbols that left, so a ticker returning later
            # cannot be classified against a price from before its absence.
            for gone in set(self._quotes) - wanted:
                self._quotes.pop(gone, None)

    def snapshot(self) -> dict[str, Quote]:
        """Current quotes, fresh ones only.

        Safe to hold without the lock: `_absorb` publishes a *replacement* Quote
        rather than mutating the stored one, so nothing here is ever written to
        after it is handed out. Returning live mutable objects instead would let
        a reader pick up the bid from one push and the ask from the next and see
        a quote that never existed — a crossed one, most visibly.
        """
        now = time.time()
        with self._lock:
            return {k: v for k, v in self._quotes.items() if v.fresh(now)}

    def status(self) -> dict:
        with self._lock:
            fresh = sum(1 for q in self._quotes.values() if q.fresh() and q.two_sided)
            return {
                "connected": self._connected,
                "error": self._error,
                "subscribed": len(self._subscribed),
                "quoted": fresh,
                "age": round(time.time() - self._last_message_at, 1) if self._last_message_at else None,
            }

    # ── reader thread ────────────────────────────────────────────

    def _run(self) -> None:
        failures = 0
        while not self._stop.is_set():
            try:
                self._session()
                failures = 0
            except Exception as exc:  # noqa: BLE001 — the reader must survive
                with self._lock:
                    self._connected = False
                    # Type name only for transport errors: they can embed the
                    # request URL and its cookie jar, and this string reaches the
                    # UI. QuoteAuthError's text is written in this module, so it
                    # is safe to pass through — and it is the one failure whose
                    # cause the user can actually act on.
                    self._error = (str(exc) if isinstance(exc, QuoteAuthError)
                                   else type(exc).__name__)
                delay = BACKOFF[min(failures, len(BACKOFF) - 1)]
                failures += 1
                self._stop.wait(delay)
        with self._lock:
            self._connected = False

    def _auth_token(self) -> str:
        jar = cookie_jar()
        if not jar:
            raise QuoteAuthError("no TRADINGVIEW_SESSIONID in .env")
        response = requests.get(TOKEN_URL, headers=HEADERS, cookies=jar, timeout=20)
        if response.status_code in (401, 403):
            raise QuoteAuthError("TradingView session cookie rejected — log in again and re-copy it")
        response.raise_for_status()
        token = response.text.strip().strip('"')
        if not token:
            raise QuoteAuthError("TradingView returned an empty quote token")
        return token

    def _session(self) -> None:
        """One connection's lifetime. Returns to trigger a reconnect."""
        # Token first: a missing or rejected cookie should not cost a socket.
        token = self._auth_token()
        socket = websocket.create_connection(
            SOCKET_URL,
            header=[f"{k}: {v}" for k, v in HEADERS.items()],
            origin=ORIGIN,
            timeout=self._connect_timeout,
        )
        try:
            name = "qs_" + "".join(random.choices(string.ascii_lowercase, k=12))
            socket.send(encode("set_auth_token", [token]))
            socket.send(encode("quote_create_session", [name]))
            socket.send(encode("quote_set_fields", [name, *QUOTE_FIELDS]))

            with self._lock:
                self._subscribed = set()
                self._connected = True
                self._error = ""
                self._last_message_at = time.time()

            socket.settimeout(READ_TIMEOUT)
            while not self._stop.is_set():
                self._apply_diff(socket, name)
                try:
                    raw = socket.recv()
                except websocket.WebSocketTimeoutException:
                    continue  # no push this tick; loop back and re-check the diff
                if not raw:
                    continue
                self._consume(socket, raw)
        finally:
            with self._lock:
                self._connected = False
            try:
                socket.close()
            except Exception:  # noqa: BLE001 — already tearing down
                pass

    def _apply_diff(self, socket, name: str) -> None:
        with self._lock:
            add = sorted(self._desired - self._subscribed)
            drop = sorted(self._subscribed - self._desired)
            self._subscribed = set(self._desired)
        for start in range(0, len(add), SUBSCRIBE_BATCH):
            socket.send(encode("quote_add_symbols", [name, *add[start:start + SUBSCRIBE_BATCH]]))
        for start in range(0, len(drop), SUBSCRIBE_BATCH):
            socket.send(encode("quote_remove_symbols", [name, *drop[start:start + SUBSCRIBE_BATCH]]))

    def _consume(self, socket, raw: str) -> None:
        for payload in iter_frames(raw):
            if _HEARTBEAT.match(payload):
                # Echo verbatim. The server drops a connection that stops
                # answering, and the drop looks like a quiet market from here.
                socket.send(f"~m~{len(payload)}~m~{payload}")
                continue
            try:
                message = json.loads(payload)
            except (json.JSONDecodeError, TypeError):
                continue
            if not isinstance(message, dict) or message.get("m") != "qsd":
                continue
            self._absorb(message)

    def _absorb(self, message: dict) -> None:
        params = message.get("p")
        if not isinstance(params, list) or len(params) < 2 or not isinstance(params[1], dict):
            return
        symbol = params[1].get("n")
        values = params[1].get("v")
        if not symbol or not isinstance(values, dict):
            return
        now = time.time()
        with self._lock:
            self._last_message_at = now
            # Copy-on-write. Updates are partial — a push carrying only `lp` must
            # not blank the bid and ask an earlier push established — so start
            # from the previous values, but publish a *new* object so any
            # snapshot already handed to the poll thread stays internally
            # consistent. See `snapshot`.
            previous = self._quotes.get(symbol)
            fields = dict(previous.__dict__) if previous else {}
            for key, attr in (("bid", "bid"), ("ask", "ask"), ("lp", "last"),
                              ("volume", "volume"), ("bid_size", "bid_size"),
                              ("ask_size", "ask_size")):
                value = values.get(key)
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    fields[attr] = float(value)
            for key in ("update_mode", "current_session"):
                value = values.get(key)
                if isinstance(value, str) and value:
                    fields[key] = value
            fields["updated_at"] = now
            self._quotes[symbol] = Quote(**fields)


def merge_quotes(rows, quotes: dict[str, Quote]) -> tuple[list[dict], int]:
    """Overlay socket quotes onto screener rows. Returns (rows, quoted count).

    Rows without a fresh two-sided quote keep their screener values and are left
    for the classifier to reject as `no_quote`. Dropping them here instead would
    hide a broken socket behind a shrinking universe.
    """
    merged, quoted = [], 0
    for row in rows:
        ticker = row.get("ticker")
        quote = quotes.get(ticker) if isinstance(ticker, str) else None
        if quote is None or not quote.two_sided:
            merged.append(row)
            continue
        row = dict(row)
        row["bid"] = quote.bid
        row["ask"] = quote.ask
        # Price and volume come from the quote's own clock, not the screener's.
        if quote.last is not None:
            row["close"] = quote.last
        if quote.volume is not None:
            row["volume"] = quote.volume
        merged.append(row)
        quoted += 1
    return merged, quoted
