"""Poll loop and local server for the bid/ask tape dashboard.

Runs entirely locally: a background thread polls the screener and rewrites a
state file; a loopback HTTP server serves the static app plus that one file.

The document root is pinned to this package's `web/` directory. It is never the
repository, which holds `.env` — a root-relative server would hand out the
TradingView and Alpaca credentials to anything that could reach the port.
"""

from __future__ import annotations

import argparse
import functools
import http.server
import json
import math
import os
import socketserver
import subprocess
import sys
import threading
import time
import webbrowser
from datetime import datetime
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

from config.settings import PROJECT_ROOT
from src.bidask.config import load_config
from src.bidask.crypto_state import REF_24H, crypto_sides
from src.bidask.feed import fetch
from src.bidask.grouping import SIDES_FIELD, build_columns, load_themes
from src.bidask.session_state import CLOSED, resolve_state, sides_for
from src.bidask.universe import apply_rvol_gate, build_universe
from src.bidask.rvol_at_time import (
    CRYPTO,
    CRYPTO_GRID,
    EQUITY_GRID,
    build_for_symbols,
    load_profiles,
    minutes_since_open,
    prune_cache,
    save_profiles,
)

ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")
WEB_DIR = Path(__file__).resolve().parent / "web"
STATE_ROUTE = "/state.json"
CADENCE_ROUTE = "/cadence"
STATE_FILENAME = "bidask_state.json"
DEFAULT_OUT_DIR = "scripts/local_runs"

MARKETS = ("crypto", "equity")

# Which clock each market's relative volume runs on. The equity board counts
# from 04:00 ET; crypto counts from 00:00 UTC, because that is where the
# vendor's own `volume` column counts from — measured, see `universe.py`.
MARKET_GRIDS = {"equity": EQUITY_GRID, "crypto": CRYPTO_GRID}

# Backoff schedule after consecutive feed failures, in multiples of the poll
# interval. Retrying at cadence against an undocumented endpoint is how an
# account gets rate-limited.
BACKOFF_STEPS = (1, 2, 4, 8, 15)


def _is_tracked_location(path: Path) -> bool:
    """True when `path` sits in the repo and git would not ignore it.

    Uses git's own knowledge rather than a hardcoded directory list, so a
    changed `.gitignore` cannot silently open a hole.
    """
    try:
        path.relative_to(PROJECT_ROOT)
    except ValueError:
        return False  # outside the repo entirely
    probe = path / STATE_FILENAME
    try:
        result = subprocess.run(
            ["git", "check-ignore", "-q", str(probe)],
            cwd=PROJECT_ROOT, capture_output=True, timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return True  # cannot prove it is ignored, so treat it as tracked
    return result.returncode != 0


def _equity_session_date(now: datetime) -> str:
    """The ET trading date the caller's clock read falls in.

    Takes the moment rather than reading the clock, so the session date and the
    elapsed-minutes figure derive from one reading. Two separate reads can
    straddle midnight and name a baseline cache for a day the elapsed figure is
    not counting from.

    There is no auction window here any more. It existed because an auction
    print is one large cross with no meaningful contemporaneous quote, so a
    trade classifier could not judge it. A price against a session reference has
    no such problem, and `bidask.open_auction_minutes` /
    `bidask.close_auction_minutes` now raise from `load_config`.
    """
    return now.astimezone(ET).strftime("%Y-%m-%d")


def _crypto_session_date(now: datetime) -> str:
    """The UTC date the caller's clock read falls in.

    Crypto's day rolls at 00:00 UTC — the anchor its `volume` column counts
    from — so its baseline cache is named for the UTC date, not the ET one.
    Naming it for the ET date would keep yesterday's curves for the four or
    five hours those two dates disagree, which is every evening the board runs.
    """
    return now.astimezone(UTC).strftime("%Y-%m-%d")


def _session_date(market: str, now: datetime) -> str:
    return (_crypto_session_date(now) if market == "crypto"
            else _equity_session_date(now))


def _drop_non_finite(record: dict) -> None:
    """Replace NaN and infinity in a member payload with None, in place.

    ⛔ Load-bearing, and its absence takes out the WHOLE board rather than one
    field. `write_state` serializes with `allow_nan=False`, so a single
    non-finite cell raises and no state file is written at all — both tabs then
    freeze at the last good poll while the console scrolls one line per cycle.
    It reads as a dead feed, which is the one thing this app is built not to
    misreport.

    It is not a rare edge either: the extended-hours columns are genuinely
    absent for any name that did not trade in that window, measured 2026-09-17
    at 322 of 1,821 rows in the `premarket_*` trio and 4 in `postmarket_*` on
    one live poll. Every equity poll carries some.

    The retired `session.py` scrubbed these on the way into its display meta.
    That module was deleted once the columns stopped flowing through it, but
    the raw screener record now reaches the payload directly instead — the
    guard's caller moved rather than going away, which is exactly the shape a
    caller sweep misses.
    """
    for key, value in record.items():
        if isinstance(value, float) and not math.isfinite(value):
            record[key] = None


def _session_state(rows) -> str:
    """The session state this response describes, from the feed's own field.

    The local clock is never consulted: a holiday, an early close and a feed
    outage all look like an ordinary afternoon from here. Extraction mirrors
    `feed._market_status` — one value describes the whole response, so reading
    it per row would let two tickers in one poll be judged against different
    references and different floors.
    """
    if "current_session" not in getattr(rows, "columns", ()):
        return CLOSED
    values = rows["current_session"].dropna().unique().tolist()
    return resolve_state(values[0]) if values else CLOSED


class TapeEngine:
    """Polls each market and rewrites the state file from that poll alone.

    Nothing here remembers an earlier poll's tape. The board is a claim about
    now: every ticker's side and score are recomputed from this poll's price and
    relative volume, so there is no counter to age out and no accumulated bias
    to bound. The one piece of carried state is the relative-volume baseline,
    which depends only on completed sessions and is rebuilt once a day.
    """

    def __init__(self, cfg, out_dir: Path, markets=MARKETS):
        self.cfg = cfg
        self.out_path = out_dir / STATE_FILENAME
        self.markets = markets
        self.themes = load_themes()
        self.errors = {m: "" for m in markets}
        self.feeds = {m: "" for m in markets}
        self.market_status = {m: "" for m in markets}
        # Rows the screener matched server-side, and rows that survived our own
        # floors. Published as a pair because the ratio is the diagnosis: a
        # healthy `matched` beside a zero `universe` is an upstream field
        # change, and it is indistinguishable from a quiet market without both.
        self.matched = {m: 0 for m in markets}
        self.universe = {m: 0 for m in markets}
        # One poll's two frames, as row dicts. `qualified` cleared the
        # relative-volume gate and carries the side or sides each row earned;
        # `pregate` is the liquidity-filtered universe before it, which is
        # `build_columns`'s breadth denominator. Both are rewritten every poll
        # and cleared before a failed one — the board is a claim about now, and
        # last poll's columns standing through an outage would say the market
        # is doing something it may have stopped doing.
        self.qualified = {m: [] for m in markets}
        self.pregate = {m: [] for m in markets}
        self.gates = {m: None for m in markets}
        self.session_states = {m: CLOSED for m in markets}
        # Latched so the alarm prints on the transition rather than 360 times
        # an hour. A line per poll is noise, and noise is not a signal.
        self._all_dropped = {m: False for m in markets}
        self.consecutive_failures = 0
        # Mutable so the in-app control can retune cadence without a restart.
        # cfg stays frozen; this is the live value the loop reads each cycle.
        self.poll_seconds = cfg.poll_seconds
        # Set when cadence changes, so the poll loop can abandon a wait it is
        # already sitting in rather than finishing the old interval first.
        self.wake = threading.Event()
        self._lock = threading.Lock()
        # Relative Volume at Time baselines: one cumulative-volume curve per
        # ticker, from its own recent sessions. Built once per session in the
        # background because it depends only on completed sessions, then read
        # by every poll. Empty until the warm-up lands, which fails the volume
        # leg closed rather than admitting everything.
        #
        # Per market, because the two run on different clocks and roll on
        # different dates. One shared table would have each warm-up replace the
        # other's curves every poll, and a crypto curve read against the equity
        # grid is wrong by four or five hours with nothing on screen to show it.
        self.profiles: dict = {m: {} for m in markets}
        self.profile_dates: dict = {m: None for m in markets}
        self.profile_status = {m: "pending" for m in markets}
        self._profile_threads: dict = {m: None for m in markets}
        # Retry state for a warm-up that failed. `profile_dates` records
        # success and the thread handle records in-flight, so without these a
        # failure is the one outcome nothing remembers — and the next poll,
        # ten seconds later, starts the whole download again.
        self._profile_failures: dict = {m: 0 for m in markets}
        self._profile_retry_at: dict = {m: 0.0 for m in markets}

    def ensure_profiles(self, market: str, session_date: str, rows) -> None:
        """Start this market's baseline warm-up once, off the poll thread.

        The bars come from the TradingView chart socket (`src/bidask/tvbars.py`)
        — the only source here that carries real extended-hours volume, where
        yfinance returns those same bars with zero volume on every one. It is
        also the crypto source: measured 2026-09-17, 60 of 60 BINANCE symbols
        resolved there, including the `.P` perpetuals the board mostly polls.
        The download would stall the poll loop if it ran inline, so it runs in
        a background thread and caches to disk.

        It runs once per session because the baseline depends only on completed
        sessions. A same-day restart reuses the cache; a cache from another
        session is discarded rather than reused, because a stale baseline is
        silently wrong for every ticker rather than visibly absent for all.

        ⛔ Bars are requested for the row's own `feed_symbol` and the result is
        re-keyed to its `symbol`. On the crypto tab those differ — the board
        shows `BTC` while the instrument is `BINANCE:BTCUSDT.P` — and the chart
        socket resolves nothing from the display name.
        """
        if self.profile_dates.get(market) == session_date:
            return
        thread = self._profile_threads.get(market)
        if thread is not None and thread.is_alive():
            return
        # ⛔ A failed build must not relaunch on the next poll. `profile_dates`
        # is set only on success, so without this the warm-up restarts every
        # ~10 seconds — a ~1,900-symbol websocket download, against a vendor
        # that just refused us, forever. The backoff reuses the feed's own
        # schedule so one idea governs both retry paths.
        if time.time() < self._profile_retry_at.get(market, 0.0):
            return
        fetch_by_display = self._symbol_map(rows)
        if not fetch_by_display:
            return
        grid = MARKET_GRIDS.get(market, EQUITY_GRID)

        def defer() -> None:
            """Back off this market's warm-up after a failed attempt."""
            self._profile_failures[market] = self._profile_failures.get(market, 0) + 1
            step = BACKOFF_STEPS[min(self._profile_failures[market],
                                     len(BACKOFF_STEPS) - 1)]
            self._profile_retry_at[market] = time.time() + self.poll_seconds * step

        def warm() -> None:
            cached = load_profiles(self.out_path.parent, session_date,
                                   market=market, grid=grid)
            if cached:
                self.profiles[market] = cached
                self.profile_dates[market] = session_date
                self.profile_status[market] = f"cached ({len(cached)})"
                print(f"  rvol baselines [{market}]: reused {len(cached)} "
                      "from today's cache")
                return
            print(f"  rvol baselines [{market}]: building for "
                  f"{len(fetch_by_display)} tickers…")
            started = time.time()
            try:
                built = build_for_symbols(list(fetch_by_display.values()),
                                          sessions=self.cfg.in_play_rvol_sessions,
                                          grid=grid)
            except Exception as exc:  # noqa: BLE001 — the tape must keep running
                self.profile_status[market] = f"failed ({type(exc).__name__})"
                defer()
                # There is no second admission path: relative volume is the
                # gate. A failed build therefore empties the board, and the
                # payload says so rather than letting it read as a quiet
                # market.
                print(f"  rvol baselines [{market}]: build failed "
                      f"({type(exc).__name__}); the board stays empty until it"
                      " succeeds")
                return
            # Back to the names the gate looks up. A symbol whose bars never
            # arrived is simply absent, which scores 0 and excludes the row.
            keyed = {display: built[fetch] for display, fetch
                     in fetch_by_display.items() if fetch in built}
            # ⛔ Resolving NOTHING is a failure, not a finished warm-up. Every
            # name requested here came from rows the screener returned in this
            # same poll, so a build that keys zero of them did not find a quiet
            # market — it failed to reach the vendor. Latching `profile_dates`
            # on that empties the board for the rest of the session with a
            # status pill reading `ready (0/1900)`, which is the quiet-market
            # misreport this whole design exists to prevent.
            if not keyed:
                self.profile_status[market] = f"failed (0/{len(fetch_by_display)} resolved)"
                defer()
                print(f"  rvol baselines [{market}]: resolved 0 of "
                      f"{len(fetch_by_display)}; treating as a failure, not a"
                      " finished warm-up")
                return
            self.profiles[market] = keyed
            self.profile_dates[market] = session_date
            self._profile_failures[market] = 0
            self._profile_retry_at[market] = 0.0
            self.profile_status[market] = f"ready ({len(keyed)}/{len(fetch_by_display)})"
            save_profiles(keyed, self.out_path.parent, session_date,
                          market=market, grid=grid)
            prune_cache(self.out_path.parent, session_date, market=market)
            print(f"  rvol baselines [{market}]: {len(keyed)}/"
                  f"{len(fetch_by_display)} ready in {time.time() - started:.0f}s")

        thread = threading.Thread(target=warm, daemon=True,
                                  name=f"bidask-rvol-warmup-{market}")
        self._profile_threads[market] = thread
        thread.start()

    @staticmethod
    def _symbol_map(rows) -> dict:
        """`{display symbol: symbol to fetch bars for}` from one poll's rows.

        `feed_symbol` is the exchange-qualified instrument the vendor names;
        equities have none and fall back to the display symbol, which
        `tvbars.qualified_symbols` then resolves across the usual exchanges.
        """
        if "symbol" not in getattr(rows, "columns", ()):
            return {}
        display = [str(s) for s in rows["symbol"].tolist()]
        if "feed_symbol" not in rows.columns:
            return {name: name for name in display}
        feed = [str(s) for s in rows["feed_symbol"].tolist()]
        return {name: (source if source and source not in ("nan", "None") else name)
                for name, source in zip(display, feed)}

    def set_poll_seconds(self, seconds) -> int:
        """Retune cadence at runtime, bounded by config. Returns the value set."""
        self.poll_seconds = self.cfg.clamp_poll_seconds(seconds)
        # Cut short a wait already in progress, so a change from 60s to 3s takes
        # effect now rather than up to a minute later.
        self.wake.set()
        return self.poll_seconds

    def poll_once(self) -> bool:
        """One full cycle across every market. Returns False if every feed failed."""
        any_ok = False
        for market in self.markets:
            payload = fetch(market, self.cfg)
            self.errors[market] = payload.error
            self.feeds[market] = payload.feed
            self.market_status[market] = payload.market_status
            self.matched[market] = payload.matched
            self.universe[market] = len(payload.rows)
            # A 100% drop rate is an upstream-breakage signal, not a quiet
            # market — the same alarm the EP scan grew after Finviz mangled its
            # tickers. `Value.Traded` went dark for a whole session precisely
            # because this ratio was computed every poll and never read.
            dropped = bool(payload.matched) and payload.rows.empty and not payload.error
            if dropped != self._all_dropped[market]:
                self._all_dropped[market] = dropped
                if dropped:
                    print(f"  WARNING {market}: screener matched {payload.matched} rows "
                          "and every one failed the local floors — an upstream field "
                          "change is the usual cause, not a quiet market")
                else:
                    print(f"  {market}: universe recovered ({len(payload.rows)} rows)")
            # Reset before the early exit. The columns are a claim about right
            # now, so last poll's rows must not stand through a feed outage and
            # say the market is doing something it may have stopped doing.
            self.qualified[market] = []
            self.pregate[market] = []
            self.gates[market] = None
            self.session_states[market] = CLOSED
            if payload.error:
                continue
            # A feed reading proves the response arrived, so the poll succeeded
            # even when no row survived our own floors. Charging the backoff
            # counter for that throttles 10s to 150s and prints "feed
            # unavailable" about a vendor that is streaming — the same
            # misdirection this module was just fixed to stop.
            if payload.feed or not payload.rows.empty:
                any_ok = True
            if payload.rows.empty:
                continue

            # One clock read per market, on that market's own grid, so the
            # session date and the elapsed-minutes figure cannot disagree.
            # ⛔ Both markets take this path. Crypto used to skip it, so the
            # flat 1.2 floor was expressible in config and did nothing on the
            # running board — a green unit test beside an unfiltered tab.
            grid = MARKET_GRIDS.get(market, EQUITY_GRID)
            now = datetime.now(tz=grid.tz)
            elapsed = minutes_since_open(now, grid=grid)

            # Liquidity first, and its result is kept: it is the gate's input
            # AND `build_columns`'s breadth denominator. Taken after the gate
            # instead, every group's qualifying share would be 1.0 and the
            # breadth term would rank nothing.
            universe = build_universe(payload.rows, self.cfg, market=market)
            self.pregate[market] = universe.to_dict("records")

            # Warm baselines for the liquidity-filtered set, not the raw
            # response. A row below the dollar-volume floor can never reach a
            # column, so a baseline for it is a websocket round trip spent on a
            # curve nothing will read — measured 2,797 matched against 2,179
            # surviving on one live poll, so roughly a fifth of the warm-up.
            # The trade-off is named: a borderline name that crosses the floor
            # later in the session now has no baseline, scores 0 and stays off
            # the board. That is the same fail-closed rule an unknown reading
            # already gets, and the alternative is paying for every row the
            # floor rejects on every session.
            self.ensure_profiles(market, _session_date(market, now), universe)

            # Crypto is not a session state — it has no open and no close — so
            # it carries the `crypto` gate key and its own 24-hour price
            # reference (`src/bidask/crypto_state.py`) rather than borrowing
            # either from the equity table.
            if market == "crypto":
                state = CRYPTO
            else:
                state = _session_state(payload.rows)
            self.session_states[market] = state

            gate = apply_rvol_gate(universe, self.cfg, state=state,
                                   profiles=self.profiles.get(market, {}),
                                   elapsed_minutes=elapsed)
            self.gates[market] = gate
            records = gate.rows.to_dict("records")

            # Strong and weak are two independent tests, never a branch. A
            # stock above today's open and below yesterday's close is
            # genuinely being accumulated against one reference and
            # distributed against the other, and both readings belong on
            # the board. Only a side actually earned reaches the payload:
            # membership downstream is an `in` test, so an empty tuple
            # under a live key would place a row in a column with no
            # reference to name it.
            for record in records:
                sides = (crypto_sides(record) if market == "crypto"
                         else sides_for(record, state))
                record[SIDES_FIELD] = {
                    name: references
                    for name, references in (("strong", sides.strong),
                                             ("weak", sides.weak))
                    if references
                }
                _drop_non_finite(record)
            self.qualified[market] = records

        self.consecutive_failures = 0 if any_ok else self.consecutive_failures + 1
        # A write failure must not touch consecutive_failures: that counter
        # throttles the feed, and a busy state file says nothing about the feed.
        self.write_state()
        return any_ok

    def build_state(self) -> dict:
        state = {"poll_seconds": self.poll_seconds,
                 "min_poll_seconds": self.cfg.min_poll_seconds,
                 "max_poll_seconds": self.cfg.max_poll_seconds,
                 "generated_at": datetime.now().strftime("%H:%M:%S")}
        for market in self.markets:
            # The rows that cleared this poll's gate, ranked against the
            # pre-gate universe. Stateless: nothing here remembers an earlier
            # poll, so the columns describe the market now rather than the
            # session so far.
            columns = build_columns(
                self.qualified[market],
                self.themes,
                self.cfg,
                grouped=(market != "crypto"),
                universe=self.pregate[market],
            )
            feed = self.feeds[market]
            state[market] = {
                "columns": columns,
                "feed": feed,
                "delayed": (not feed) or feed.startswith("delayed"),
                # Distinct from `feed`: a real-time entitlement on a closed
                # market is still a closed market.
                "market_status": self.market_status[market],
                # The pair the empty column needs to name its own cause.
                "matched": self.matched[market],
                "universe": self.universe[market],
                "error": self.errors[market],
                "scanned_at": datetime.now().strftime("%H:%M:%S"),
            }
            # The board's only statement of why a column is empty. Relative
            # volume is the sole admission path on BOTH tabs, so this block is
            # what separates a broken source from a quiet market — and the
            # crypto tab needs it at least as much, since a 24/7 market has no
            # closing bell to explain an empty column.
            state[market]["rvol"] = self.rvol_status(market)
        return state

    def rvol_status(self, market: str) -> dict:
        """What the relative-volume gate could and could not judge this poll.

        Relative volume is the only admission path to either column, so when it
        cannot be computed the board is empty — and an empty board that says
        nothing about why is indistinguishable from a quiet market. That
        ambiguity is the failure this whole redesign was written around, so the
        guard lives here rather than in the page: a browser can only report a
        cause the payload carries.

        `scored` against `polled` is the pair the page renders. Partial nulls
        keep publishing — a board of the rows that could be judged is worth
        more than no board — and the pair is what says how much of the market
        that board covers.
        """
        gate = self.gates.get(market)
        status = self.profile_status.get(market, "pending")
        return {
            # The warm-up's own state, so a thin board during the download
            # reads as a warm-up in progress rather than as a dull morning.
            "status": status,
            "tickers": len(self.profiles.get(market, {})),
            "session_state": self.session_states.get(market, CLOSED),
            # R15: what the sides were measured against, so the crypto column
            # is not read as an equity session measure. The equity states name
            # their own references per row; crypto has exactly one.
            "reference": REF_24H if market == "crypto" else "",
            # Which clock the ratio runs on, named rather than implied. The two
            # tabs share a number whose anchor differs by five hours.
            "anchor": "00:00 UTC" if market == "crypto" else "04:00 ET",
            "floor": None if gate is None else gate.floor,
            "scored": 0 if gate is None else gate.scored,
            "polled": 0 if gate is None else gate.polled,
            "unavailable": bool(gate is not None and gate.source_unavailable),
            "reason": self._rvol_reason(market, gate),
        }

    def _rvol_reason(self, market: str, gate) -> str:
        """Name the cause when the source failed, else "".

        Empty on a healthy poll, on an empty response, and on a closed market:
        none of those is a broken source, and claiming one would send the next
        reader after a vendor that is working. `RvolGate.source_unavailable`
        makes those three distinctions; this only has to name what is left.
        """
        if gate is None or not gate.source_unavailable:
            return ""
        status = self.profile_status.get(market, "pending")
        if not self.profiles.get(market):
            return f"relative-volume baselines unavailable ({status})"
        return (f"relative volume unusable for all {gate.polled} rows polled "
                f"(baselines: {status})")

    def write_state(self) -> bool:
        """Write atomically so a mid-write fetch never sees a truncated file.

        `allow_nan=False` is load-bearing: the default emits a bare `NaN` token,
        which is valid Python but invalid JSON, and the browser would reject the
        whole document. Failing here is far better than shipping a payload the
        page cannot parse.

        On Windows `os.replace` raises PermissionError when the destination is
        open by another handle — which is exactly what the HTTP thread does when
        it serves the file on the same cadence the writer uses. Retry briefly
        rather than treating a read collision as a write failure.
        """
        try:
            payload = json.dumps(self.build_state(), separators=(",", ":"), allow_nan=False)
        except ValueError as exc:
            print(f"  state serialization failed (non-finite value): {exc}")
            return False

        tmp = self.out_path.with_suffix(".tmp")
        with self._lock:
            tmp.write_text(payload, encoding="utf-8")
            for attempt in range(6):
                try:
                    os.replace(tmp, self.out_path)
                    return True
                except PermissionError:
                    time.sleep(0.05 * (attempt + 1))
            print("  state file busy; skipped this write")
            tmp.unlink(missing_ok=True)
            return False


class Handler(http.server.SimpleHTTPRequestHandler):
    """Serves the app directory plus exactly one state route."""

    state_path: Path = None  # set by the factory below
    engine = None            # set by the factory below

    def translate_path(self, path):
        # Everything else resolves inside WEB_DIR because the base class is
        # constructed with directory=WEB_DIR.
        return super().translate_path(path)

    def end_headers(self):
        """Forbid caching of the app's own assets.

        SimpleHTTPRequestHandler sends only Last-Modified on an HTTP/1.0
        response, so browsers apply heuristic caching and will serve a stale
        app.js or index.html without revalidating — an edit to the app then
        appears not to have taken effect at all. This is a local single-user
        tool refetching every few seconds; there is nothing to gain from caching.
        """
        self.send_header("Cache-Control", "no-store, must-revalidate")
        super().end_headers()

    def do_GET(self):  # noqa: N802 — base-class naming
        if self.path.split("?", 1)[0] == STATE_ROUTE:
            self._serve_state()
            return
        super().do_GET()

    def do_POST(self):  # noqa: N802 — base-class naming
        """Cadence retune. POST rather than GET so a browser prefetch or a
        speculative connection can never silently change the poll rate."""
        if self.path.split("?", 1)[0] != CADENCE_ROUTE or self.engine is None:
            self.send_error(404)
            return
        try:
            length = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(length) or b"{}")
        except (ValueError, OSError):
            self.send_error(400, "bad request")
            return
        applied = self.engine.set_poll_seconds(body.get("seconds"))
        # Rewrite immediately. The state file still holds the cadence from the
        # last poll, and the client treats that file as authoritative — without
        # this, its next refresh reads the stale value and resets the cadence
        # the user just chose.
        self.engine.write_state()
        payload = json.dumps({"poll_seconds": applied}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _serve_state(self):
        try:
            body = self.state_path.read_bytes()
        except OSError:
            self.send_error(503, "state not ready")
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()  # adds Cache-Control: no-store
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        pass  # the poll loop's own output is the useful signal


def make_handler(state_path: Path, engine=None):
    cls = type("BoundHandler", (Handler,), {"state_path": state_path, "engine": engine})
    return functools.partial(cls, directory=str(WEB_DIR))


def run(out_dir: Path, port: int, poll_seconds: Optional[int], open_browser: bool = True) -> int:
    cfg = load_config({"poll_seconds": poll_seconds} if poll_seconds else None)

    out_dir = out_dir.resolve()
    if _is_tracked_location(out_dir):
        print(f"refusing to write into a git-tracked location: {out_dir}")
        print("point --out-dir at an ignored directory (default: scripts/local_runs)")
        return 1
    out_dir.mkdir(parents=True, exist_ok=True)

    engine = TapeEngine(cfg, out_dir)
    engine.write_state()  # so the page has something to fetch immediately

    stop = threading.Event()

    def loop():
        while not stop.is_set():
            started = time.time()
            try:
                ok = engine.poll_once()
            except Exception as exc:  # noqa: BLE001 — the loop must survive
                print(f"  poll error: {type(exc).__name__}")
                engine.consecutive_failures += 1
                ok = False
            step = BACKOFF_STEPS[min(engine.consecutive_failures, len(BACKOFF_STEPS) - 1)]
            # Read the live value, not the frozen config: the in-app control
            # retunes cadence between cycles without a restart.
            wait = engine.poll_seconds * (step if not ok else 1)
            if not ok and engine.consecutive_failures:
                print(f"  feed unavailable, backing off {wait}s")
            # Wait on `wake` rather than `stop` so a cadence change interrupts
            # the interval instead of being deferred to the end of it.
            engine.wake.clear()
            deadline = time.time() + max(0.0, wait - (time.time() - started))
            while not stop.is_set() and time.time() < deadline:
                if engine.wake.wait(min(1.0, deadline - time.time())):
                    break  # cadence retuned; start the next cycle now

    thread = threading.Thread(target=loop, daemon=True, name="bidask-poll")
    thread.start()

    # Threaded: a single-threaded server serialises every connection, so one
    # stalled peer (Chrome's speculative preconnect opens sockets without
    # sending a request) would freeze the dashboard while the poll loop kept
    # writing state normally — a frozen UI with a healthy backend.
    class _Server(socketserver.ThreadingMixIn, http.server.HTTPServer):
        daemon_threads = True
        # NOT reusable on Windows: there SO_REUSEADDR lets a second process bind
        # an address another process is already listening on, with connections
        # routed indeterminately between them — a second launch would silently
        # shadow the first and keep serving stale files. Fail loudly instead.
        allow_reuse_address = os.name != "nt"

    handler = make_handler(out_dir / STATE_FILENAME, engine)
    try:
        server = _Server(("127.0.0.1", port), handler)
    except OSError as exc:
        stop.set()
        print(f"\n  cannot bind 127.0.0.1:{port}: {exc}")
        print("  a dashboard is probably already running on that port —")
        print("  close its window, or relaunch with a different --port.")
        return 1

    with server as httpd:
        url = f"http://127.0.0.1:{port}/index.html"
        print("=" * 62)
        print("  Bid/Ask Tape Pressure")
        print("=" * 62)
        print(f"  serving : {url}")
        print(f"  state   : {out_dir / STATE_FILENAME}")
        print(f"  cadence : {engine.poll_seconds}s "
              f"(adjustable in-app, {cfg.min_poll_seconds}-{cfg.max_poll_seconds}s)")
        print("  Ctrl-C to stop")
        if open_browser:
            threading.Timer(1.0, lambda: webbrowser.open(url)).start()
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\n  stopping…")
        finally:
            stop.set()
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Bid/ask tape pressure dashboard")
    ap.add_argument("--out-dir", default=DEFAULT_OUT_DIR)
    ap.add_argument("--port", type=int, default=8787)
    ap.add_argument("--poll-seconds", type=int, default=None)
    ap.add_argument("--no-browser", action="store_true")
    args = ap.parse_args()
    return run(
        out_dir=Path(args.out_dir),
        port=args.port,
        poll_seconds=args.poll_seconds,
        open_browser=not args.no_browser,
    )


if __name__ == "__main__":
    sys.exit(main())
