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
import os
import socketserver
import subprocess
import sys
import threading
import time
import webbrowser
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

from config.settings import PROJECT_ROOT
from src.bidask.config import load_config
from src.bidask.feed import fetch
from src.bidask.grouping import build_columns, load_themes
from src.bidask.session import SessionAccumulator
from src.bidask.universe import build_universe

ET = ZoneInfo("America/New_York")
WEB_DIR = Path(__file__).resolve().parent / "web"
STATE_ROUTE = "/state.json"
CADENCE_ROUTE = "/cadence"
STATE_FILENAME = "bidask_state.json"
DEFAULT_OUT_DIR = "scripts/local_runs"

MARKETS = ("crypto", "equity")

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


def _equity_session_context(now: Optional[datetime] = None) -> tuple[str, bool]:
    """Return (session date, whether we are inside an auction window).

    Auction prints are single large crosses with no meaningful contemporaneous
    continuous quote, so classification is meaningless there.
    """
    cfg = load_config()
    now_et = (now or datetime.now(tz=ET)).astimezone(ET)
    open_min = 9 * 60 + 30
    close_min = 16 * 60
    minutes = now_et.hour * 60 + now_et.minute
    # Both windows are bounded. An unbounded lower test (`minutes < open+15`)
    # is also true at 04:00 and 08:00, which would reject every pre-market and
    # after-hours poll as an "auction" — 18 hours of the day misdiagnosed.
    in_auction = (
        (open_min <= minutes < open_min + cfg.open_auction_minutes)
        or (close_min - cfg.close_auction_minutes <= minutes < close_min)
    )
    return now_et.strftime("%Y-%m-%d"), in_auction


def _crypto_session_context() -> tuple[str, bool]:
    """Crypto trades continuously: UTC day boundary, never an auction window."""
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%d"), False


class TapeEngine:
    """Owns one accumulator per market and rewrites the state file each poll."""

    def __init__(self, cfg, out_dir: Path, markets=MARKETS):
        self.cfg = cfg
        self.out_path = out_dir / STATE_FILENAME
        self.markets = markets
        self.themes = load_themes()
        self.accumulators = {m: SessionAccumulator(cfg, m) for m in markets}
        self.errors = {m: "" for m in markets}
        self.feeds = {m: "" for m in markets}
        self.market_status = {m: "" for m in markets}
        self.consecutive_failures = 0
        # Mutable so the in-app control can retune cadence without a restart.
        # cfg stays frozen; this is the live value the loop reads each cycle.
        self.poll_seconds = cfg.poll_seconds
        # Set when cadence changes, so the poll loop can abandon a wait it is
        # already sitting in rather than finishing the old interval first.
        self.wake = threading.Event()
        self._lock = threading.Lock()

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
            if payload.error or payload.rows.empty:
                continue
            any_ok = True

            rows = build_universe(
                payload.rows, self.cfg,
                in_play=(market == "equity"), market=market,
            )
            session_date, in_auction = (
                _equity_session_context() if market == "equity" else _crypto_session_context()
            )
            self.accumulators[market].apply(
                rows.to_dict("records"),
                session_date=session_date,
                in_auction_window=in_auction,
            )

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
            acc = self.accumulators[market]
            columns = build_columns(
                acc.active(self.cfg.min_hits_to_show),
                self.themes,
                self.cfg,
                grouped=(market != "crypto"),
            )
            ask_side = sum(s.ask_hits for s in acc.states.values())
            bid_side = sum(s.bid_hits for s in acc.states.values())
            feed = self.feeds[market]
            state[market] = {
                "columns": columns,
                "ask_side": ask_side,
                "bid_side": bid_side,
                "stats": acc.snapshot_stats(),
                "feed": feed,
                "delayed": (not feed) or feed.startswith("delayed"),
                # Distinct from `feed`: a real-time entitlement on a closed
                # market is still a closed market.
                "market_status": self.market_status[market],
                "error": self.errors[market],
                "scanned_at": datetime.now().strftime("%H:%M:%S"),
            }
        return state

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
