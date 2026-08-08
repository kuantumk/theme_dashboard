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
    in_auction = (
        minutes < open_min + cfg.open_auction_minutes
        or minutes >= close_min - cfg.close_auction_minutes
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
        self.consecutive_failures = 0
        self._lock = threading.Lock()

    def poll_once(self) -> bool:
        """One full cycle across every market. Returns False if every feed failed."""
        any_ok = False
        for market in self.markets:
            payload = fetch(market, self.cfg)
            self.errors[market] = payload.error
            self.feeds[market] = payload.feed
            if payload.error or payload.rows.empty:
                continue
            any_ok = True

            rows = build_universe(payload.rows, self.cfg, in_play=(market == "equity"))
            session_date, in_auction = (
                _equity_session_context() if market == "equity" else _crypto_session_context()
            )
            self.accumulators[market].apply(
                rows.to_dict("records"),
                session_date=session_date,
                in_auction_window=in_auction,
            )

        self.consecutive_failures = 0 if any_ok else self.consecutive_failures + 1
        self.write_state()
        return any_ok

    def build_state(self) -> dict:
        state = {"poll_seconds": self.cfg.poll_seconds,
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
                "error": self.errors[market],
                "scanned_at": datetime.now().strftime("%H:%M:%S"),
            }
        return state

    def write_state(self) -> None:
        """Write atomically so a mid-write fetch never sees a truncated file."""
        payload = json.dumps(self.build_state(), separators=(",", ":"))
        tmp = self.out_path.with_suffix(".tmp")
        with self._lock:
            tmp.write_text(payload, encoding="utf-8")
            os.replace(tmp, self.out_path)


class Handler(http.server.SimpleHTTPRequestHandler):
    """Serves the app directory plus exactly one state route."""

    state_path: Path = None  # set by the factory below

    def translate_path(self, path):
        # Everything else resolves inside WEB_DIR because the base class is
        # constructed with directory=WEB_DIR.
        return super().translate_path(path)

    def do_GET(self):  # noqa: N802 — base-class naming
        if self.path.split("?", 1)[0] == STATE_ROUTE:
            self._serve_state()
            return
        super().do_GET()

    def _serve_state(self):
        try:
            body = self.state_path.read_bytes()
        except OSError:
            self.send_error(503, "state not ready")
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        pass  # the poll loop's own output is the useful signal


def make_handler(state_path: Path):
    cls = type("BoundHandler", (Handler,), {"state_path": state_path})
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
            wait = cfg.poll_seconds * (step if not ok else 1)
            if not ok and engine.consecutive_failures:
                print(f"  feed unavailable, backing off {wait}s")
            stop.wait(max(0.0, wait - (time.time() - started)))

    thread = threading.Thread(target=loop, daemon=True, name="bidask-poll")
    thread.start()

    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer(("127.0.0.1", port), make_handler(out_dir / STATE_FILENAME)) as httpd:
        url = f"http://127.0.0.1:{port}/index.html"
        print("=" * 62)
        print("  Bid/Ask Tape Pressure")
        print("=" * 62)
        print(f"  serving : {url}")
        print(f"  state   : {out_dir / STATE_FILENAME}")
        print(f"  cadence : {cfg.poll_seconds}s")
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
