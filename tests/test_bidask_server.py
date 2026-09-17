"""Server and state-writing tests.

The path-traversal tests are the load-bearing ones: an unpinned document root
would serve `.env`, which holds the TradingView, Alpaca, and IBKR credentials.
"""

import json
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from tempfile import TemporaryDirectory

import socketserver

from config.settings import PROJECT_ROOT
from src.bidask.server import (
    STATE_FILENAME,
    _crypto_session_context,
    _equity_session_context,
    _is_tracked_location,
    make_handler,
)


class TestOutputLocationGuard(unittest.TestCase):
    def test_tracked_directories_are_refused(self):
        self.assertTrue(_is_tracked_location(PROJECT_ROOT / "docs" / "data"))
        self.assertTrue(_is_tracked_location(PROJECT_ROOT / "data"))
        self.assertTrue(_is_tracked_location(PROJECT_ROOT))

    def test_gitignored_local_runs_is_allowed(self):
        self.assertFalse(_is_tracked_location(PROJECT_ROOT / "scripts" / "local_runs"))


class TestSessionContext(unittest.TestCase):
    def test_crypto_never_reports_an_auction_window(self):
        _, in_auction = _crypto_session_context()
        self.assertFalse(in_auction)

    def test_equity_context_returns_a_date_and_flag(self):
        date, in_auction = _equity_session_context()
        self.assertRegex(date, r"^\d{4}-\d{2}-\d{2}$")
        self.assertIsInstance(in_auction, bool)

    def test_auction_window_is_bounded_on_both_sides(self):
        # Regression: an unbounded lower test (`minutes < open+15`) is also true
        # at 04:00 and 08:00, which rejected every extended-hours poll as an
        # "auction" -- 18 hours of the day misdiagnosed.
        from datetime import datetime as dt

        from src.bidask.server import ET as SERVER_ET

        def at(hour, minute):
            moment = dt(2026, 8, 10, hour, minute, tzinfo=SERVER_ET)
            return _equity_session_context(moment)[1]

        for hour, minute in [(0, 30), (4, 0), (8, 0), (9, 29),
                             (12, 0), (16, 30), (20, 0), (23, 0)]:
            self.assertFalse(at(hour, minute),
                             f"{hour:02d}:{minute:02d} ET must not be an auction window")
        for hour, minute in [(9, 30), (9, 44), (15, 55), (15, 59)]:
            self.assertTrue(at(hour, minute),
                            f"{hour:02d}:{minute:02d} ET must be an auction window")


class TestServerRouting(unittest.TestCase):
    """Boot a real server on an ephemeral port and probe it."""

    @classmethod
    def setUpClass(cls):
        cls.tmp = TemporaryDirectory()
        cls.state_path = Path(cls.tmp.name) / STATE_FILENAME
        cls.state_path.write_text(json.dumps({"poll_seconds": 10}), encoding="utf-8")

        socketserver.TCPServer.allow_reuse_address = True
        cls.httpd = socketserver.TCPServer(("127.0.0.1", 0), make_handler(cls.state_path))
        cls.port = cls.httpd.server_address[1]
        cls.thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        cls.httpd.server_close()
        cls.tmp.cleanup()

    def get(self, path):
        url = f"http://127.0.0.1:{self.port}{path}"
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                return resp.status, resp.read()
        except urllib.error.HTTPError as exc:
            return exc.code, b""

    def test_state_route_serves_the_state_file(self):
        status, body = self.get("/state.json")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["poll_seconds"], 10)

    def test_app_index_is_served(self):
        status, body = self.get("/index.html")
        self.assertEqual(status, 200)
        self.assertIn(b"Tape Pressure", body)

    def test_dotenv_is_not_reachable(self):
        status, _ = self.get("/.env")
        self.assertEqual(status, 404)

    def test_parent_traversal_is_not_reachable(self):
        for path in ("/../.env", "/../../.env", "/..%2f.env"):
            status, _ = self.get(path)
            self.assertNotEqual(status, 200, f"{path} must not resolve")

    def test_repo_paths_are_not_reachable(self):
        for path in ("/data/ticker_themes.json", "/config/workflow_config.yaml",
                     "/docs/index.html"):
            status, _ = self.get(path)
            self.assertEqual(status, 404, f"{path} must not resolve")


class TestAllDroppedAlarm(unittest.TestCase):
    """A 100% drop rate is upstream breakage, not a quiet market.

    `Value.Traded` went dark for a full trading session because the screener's
    own match count was computed on every poll and read by nothing. The pair
    (`matched`, `universe`) is what separates "the vendor sent nothing" from
    "our floors removed all 2,806 rows", and that distinction is the diagnosis.
    """

    def engine(self):
        from src.bidask.config import load_config
        from src.bidask.server import TapeEngine
        with TemporaryDirectory() as tmp:
            return TapeEngine(load_config(), Path(tmp), markets=("equity",))

    def test_state_carries_the_match_and_universe_pair(self):
        eng = self.engine()
        eng.matched["equity"], eng.universe["equity"] = 2806, 0
        state = eng.build_state()["equity"]
        self.assertEqual(state["matched"], 2806)
        self.assertEqual(state["universe"], 0)

    def test_the_page_names_the_drop_before_it_blames_the_cookie(self):
        # With no universe nothing is subscribed, so the quote-health branches
        # would accuse the session cookie of a vendor field change. Order is
        # the whole point of this pin; nothing on screen shows it.
        app = (PROJECT_ROOT / "src" / "bidask" / "web" / "app.js").read_text(encoding="utf-8")
        body = app.split("function emptyReason(")[1]
        reason = body.split("function ")[0]
        self.assertIn("view.matched > 0 && !view.universe", reason,
                      "emptyReason no longer names a fully-dropped universe")
        self.assertLess(reason.index("view.matched > 0 && !view.universe"),
                        reason.index("view.quotes"),
                        "the all-dropped test must precede the quote-health tests")


class TestSessionStateFromTheFeed(unittest.TestCase):
    """R1: the state comes from the feed's own field, never from the clock.

    One value describes the whole response. Reading it per row would let two
    tickers in one poll be judged against different references and different
    floors, which no banner on screen would show.
    """

    def frame(self, rows):
        import pandas as pd
        return pd.DataFrame(rows)

    def test_the_feeds_own_spelling_resolves(self):
        from src.bidask.server import _session_state
        from src.bidask.session_state import MARKET, POST_MARKET, PRE_MARKET
        self.assertEqual(_session_state(self.frame([{"current_session": "market"}])),
                         MARKET)
        self.assertEqual(_session_state(self.frame([{"current_session": "pre_market"}])),
                         PRE_MARKET)
        self.assertEqual(_session_state(self.frame([{"current_session": "post_market"}])),
                         POST_MARKET)

    def test_an_absent_or_unmapped_value_is_closed_never_open(self):
        # A state the board has no rules for must not borrow another state's
        # reference price and volume floor.
        from src.bidask.server import _session_state
        from src.bidask.session_state import CLOSED
        self.assertEqual(_session_state(self.frame([{"close": 10.0}])), CLOSED)
        self.assertEqual(_session_state(self.frame([{"current_session": "brand_new"}])),
                         CLOSED)
        self.assertEqual(_session_state(self.frame([{"current_session": None}])), CLOSED)


class TestRvolStatusBlock(unittest.TestCase):
    """KTD2's fail-closed guard, published where the page can read it.

    Relative volume is the only admission path to a column, so an unusable
    source empties the board. An empty board carrying no cause reads as a quiet
    market — the exact ambiguity that hid a broken universe for a full session.
    """

    def engine(self, **gate_fields):
        import pandas as pd
        from src.bidask.config import load_config
        from src.bidask.server import TapeEngine
        from src.bidask.universe import RvolGate
        with TemporaryDirectory() as tmp:
            eng = TapeEngine(load_config(), Path(tmp), markets=("equity",))
        if gate_fields:
            eng.gates["equity"] = RvolGate(rows=pd.DataFrame(), **gate_fields)
        return eng

    def test_a_healthy_poll_names_no_cause(self):
        eng = self.engine(floor=1.2, scored=940, polled=2100)
        eng.profiles = {"AAA": [1.0]}
        block = eng.rvol_status("equity")
        self.assertFalse(block["unavailable"])
        self.assertEqual(block["reason"], "")
        self.assertEqual((block["scored"], block["polled"]), (940, 2100))
        self.assertEqual(block["floor"], 1.2)

    def test_partial_nulls_keep_publishing_with_the_coverage_pair(self):
        # A board of the rows that could be judged is worth more than no board;
        # the pair is what says how much of the market it covers.
        eng = self.engine(floor=1.2, scored=3, polled=2100)
        eng.profiles = {"AAA": [1.0]}
        block = eng.rvol_status("equity")
        self.assertFalse(block["unavailable"])
        self.assertEqual((block["scored"], block["polled"]), (3, 2100))

    def test_no_usable_reading_anywhere_publishes_an_unavailable_source(self):
        eng = self.engine(floor=1.2, scored=0, polled=2100)
        eng.profiles = {"AAA": [1.0]}
        eng.profile_status = "ready (0/2100)"
        block = eng.rvol_status("equity")
        self.assertTrue(block["unavailable"])
        self.assertIn("2100", block["reason"])

    def test_a_warm_up_still_running_says_so_rather_than_blaming_the_market(self):
        eng = self.engine(floor=1.2, scored=0, polled=2100)
        eng.profile_status = "pending"
        block = eng.rvol_status("equity")
        self.assertTrue(block["unavailable"])
        self.assertIn("pending", block["reason"])
        self.assertEqual(block["tickers"], 0)

    def test_a_closed_market_is_not_reported_as_a_broken_source(self):
        # No floor is defined for a closed board, so every row scoring zero is
        # the market being shut. Blaming the vendor would send the next reader
        # after a service that is working.
        eng = self.engine(floor=None, scored=0, polled=2100)
        block = eng.rvol_status("equity")
        self.assertFalse(block["unavailable"])
        self.assertEqual(block["reason"], "")
        self.assertIsNone(block["floor"])

    def test_an_empty_response_is_not_a_broken_source(self):
        eng = self.engine(floor=1.2, scored=0, polled=0)
        block = eng.rvol_status("equity")
        self.assertFalse(block["unavailable"])
        self.assertEqual(block["reason"], "")

    def test_the_block_survives_a_poll_that_never_reached_the_gate(self):
        # A feed error clears the gate, and the payload must still serialize.
        block = self.engine().rvol_status("equity")
        self.assertEqual((block["scored"], block["polled"]), (0, 0))
        self.assertFalse(block["unavailable"])

    def test_the_whole_state_payload_is_json_serializable(self):
        # `write_state` uses allow_nan=False, so one non-finite value costs the
        # whole document rather than one field.
        eng = self.engine(floor=1.2, scored=0, polled=7)
        json.dumps(eng.build_state(), allow_nan=False)


class TestColumnsComeFromThisPoll(unittest.TestCase):
    """The board is a claim about now, built from the gate rather than a counter.

    A failed poll must clear it: last poll's columns standing through an outage
    say the market is doing something it may have stopped doing.
    """

    def engine(self):
        from src.bidask.config import load_config
        from src.bidask.server import TapeEngine
        with TemporaryDirectory() as tmp:
            engine = TapeEngine(load_config(), Path(tmp), markets=("equity",))
        # No taxonomy, so every row takes the industry fallback and these cases
        # do not move whenever a ticker is tagged in `data/ticker_themes.json`.
        engine.themes = {}
        return engine

    def test_qualifying_rows_reach_the_columns_with_their_sides(self):
        eng = self.engine()
        eng.qualified["equity"] = [
            {"symbol": "AAA", "rvol_at_time": 3.0, "industry": "Software",
             "sides": {"strong": ("open",), "weak": ("prev close",)}},
        ]
        eng.pregate["equity"] = [{"symbol": "AAA", "industry": "Software"},
                                 {"symbol": "BBB", "industry": "Software"}]
        columns = eng.build_state()["equity"]["columns"]
        # A gapped-down recovering name is genuinely strong against its open
        # and weak against yesterday, and belongs in both columns.
        self.assertEqual(columns["strong"][0]["members"][0]["symbol"], "AAA")
        self.assertEqual(columns["weak"][0]["members"][0]["symbol"], "AAA")

    def test_the_breadth_denominator_is_the_pre_gate_universe(self):
        # Two members of the industry roster, one qualifying: the share is 1/2,
        # not 1/1. Taken after the gate every share would be 1.0.
        eng = self.engine()
        eng.qualified["equity"] = [
            {"symbol": "AAA", "rvol_at_time": 2.0, "industry": "Software",
             "sides": {"strong": ("open",)}},
            {"symbol": "BBB", "rvol_at_time": 2.0, "industry": "Software",
             "sides": {"strong": ("open",)}},
        ]
        eng.pregate["equity"] = [{"symbol": s, "industry": "Software"}
                                 for s in ("AAA", "BBB", "CCC", "DDD")]
        half = eng.build_state()["equity"]["columns"]["strong"][0]["score"]

        eng.pregate["equity"] = eng.pregate["equity"][:2]
        full = eng.build_state()["equity"]["columns"]["strong"][0]["score"]
        self.assertLess(half, full)

    def test_an_empty_poll_renders_no_columns(self):
        eng = self.engine()
        columns = eng.build_state()["equity"]["columns"]
        self.assertEqual(columns["strong"], [])
        self.assertEqual(columns["weak"], [])


class TestPollOnceEndToEnd(unittest.TestCase):
    """One poll from a canned feed response all the way to the payload.

    The unit tests above each cover one joint. This covers the joins between
    them — the gate's output shape, the side dict the columns read, the
    breadth denominator, and the coverage pair — which is where a redesign
    this size actually breaks, and which no single-module test can see.
    """

    ELAPSED = 390.0   # 10:30 ET: 60 minutes into the regular session, floor 1.2

    def rows(self):
        import numpy as np
        import pandas as pd
        from src.bidask.rvol_at_time import BARS_PER_SESSION, baseline_at

        profile = np.cumsum(np.full(BARS_PER_SESSION, 192_000.0 / BARS_PER_SESSION))
        usual = baseline_at(profile, self.ELAPSED)

        def row(symbol, ratio, *, from_open, on_day):
            return {"symbol": symbol, "ticker": f"NASDAQ:{symbol}",
                    "close": 20.0, "avg_volume": 1_000_000,
                    "industry": "Software", "current_session": "market",
                    "premarket_volume": 6_000.0,
                    "volume": ratio * usual - 6_000.0,
                    "postmarket_volume": 0.0,
                    "change_from_open": from_open, "change": on_day}

        return profile, pd.DataFrame([
            # Gapped down and bid back up: strong against its open, weak
            # against yesterday, and it belongs in both columns.
            row("GAPR", 2.0, from_open=3.1, on_day=-4.2),
            row("HEAV", 1.8, from_open=2.0, on_day=2.5),
            # Running hard on a tape nobody is trading. R16: excluded.
            row("QUIT", 0.4, from_open=12.0, on_day=11.0),
        ])

    def engine_and_state(self):
        from unittest import mock

        from src.bidask.config import load_config
        from src.bidask.feed import Payload
        from src.bidask.server import TapeEngine

        profile, rows = self.rows()
        payload = Payload(rows=rows, feed="streaming", matched=len(rows),
                          market_status="market open")
        with TemporaryDirectory() as tmp:
            eng = TapeEngine(load_config(), Path(tmp), markets=("equity",))
            eng.themes = {}       # industry fallback, independent of the tag file
            eng.quotes = None     # the quote socket is the next unit's to delete
            eng.profiles = {s: profile for s in ("GAPR", "HEAV", "QUIT")}
            eng.profile_status = f"ready ({len(eng.profiles)}/3)"

            # The clock is pinned so the floor is 1.2 whatever hour the suite
            # runs at, and the warm-up is stubbed so no socket opens.
            with mock.patch("src.bidask.server.fetch", return_value=payload), \
                 mock.patch("src.bidask.server.minutes_since_open",
                            return_value=self.ELAPSED), \
                 mock.patch.object(TapeEngine, "ensure_profiles", lambda *a: None):
                eng.poll_once()
        return eng, eng.build_state()["equity"]

    def test_the_gate_admits_only_the_unusually_traded_rows(self):
        _, state = self.engine_and_state()
        shown = {m["symbol"] for column in ("strong", "weak")
                 for group in state["columns"][column] for m in group["members"]}
        self.assertEqual(shown, {"GAPR", "HEAV"})

    def test_the_thin_runner_is_absent_from_both_columns(self):
        """AE2 through the whole stack: 12% on 0.4x reaches neither column."""
        _, state = self.engine_and_state()
        for column in ("strong", "weak"):
            for group in state["columns"][column]:
                self.assertNotIn("QUIT", [m["symbol"] for m in group["members"]])

    def test_a_gapped_down_recovering_name_occupies_both_columns(self):
        _, state = self.engine_and_state()

        def side(column):
            for group in state["columns"][column]:
                for member in group["members"]:
                    if member["symbol"] == "GAPR":
                        return member["sides"]
            return None

        # R5: each side names the reference that placed it there, because with
        # two references live the column alone cannot say which comparison won.
        self.assertEqual(list(side("strong")["strong"]), ["open"])
        self.assertEqual(list(side("weak")["weak"]), ["prev close"])

    def test_the_coverage_pair_counts_every_row_the_gate_saw(self):
        _, state = self.engine_and_state()
        # Three rows polled, all three scorable; only two cleared the floor.
        self.assertEqual((state["rvol"]["scored"], state["rvol"]["polled"]), (3, 3))
        self.assertEqual(state["rvol"]["floor"], 1.2)
        self.assertFalse(state["rvol"]["unavailable"])
        self.assertEqual(state["rvol"]["session_state"], "market")

    def test_the_payload_serializes_with_nan_forbidden(self):
        _, state = self.engine_and_state()
        json.dumps(state, allow_nan=False)


if __name__ == "__main__":
    unittest.main()
