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
    _equity_session_date,
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


class TestSessionDate(unittest.TestCase):
    """The date that names the day's relative-volume baseline cache.

    It is ET, and it comes from the moment the caller already read. Two separate
    clock reads can straddle midnight and name a cache for a day the elapsed
    figure is not counting from.
    """

    def test_the_date_is_the_ET_trading_date(self):
        from datetime import datetime as dt, timezone as tz

        from src.bidask.server import ET as SERVER_ET

        self.assertEqual(
            _equity_session_date(dt(2026, 8, 10, 9, 45, tzinfo=SERVER_ET)),
            "2026-08-10")
        # 02:30 UTC on the 11th is 22:30 ET on the 10th: still that session.
        self.assertEqual(
            _equity_session_date(dt(2026, 8, 11, 2, 30, tzinfo=tz.utc)),
            "2026-08-10")

    def test_every_hour_of_the_extended_day_is_an_ordinary_poll(self):
        """KTD8: there is no auction window left to reject a poll.

        The window existed because an auction cross has no contemporaneous
        quote for a trade classifier to judge. Nothing here classifies trades,
        so 09:30 and 15:57 are polls like any other.
        """
        from datetime import datetime as dt

        from src.bidask.server import ET as SERVER_ET

        for hour, minute in [(4, 0), (9, 30), (9, 44), (12, 0),
                             (15, 55), (15, 59), (20, 0)]:
            self.assertEqual(
                _equity_session_date(dt(2026, 8, 10, hour, minute, tzinfo=SERVER_ET)),
                "2026-08-10")


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

    def test_the_page_names_a_fully_dropped_universe(self):
        # A 100% drop is upstream breakage, and the page has to say so — the
        # default text blames the reader's own sliders.
        app = (PROJECT_ROOT / "src" / "bidask" / "web" / "app.js").read_text(encoding="utf-8")
        body = app.split("function emptyReason(")[1]
        reason = body.split("function ")[0]
        self.assertIn("view.matched > 0 && !view.universe", reason,
                      "emptyReason no longer names a fully-dropped universe")


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
        eng.profiles["equity"] = {"AAA": [1.0]}
        block = eng.rvol_status("equity")
        self.assertFalse(block["unavailable"])
        self.assertEqual(block["reason"], "")
        self.assertEqual((block["scored"], block["polled"]), (940, 2100))
        self.assertEqual(block["floor"], 1.2)

    def test_partial_nulls_keep_publishing_with_the_coverage_pair(self):
        # A board of the rows that could be judged is worth more than no board;
        # the pair is what says how much of the market it covers.
        eng = self.engine(floor=1.2, scored=3, polled=2100)
        eng.profiles["equity"] = {"AAA": [1.0]}
        block = eng.rvol_status("equity")
        self.assertFalse(block["unavailable"])
        self.assertEqual((block["scored"], block["polled"]), (3, 2100))

    def test_no_usable_reading_anywhere_publishes_an_unavailable_source(self):
        eng = self.engine(floor=1.2, scored=0, polled=2100)
        eng.profiles["equity"] = {"AAA": [1.0]}
        eng.profile_status["equity"] = "ready (0/2100)"
        block = eng.rvol_status("equity")
        self.assertTrue(block["unavailable"])
        self.assertIn("2100", block["reason"])

    def test_a_warm_up_still_running_says_so_rather_than_blaming_the_market(self):
        eng = self.engine(floor=1.2, scored=0, polled=2100)
        eng.profile_status["equity"] = "pending"
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
            eng.profiles["equity"] = {s: profile
                                      for s in ("GAPR", "HEAV", "QUIT")}
            eng.profile_status["equity"] = "ready (3/3)"

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


class TestTheClassifierIsGone(unittest.TestCase):
    """KTD8: one definition of strong and weak, and no accumulation behind it.

    The classifier, the quote socket and the trailing-window accumulator were
    mutually entangled — the socket existed to feed the classifier, the
    accumulator to bound its bias — so a surviving fragment of any of them is a
    second answer to the question the columns already answer.
    """

    def engine(self):
        from src.bidask.config import load_config
        from src.bidask.server import TapeEngine
        with TemporaryDirectory() as tmp:
            return TapeEngine(load_config(), Path(tmp), markets=("equity",))

    def test_the_engine_owns_no_quote_stream_and_no_accumulator(self):
        eng = self.engine()
        for attribute in ("quotes", "quoted", "accumulators"):
            self.assertFalse(hasattr(eng, attribute),
                             f"TapeEngine still carries `{attribute}`")

    def test_the_payload_carries_no_quote_or_counter_blocks(self):
        state = self.engine().build_state()
        self.assertNotIn("hit_window_minutes", state)
        for key in ("quotes", "stats", "ask_side", "bid_side"):
            self.assertNotIn(key, state["equity"],
                             f"the state payload still publishes `{key}`")

    def test_nothing_in_the_repo_imports_the_retired_modules(self):
        # A surviving import is the one failure mode that would keep a second
        # definition of strong and weak alive with nothing on screen to show it.
        #
        # The module PATH is parsed rather than the line searched: a substring
        # scan matches a docstring that happens to open with "from the same
        # row…", and it cannot tell `session` from the `session_state` module
        # that survives.
        import re
        retired = {"classify", "tvquote", "session"}
        statement = re.compile(r"^\s*(?:from|import)\s+([\w.]+)")
        offenders = []
        for folder in ("src", "tests"):
            for path in sorted((PROJECT_ROOT / folder).rglob("*.py")):
                for number, line in enumerate(
                        path.read_text(encoding="utf-8").splitlines(), 1):
                    found = statement.match(line)
                    if found and retired & set(found.group(1).split(".")):
                        offenders.append(f"{path.name}:{number}: {line.strip()}")
        self.assertEqual(offenders, [])

    def test_the_scan_above_would_notice_a_surviving_import(self):
        """A guard that matches nothing is indistinguishable from a clean tree.

        This one is deliberately narrow — it parses the module path — so it is
        worth proving it still fires on the shape it exists to catch.
        """
        import re
        retired = {"classify", "tvquote", "session"}
        statement = re.compile(r"^\s*(?:from|import)\s+([\w.]+)")
        for line, caught in (
                ("from src.bidask.session import SessionAccumulator", True),
                ("    from src.bidask.classify import classify", True),
                ("import src.bidask.tvquote", True),
                ("from src.bidask.session_state import MARKET", False),
                ('    """…from the same per-day row as the payload."""', False)):
            found = statement.match(line)
            hit = bool(found and retired & set(found.group(1).split(".")))
            self.assertEqual(hit, caught, line)


class TestRetiredClassifierConfigKeys(unittest.TestCase):
    """Every retired key raises and names what replaced it.

    Silently ignoring one would leave a tunable in the YAML that the board no
    longer reads — a config that claims a behaviour the code stopped having,
    with no feedback at all for whoever tunes it.
    """

    KEYS = {
        "band_frac": 0.3,
        "max_spread_pct": 2.0,
        "open_auction_minutes": 15,
        "close_auction_minutes": 5,
        "winsor_multiple": 10.0,
        "hit_window_minutes": 30,
        "min_hits_to_show": 3,
    }

    def test_each_key_raises_and_names_itself(self):
        from src.bidask.config import load_config
        for key, value in self.KEYS.items():
            with self.subTest(key=key):
                with self.assertRaises(ValueError) as caught:
                    load_config({key: value})
                self.assertIn(key, str(caught.exception))

    def test_the_shipped_yaml_carries_none_of_them(self):
        # `load_config` reads the shipped block, so a leftover key would raise
        # on every launch — but only at launch, which is a poor place to find out.
        from config.settings import CONFIG
        block = CONFIG.get("bidask") or {}
        self.assertEqual(sorted(set(self.KEYS) & set(block)), [])


class TestCryptoPollReachesTheGate(unittest.TestCase):
    """U8's own trap: the floor was configurable and the poll never applied it.

    `poll_once` used to run the gate for equities only, so `crypto: [[0, 1.2]]`
    sat in the config, passed its unit test, and filtered nothing on the
    running board. A green test beside an unfiltered tab is worse than a
    visible failure, so this class drives the whole poll rather than calling
    the gate.

    ELAPSED is 720 minutes — 12:00 UTC. Pinned so the crypto floor is read at a
    fixed point of its own day whatever hour the suite runs at, and chosen past
    the anchor because no volume is expected at 00:00 UTC and nothing can be
    judged there.
    """

    ELAPSED = 720.0

    def rows(self):
        import numpy as np
        import pandas as pd
        from src.bidask.rvol_at_time import CRYPTO_BARS_PER_DAY, CRYPTO_GRID, baseline_at

        profile = np.cumsum(np.full(CRYPTO_BARS_PER_DAY,
                                    288_000.0 / CRYPTO_BARS_PER_DAY))
        usual = baseline_at(profile, self.ELAPSED, grid=CRYPTO_GRID)

        def row(symbol, ratio, change):
            # `volume` is the UTC-day cumulative — measured, not assumed; see
            # `universe.VOLUME_FIELDS`. `feed_symbol` is the instrument the
            # baseline warm-up would fetch bars for.
            return {"symbol": symbol, "feed_symbol": f"BINANCE:{symbol}USDT.P",
                    "close": 100.0, "avg_volume": None,
                    "volume": ratio * usual, "change_pct": change}

        return profile, pd.DataFrame([
            row("BTC", 2.4, 3.1),      # heavy and up over 24h: strong column
            row("ETH", 1.9, -2.6),     # heavy and down over 24h: weak column
            row("DOGE", 1.1, 14.0),    # 14% on 1.1x its usual: neither column
            row("USDT", 9.0, 0.2),     # stablecoin: excluded upstream
        ])

    def engine_and_state(self, profiles=None):
        from unittest import mock

        from src.bidask.config import load_config
        from src.bidask.feed import Payload
        from src.bidask.server import TapeEngine

        profile, rows = self.rows()
        payload = Payload(rows=rows, feed="streaming", matched=len(rows),
                          market_status="24/7")
        with TemporaryDirectory() as tmp:
            eng = TapeEngine(load_config(), Path(tmp), markets=("crypto",))
            eng.profiles["crypto"] = ({s: profile for s in ("BTC", "ETH", "DOGE")}
                                      if profiles is None else profiles)
            eng.profile_status["crypto"] = "ready (3/3)"
            with mock.patch("src.bidask.server.fetch", return_value=payload), \
                 mock.patch("src.bidask.server.minutes_since_open",
                            return_value=self.ELAPSED), \
                 mock.patch.object(TapeEngine, "ensure_profiles", lambda *a: None):
                eng.poll_once()
        return eng, eng.build_state()["crypto"]

    def shown(self, state):
        return {m["symbol"] for column in ("strong", "weak")
                for group in state["columns"][column] for m in group["members"]}

    def test_a_row_below_the_floor_is_absent_from_the_rendered_board(self):
        """R14 where it counts: through the poll, not through a gate call."""
        _, state = self.engine_and_state()
        self.assertNotIn("DOGE", self.shown(state))

    def test_the_rows_above_the_floor_reach_their_columns(self):
        _, state = self.engine_and_state()
        self.assertEqual(self.shown(state), {"BTC", "ETH"})

    def test_a_fourteen_percent_move_on_a_thin_tape_admits_nothing(self):
        """R16 on the crypto tab. DOGE is the biggest mover in the response."""
        _, state = self.engine_and_state()
        for column in ("strong", "weak"):
            for group in state["columns"][column]:
                self.assertNotIn("DOGE", [m["symbol"] for m in group["members"]])

    def test_each_side_names_the_24_hour_reference(self):
        """R15. The label is what stops it reading as an equity session test."""
        from src.bidask.crypto_state import REF_24H
        _, state = self.engine_and_state()

        def sides_of(column, symbol):
            for group in state["columns"][column]:
                for member in group["members"]:
                    if member["symbol"] == symbol:
                        return member["sides"]
            return None

        self.assertEqual(list(sides_of("strong", "BTC")["strong"]), [REF_24H])
        self.assertEqual(list(sides_of("weak", "ETH")["weak"]), [REF_24H])
        self.assertNotIn("weak", sides_of("strong", "BTC"))

    def test_the_payload_names_the_reference_and_the_anchor(self):
        """The page can only report a cause the payload carries."""
        from src.bidask.crypto_state import REF_24H
        _, state = self.engine_and_state()
        self.assertEqual(state["rvol"]["reference"], REF_24H)
        self.assertEqual(state["rvol"]["anchor"], "00:00 UTC")
        self.assertEqual(state["rvol"]["session_state"], "crypto")
        self.assertEqual(state["rvol"]["floor"], 1.2)

    def test_the_coverage_pair_counts_the_rows_the_gate_saw(self):
        _, state = self.engine_and_state()
        # Three rows after the stablecoin exclusion, all three scorable.
        self.assertEqual((state["rvol"]["scored"], state["rvol"]["polled"]), (3, 3))
        self.assertFalse(state["rvol"]["unavailable"])

    def test_the_stablecoin_never_reaches_the_gate_at_all(self):
        eng, state = self.engine_and_state()
        self.assertNotIn("USDT", self.shown(state))
        self.assertNotIn("USDT", {r["symbol"] for r in eng.pregate["crypto"]})

    def test_an_unavailable_source_says_so_rather_than_showing_a_quiet_market(self):
        """R18. A 24/7 market has no closing bell to explain an empty board."""
        _, state = self.engine_and_state(profiles={})
        self.assertEqual(self.shown(state), set())
        self.assertTrue(state["rvol"]["unavailable"])
        self.assertIn("baselines", state["rvol"]["reason"])

    def test_the_crypto_payload_serializes_with_nan_forbidden(self):
        _, state = self.engine_and_state()
        json.dumps(state, allow_nan=False)


class TestCryptoBaselineSymbols(unittest.TestCase):
    """The warm-up fetches bars for the instrument, not the display symbol.

    MEASURED 2026-09-17: the crypto board's rows are mostly perpetual swaps —
    `BTC` on screen is `BINANCE:BTCUSDT.P` — and the chart socket resolves
    nothing from a bare `BTC`. Getting this backwards leaves every crypto row
    without a baseline, which scores 0 and empties the board.
    """

    def _engine(self, markets=("crypto",)):
        from src.bidask.config import load_config
        from src.bidask.server import TapeEngine
        with TemporaryDirectory() as tmp:
            return TapeEngine(load_config(), Path(tmp), markets=markets)

    def test_the_feed_symbol_is_what_bars_are_requested_for(self):
        import pandas as pd
        rows = pd.DataFrame([{"symbol": "BTC", "feed_symbol": "BINANCE:BTCUSDT.P"},
                             {"symbol": "ETH", "feed_symbol": "BINANCE:ETHUSDT.P"}])
        self.assertEqual(self._engine()._symbol_map(rows),
                         {"BTC": "BINANCE:BTCUSDT.P", "ETH": "BINANCE:ETHUSDT.P"})

    def test_equities_carry_no_feed_symbol_and_fall_back_to_their_own(self):
        import pandas as pd
        rows = pd.DataFrame([{"symbol": "AAPL"}, {"symbol": "NVDA"}])
        self.assertEqual(self._engine(markets=("equity",))._symbol_map(rows),
                         {"AAPL": "AAPL", "NVDA": "NVDA"})

    def test_a_missing_feed_symbol_falls_back_rather_than_asking_for_nan(self):
        import numpy as np
        import pandas as pd
        rows = pd.DataFrame([{"symbol": "BTC", "feed_symbol": np.nan}])
        self.assertEqual(self._engine()._symbol_map(rows), {"BTC": "BTC"})

    def test_the_two_markets_keep_separate_baseline_tables(self):
        eng = self._engine(markets=("crypto", "equity"))
        eng.profiles["crypto"] = {"BTC": [1.0]}
        eng.profiles["equity"] = {"AAPL": [1.0], "NVDA": [1.0]}
        self.assertEqual(eng.rvol_status("crypto")["tickers"], 1)
        self.assertEqual(eng.rvol_status("equity")["tickers"], 2)


class TestCryptoSessionDate(unittest.TestCase):
    """Crypto's day rolls at 00:00 UTC, so its baseline cache is named for it."""

    def test_the_date_is_the_utc_date_not_the_et_one(self):
        from datetime import datetime
        from zoneinfo import ZoneInfo

        from src.bidask.server import _session_date

        # 21:30 ET on the 17th is 01:30 UTC on the 18th: the two markets are
        # on different dates, and a shared cache name would serve one of them
        # yesterday's curves.
        evening = datetime(2026, 9, 17, 21, 30, tzinfo=ZoneInfo("America/New_York"))
        self.assertEqual(_session_date("crypto", evening), "2026-09-18")
        self.assertEqual(_session_date("equity", evening), "2026-09-17")


if __name__ == "__main__":
    unittest.main()
