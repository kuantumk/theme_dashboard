"""Chart-socket fetcher tests, driven through a replayed socket.

The warm-up's wall clock is one number: symbols x bars / throughput. Measured
2026-09-18 against the live socket on 60 real screener symbols, 6 connections:

    route                      bars    s/symbol
    one series at a time       2,496   0.106 - 0.131
    8 series per connection    2,496   0.087
    32 series per connection      768  0.040

So the socket serves several series on one connection at once, and the shipped
code was draining them one at a time. Two separate effects are in that table
and the design needs both: batching amortises a fixed per-series cost, and a
smaller bar count cuts the bytes. Neither alone reaches 0.040.

⛔ Throughput is the easy half. COMPLETENESS is what these tests defend. A
symbol the socket drops has no baseline, scores 0, and is absent from the board
with nothing on screen to say why — so a faster fetcher that loses two names
per run is worse than the slow one. Every configuration above returned 60 of
60; the measured ceiling is in `DEFAULT_WORKERS`.

These run against `_FakeSocket`, which replays canned frames. That is weaker
than the live socket and it is what makes the routing, fallback and refusal
paths testable at all — before this file they had no coverage, so a batch that
handed one symbol's bars to another symbol's key would have passed the suite.
"""

import json
import unittest
from unittest import mock

import websocket

from src.bidask import tvbars
from src.bidask.tvbars import (DEFAULT_BAR_COUNT, absorb_bars, bars_to_frame,
                               batch_for, fetch_bars, qualified_symbols)


def frame(payload: str) -> str:
    return f"~m~{len(payload)}~m~{payload}"


def message(method: str, params: list) -> str:
    return frame(json.dumps({"m": method, "p": params}))


def bar_frame(chart: str, stamps) -> str:
    rows = [{"i": i, "v": [s, 1.0, 1.0, 1.0, 1.0, 100.0 + i]}
            for i, s in enumerate(stamps)]
    return frame(json.dumps({"m": "timescale_update",
                             "p": [chart, {"sds_1": {"s": rows}}]}))


class _FakeSocket:
    """Replays frames in response to `create_series`, per chart session.

    `script` maps a resolved symbol to a list of (method, extra) the socket
    answers with. `"bars"` stands for a timescale update carrying three bars.
    """

    def __init__(self, script, *, drop_after=None):
        self.script = script
        self.sent = []
        self.pending = []
        self.charts = {}           # chart id -> resolved symbol
        self.closed = False
        self.drop_after = drop_after

    def settimeout(self, _seconds):
        pass

    def send(self, raw):
        self.sent.append(raw)
        if self.drop_after is not None and len(self.sent) > self.drop_after:
            raise websocket.WebSocketConnectionClosedException("dropped")
        if '"resolve_symbol"' in raw:
            params = json.loads(raw.split("~m~", 2)[2])["p"]
            spec = json.loads(params[2].lstrip("="))
            self.charts[params[0]] = spec["symbol"]
        if '"create_series"' in raw:
            params = json.loads(raw.split("~m~", 2)[2])["p"]
            chart = params[0]
            for step in self.script.get(self.charts.get(chart), []):
                if step == "bars":
                    self.pending.append(bar_frame(chart, [1000, 1300, 1600]))
                else:
                    self.pending.append(message(step, [chart, {}]))

    def recv(self):
        if not self.pending:
            raise websocket.WebSocketTimeoutException("idle")
        return self.pending.pop(0)

    def close(self):
        self.closed = True


def run_fetch(script, symbols, **kwargs):
    sockets = []

    def opener(_token, **_kw):
        sock = _FakeSocket(script)
        sockets.append(sock)
        return sock

    with mock.patch.object(tvbars, "_open_socket", opener), \
         mock.patch.object(tvbars, "auth_token", lambda: "tok"):
        got = fetch_bars(symbols, workers=1, **kwargs)
    return got, sockets


class TestABatchKeepsEverySymbolSeparate(unittest.TestCase):
    """⛔ The failure a batch makes possible: bars landing on the wrong key.

    With one series per connection the association was positional and could not
    be wrong. Draining eight at once, every bar has to be routed by its chart
    session id — and a mix-up is invisible downstream, because the curve still
    looks like a plausible curve. It would simply be another company's.
    """

    def test_each_symbol_receives_only_its_own_bars(self):
        script = {"NASDAQ:AAA": ["bars", "series_completed"],
                  "NASDAQ:BBB": ["bars", "series_completed"]}
        got, sockets = run_fetch(script, ["NASDAQ:AAA", "NASDAQ:BBB"], batch=2)
        self.assertEqual(set(got), {"NASDAQ:AAA", "NASDAQ:BBB"})
        charts = sockets[0].charts
        self.assertEqual(len(set(charts)), 2,
                         "each symbol needs its own chart session")

    def test_one_connection_serves_the_whole_batch(self):
        script = {f"NASDAQ:S{i}": ["bars", "series_completed"] for i in range(8)}
        got, sockets = run_fetch(script, [f"NASDAQ:S{i}" for i in range(8)],
                                 batch=8)
        self.assertEqual(len(got), 8)
        self.assertEqual(len(sockets), 1,
                         "a batch must not open a socket per symbol")

    def test_results_are_keyed_by_the_callers_own_string(self):
        """The profile table keeps one naming convention end to end."""
        script = {"NASDAQ:AAA": ["bars", "series_completed"]}
        got, _ = run_fetch(script, ["aaa"], batch=4)
        self.assertEqual(list(got), ["aaa"])


class TestTheExchangeFallbackSurvivesBatching(unittest.TestCase):
    """A bare ticker still tries NASDAQ, then NYSE, then AMEX.

    ⛔ `symbol_error` is an answer ABOUT THE SYMBOL and arrives in under a
    second; a wrong exchange never sends `series_completed`. Waiting for a
    clean completion instead costs the full 20s timeout on every candidate,
    which across a batch would stall the whole chunk rather than one symbol.
    """

    def test_a_ticker_listed_on_the_second_exchange_still_resolves(self):
        script = {"NASDAQ:GNRC": ["symbol_error"],
                  "NYSE:GNRC": ["bars", "series_completed"]}
        got, _ = run_fetch(script, ["GNRC"], batch=4)
        self.assertIn("GNRC", got)
        self.assertEqual(len(got["GNRC"]), 3)

    def test_the_series_error_that_trails_a_symbol_error_is_not_a_refusal(self):
        """⛔ Measured against the live socket 2026-09-18, and it cost a batch.

        A wrong exchange sends `symbol_error` and then `series_error` for the
        SAME chart. The sequential drain returned on the first and never saw
        the second. A batch keeps reading, so the follow-up overwrote the
        reason — and `series_error` alone means the socket refused, which
        discards the whole chunk and retries it on a fresh connection.

        One bare ticker therefore took every symbol batched with it down. The
        first answer about a chart is the answer; later frames for it are echo.
        """
        script = {"NASDAQ:GNRC": ["symbol_error", "series_error"],
                  "NYSE:GNRC": ["bars", "series_completed"],
                  "NASDAQ:AAA": ["bars", "series_completed"]}
        got, sockets = run_fetch(script, ["GNRC", "NASDAQ:AAA"], batch=2)
        self.assertEqual(set(got), {"GNRC", "NASDAQ:AAA"},
                         "a trailing series_error discarded the batch")
        self.assertEqual(len(sockets), 1,
                         "the connection was thrown away over a listed answer")

    def test_a_ticker_on_no_exchange_is_absent_rather_than_guessed(self):
        script = {f"{ex}:NOPE": ["symbol_error"]
                  for ex in ("NASDAQ", "NYSE", "AMEX")}
        got, _ = run_fetch(script, ["NOPE"], batch=4)
        self.assertEqual(got, {},
                         "callers fail closed on a missing baseline")

    def test_one_unlisted_symbol_does_not_cost_its_batch_mates(self):
        script = {"NASDAQ:AAA": ["bars", "series_completed"],
                  "NASDAQ:NOPE": ["symbol_error"],
                  "NYSE:NOPE": ["symbol_error"],
                  "AMEX:NOPE": ["symbol_error"],
                  "NASDAQ:BBB": ["bars", "series_completed"]}
        got, _ = run_fetch(script, ["NASDAQ:AAA", "NOPE", "NASDAQ:BBB"], batch=3)
        self.assertEqual(set(got), {"NASDAQ:AAA", "NASDAQ:BBB"})


class TestARefusalIsNotChargedToTheSymbol(unittest.TestCase):
    """⛔ A transport failure must not read as a delisted ticker.

    Collapsing the two is what makes a concurrency limit look like a missing
    symbol and thins the board with no visible cause. A refused batch is
    retried on a fresh connection; only `symbol_error` advances the exchange
    chain.
    """

    def test_a_refused_batch_is_retried_on_a_new_connection(self):
        attempts = {"n": 0}

        def opener(_token, **_kw):
            attempts["n"] += 1
            if attempts["n"] == 1:
                return _FakeSocket({"NASDAQ:AAA": ["series_error"]})
            return _FakeSocket({"NASDAQ:AAA": ["bars", "series_completed"]})

        with mock.patch.object(tvbars, "_open_socket", opener), \
             mock.patch.object(tvbars, "auth_token", lambda: "tok"):
            got = fetch_bars(["NASDAQ:AAA"], workers=1, batch=4)
        self.assertIn("NASDAQ:AAA", got)
        self.assertGreaterEqual(attempts["n"], 2)

    def test_a_dropped_socket_does_not_lose_the_rest_of_the_queue(self):
        opened = []

        def opener(_token, **_kw):
            sock = _FakeSocket({f"NASDAQ:S{i}": ["bars", "series_completed"]
                                for i in range(4)},
                               drop_after=2 if not opened else None)
            opened.append(sock)
            return sock

        with mock.patch.object(tvbars, "_open_socket", opener), \
             mock.patch.object(tvbars, "auth_token", lambda: "tok"):
            got = fetch_bars([f"NASDAQ:S{i}" for i in range(4)],
                             workers=1, batch=4)
        self.assertEqual(len(got), 4)


class TestTheBatchSizeFollowsThePayload(unittest.TestCase):
    """Measured: batch 8 at 2,496 bars, batch 32 at 768. Both returned 60/60.

    ⛔ The two do not share one best value. At 2,496 bars a batch of 16 was
    measurably WORSE than 8 (12.16s against 5.98s on 60 symbols) — the account
    saturates near 27k bars/s, so a bigger batch queues rather than
    parallelises. At 768 bars the bytes are no longer the constraint and the
    fixed per-series cost dominates, which only a bigger batch amortises. The
    rule holds bars-in-flight roughly constant instead of picking a number.
    """

    def test_a_full_history_request_uses_the_measured_cold_batch(self):
        self.assertEqual(batch_for(DEFAULT_BAR_COUNT), 8)
        self.assertEqual(batch_for(2496), 8)

    def test_a_small_incremental_request_batches_far_wider(self):
        self.assertGreaterEqual(batch_for(768), 16)

    def test_the_batch_never_collapses_to_nothing(self):
        self.assertGreaterEqual(batch_for(10 ** 9), 1)


class TestPureHelpers(unittest.TestCase):
    """Unchanged behaviour the batch rewrite must not disturb."""

    def test_a_qualified_symbol_skips_the_exchange_chain(self):
        self.assertEqual(qualified_symbols("NASDAQ:AAA"), ["NASDAQ:AAA"])
        self.assertEqual(len(qualified_symbols("AAA")), 3)

    def test_a_resent_forming_bar_replaces_rather_than_duplicates(self):
        from src.bidask.tvsocket import iter_frames
        update = json.loads(next(iter(iter_frames(bar_frame("c", [1000])))))
        collected = {}
        absorb_bars(update, collected)
        absorb_bars(update, collected)
        self.assertEqual(len(collected), 1)

    def test_an_empty_collection_yields_the_expected_columns(self):
        frame_out = bars_to_frame({})
        self.assertEqual(list(frame_out.columns),
                         ["Open", "High", "Low", "Close", "Volume"])


class TestCompletionDeadline(unittest.TestCase):
    def test_heartbeats_cannot_keep_an_unfinished_chart_alive(self):
        tick = [0.0]
        class Heartbeats(_FakeSocket):
            def recv(self):
                tick[0] += 1
                return frame("~h~123")
        sock = Heartbeats({})
        with mock.patch.object(tvbars, "_open_socket", return_value=sock), \
             mock.patch.object(tvbars.time, "monotonic", side_effect=lambda: tick[0]):
            connection = tvbars._Connection("tok", interval="5", bars=100)
            result = connection.series_batch(["NASDAQ:AAA"])
        self.assertEqual(result, [({}, "timeout")])
        self.assertLessEqual(tick[0], tvbars.BATCH_IDLE_TIMEOUT)
        self.assertTrue(any("~h~123" in raw for raw in sock.sent))

    def test_partial_rows_do_not_override_a_timeout(self):
        connection = mock.Mock()
        connection.series_batch.return_value = [({1: [1, 1, 1, 1, 1, 42]}, "timeout")]
        with self.assertRaises(tvbars._SocketRefused):
            tvbars._resolve_batch(connection, ["NASDAQ:AAA"])

    def test_partial_download_is_retried_on_a_fresh_socket(self):
        tick = [0.0]
        opened = []
        class TimedSocket(_FakeSocket):
            def recv(self):
                tick[0] += 1
                return super().recv()
        def opener(_token):
            steps = ["bars"] if not opened else ["bars", "series_completed"]
            sock = TimedSocket({"NASDAQ:AAA": steps})
            opened.append(sock)
            return sock
        with mock.patch.object(tvbars, "_open_socket", side_effect=opener), \
             mock.patch.object(tvbars.time, "monotonic", side_effect=lambda: tick[0]):
            result = fetch_bars(["NASDAQ:AAA"], token="tok", workers=1)
        self.assertEqual(len(opened), 2)
        self.assertTrue(opened[0].closed)
        self.assertEqual(len(result["NASDAQ:AAA"]), 3)


if __name__ == "__main__":
    unittest.main()
