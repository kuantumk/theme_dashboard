"""Quote-websocket tests — framing, partial updates, and the row overlay.

No network: every test drives the pure helpers or pokes `_absorb` directly, so
the suite stays runnable outside market hours.
"""

import time
import unittest

from unittest import mock

from src.bidask.tvquote import (
    Quote,
    QuoteAuthError,
    QuoteStream,
    encode,
    iter_frames,
    merge_quotes,
)


class TestAuth(unittest.TestCase):
    """The credential failure is the one a user can actually act on."""

    def test_missing_cookie_names_the_cause(self):
        with mock.patch("src.bidask.tvquote.cookie_jar", return_value={}):
            with self.assertRaises(QuoteAuthError) as caught:
                QuoteStream()._auth_token()
        self.assertIn("TRADINGVIEW_SESSIONID", str(caught.exception))

    def test_rejected_cookie_is_distinguished_from_a_transport_error(self):
        response = mock.Mock(status_code=403)
        with mock.patch("src.bidask.tvquote.cookie_jar", return_value={"sessionid": "x"}), \
             mock.patch("src.bidask.tvquote.requests.get", return_value=response):
            with self.assertRaises(QuoteAuthError) as caught:
                QuoteStream()._auth_token()
        self.assertIn("rejected", str(caught.exception))

    def run_once(self, stream, error):
        """Drive `_run` through exactly one failed connection attempt."""
        def fail():
            stream._stop.set()   # stop after this pass; the backoff wait returns at once
            raise error
        with mock.patch.object(stream, "_session", side_effect=fail):
            stream._run()

    def test_auth_message_reaches_status(self):
        stream = QuoteStream()
        self.run_once(stream, QuoteAuthError("no TRADINGVIEW_SESSIONID in .env"))
        self.assertEqual(stream.status()["error"], "no TRADINGVIEW_SESSIONID in .env")

    def test_transport_errors_are_reduced_to_a_type_name(self):
        # A transport error can embed the request URL and its cookie jar, and
        # this string is rendered in the browser.
        stream = QuoteStream()
        self.run_once(stream, OSError("https://x/?sessionid=secret"))
        self.assertEqual(stream.status()["error"], "OSError")
        self.assertNotIn("secret", stream.status()["error"])


class TestFraming(unittest.TestCase):
    def test_encode_declares_payload_length(self):
        out = encode("quote_add_symbols", ["qs_1", "NASDAQ:AAPL"])
        body = '{"m":"quote_add_symbols","p":["qs_1","NASDAQ:AAPL"]}'
        self.assertEqual(out, f"~m~{len(body)}~m~{body}")

    def test_iter_frames_splits_consecutive_messages(self):
        raw = encode("a", [1]) + encode("b", [2])
        self.assertEqual(list(iter_frames(raw)),
                         ['{"m":"a","p":[1]}', '{"m":"b","p":[2]}'])

    def test_iter_frames_slices_by_length_not_braces(self):
        # A quote payload nests objects. A non-greedy brace match would stop at
        # the first inner `}` and silently truncate every message.
        body = '{"m":"qsd","p":["qs",{"n":"NASDAQ:AAPL","v":{"bid":1.0,"ask":2.0}}]}'
        [got] = list(iter_frames(f"~m~{len(body)}~m~{body}"))
        self.assertEqual(got, body)

    def test_iter_frames_yields_heartbeats(self):
        self.assertEqual(list(iter_frames("~m~4~m~~h~7")), ["~h~7"])

    def test_iter_frames_stops_on_garbage(self):
        self.assertEqual(list(iter_frames("not a frame")), [])


class TestQuote(unittest.TestCase):
    def test_two_sided_requires_both_legs(self):
        self.assertTrue(Quote(bid=1.0, ask=1.1).two_sided)
        self.assertFalse(Quote(bid=1.0).two_sided)
        self.assertFalse(Quote(ask=1.1).two_sided)
        self.assertFalse(Quote().two_sided)

    def test_staleness_is_measured_from_last_update(self):
        q = Quote(bid=1.0, ask=1.1, updated_at=time.time() - 120)
        self.assertFalse(q.fresh())
        self.assertTrue(Quote(bid=1.0, ask=1.1).fresh())


class TestAbsorb(unittest.TestCase):
    """The socket sends partial updates; merging must not blank prior fields."""

    def message(self, symbol, values):
        return {"m": "qsd", "p": ["qs_x", {"n": symbol, "v": values}]}

    def test_first_push_populates(self):
        s = QuoteStream()
        s._absorb(self.message("NASDAQ:AAPL", {"bid": 300.93, "ask": 300.95,
                                               "lp": 300.94, "volume": 1000}))
        q = s.snapshot()["NASDAQ:AAPL"]
        self.assertEqual((q.bid, q.ask, q.last, q.volume), (300.93, 300.95, 300.94, 1000))

    def test_partial_push_preserves_untouched_fields(self):
        s = QuoteStream()
        s._absorb(self.message("NASDAQ:AAPL", {"bid": 300.93, "ask": 300.95}))
        s._absorb(self.message("NASDAQ:AAPL", {"lp": 301.10}))
        q = s.snapshot()["NASDAQ:AAPL"]
        self.assertEqual((q.bid, q.ask, q.last), (300.93, 300.95, 301.10))

    def test_non_numeric_values_are_ignored(self):
        s = QuoteStream()
        s._absorb(self.message("NASDAQ:AAPL", {"bid": 300.93, "ask": 300.95}))
        s._absorb(self.message("NASDAQ:AAPL", {"bid": None, "ask": "n/a"}))
        q = s.snapshot()["NASDAQ:AAPL"]
        self.assertEqual((q.bid, q.ask), (300.93, 300.95))

    def test_snapshot_is_not_mutated_by_later_pushes(self):
        # The reader thread pushes while the poll thread holds a snapshot. If
        # updates mutated the stored object, a held snapshot could show the bid
        # from one push beside the ask from the next — a quote that never
        # existed, and a crossed one here.
        s = QuoteStream()
        s._absorb(self.message("NASDAQ:AAPL", {"bid": 10.0, "ask": 10.1}))
        held = s.snapshot()["NASDAQ:AAPL"]
        s._absorb(self.message("NASDAQ:AAPL", {"bid": 20.0}))
        s._absorb(self.message("NASDAQ:AAPL", {"ask": 20.1}))
        self.assertEqual((held.bid, held.ask), (10.0, 10.1))
        fresh = s.snapshot()["NASDAQ:AAPL"]
        self.assertEqual((fresh.bid, fresh.ask), (20.0, 20.1))

    def test_booleans_are_not_read_as_prices(self):
        s = QuoteStream()
        s._absorb(self.message("NASDAQ:AAPL", {"bid": True, "ask": 10.1}))
        self.assertIsNone(s.snapshot()["NASDAQ:AAPL"].bid)

    def test_malformed_messages_are_dropped(self):
        s = QuoteStream()
        for bad in ({"m": "qsd", "p": []},
                    {"m": "qsd", "p": ["qs"]},
                    {"m": "qsd", "p": ["qs", {"v": {"bid": 1}}]},
                    {"m": "qsd", "p": ["qs", {"n": "X", "v": "nope"}]}):
            s._absorb(bad)
        self.assertEqual(s.snapshot(), {})

    def test_sync_forgets_symbols_that_left_the_universe(self):
        # A ticker that drops out and returns must not be classified against a
        # price from before its absence.
        s = QuoteStream()
        s._absorb(self.message("NASDAQ:AAPL", {"bid": 1.0, "ask": 1.1}))
        s._absorb(self.message("NYSE:F", {"bid": 13.8, "ask": 13.81}))
        s.sync(["NASDAQ:AAPL"])
        self.assertEqual(sorted(s.snapshot()), ["NASDAQ:AAPL"])

    def test_sync_ignores_bare_symbols(self):
        # The socket resolves nothing without an exchange prefix.
        s = QuoteStream()
        s.sync(["AAPL", "NASDAQ:AAPL", None, 42])
        self.assertEqual(s._desired, {"NASDAQ:AAPL"})


class TestMergeQuotes(unittest.TestCase):
    def rows(self):
        return [
            {"ticker": "NASDAQ:AAPL", "symbol": "AAPL", "close": 299.0,
             "volume": 100, "bid": None, "ask": None, "sector": "Tech"},
            {"ticker": "OTC:TCEHY", "symbol": "TCEHY", "close": 50.0,
             "volume": 20, "bid": None, "ask": None, "sector": "Tech"},
        ]

    def test_quote_overrides_price_and_volume(self):
        # Price and quote must share a clock: the classifier compares them, so a
        # screener `close` against a socket quote adds a second skew term.
        quotes = {"NASDAQ:AAPL": Quote(bid=300.93, ask=300.95, last=300.94, volume=1234)}
        merged, quoted = merge_quotes(self.rows(), quotes)
        self.assertEqual(quoted, 1)
        self.assertEqual(merged[0]["bid"], 300.93)
        self.assertEqual(merged[0]["ask"], 300.95)
        self.assertEqual(merged[0]["close"], 300.94)
        self.assertEqual(merged[0]["volume"], 1234)
        self.assertEqual(merged[0]["sector"], "Tech")  # screener metadata survives

    def test_unquoted_rows_are_kept_not_dropped(self):
        # Dropping them would hide a dead socket behind a shrinking universe;
        # kept, they surface honestly as `no_quote` rejections.
        merged, quoted = merge_quotes(self.rows(), {})
        self.assertEqual(quoted, 0)
        self.assertEqual([r["symbol"] for r in merged], ["AAPL", "TCEHY"])
        self.assertIsNone(merged[0]["bid"])

    def test_one_sided_quote_is_not_merged(self):
        merged, quoted = merge_quotes(self.rows(), {"NASDAQ:AAPL": Quote(bid=300.93)})
        self.assertEqual(quoted, 0)
        self.assertIsNone(merged[0]["bid"])

    def test_source_rows_are_not_mutated(self):
        rows = self.rows()
        merge_quotes(rows, {"NASDAQ:AAPL": Quote(bid=1.0, ask=1.1, last=1.05)})
        self.assertIsNone(rows[0]["bid"])

    def test_missing_ticker_column_is_tolerated(self):
        merged, quoted = merge_quotes([{"symbol": "AAPL", "close": 1.0}],
                                      {"NASDAQ:AAPL": Quote(bid=1.0, ask=1.1)})
        self.assertEqual(quoted, 0)
        self.assertEqual(len(merged), 1)


if __name__ == "__main__":
    unittest.main()
