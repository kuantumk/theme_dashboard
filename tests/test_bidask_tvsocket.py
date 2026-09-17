"""TradingView socket auth and framing.

What survives of the retired quote stream: the token mint and the `~m~` frame
codec, both shared with `src/bidask/tvbars.py`. No network — every test drives a
pure helper or a mocked request, so the suite runs outside market hours.
"""

import unittest
from unittest import mock

from src.bidask.tvsocket import (
    QuoteAuthError,
    auth_token,
    encode,
    iter_frames,
)


class TestAuth(unittest.TestCase):
    """The credential failure is the one a user can actually act on."""

    def test_missing_cookie_names_the_cause(self):
        with mock.patch("src.bidask.tvsocket.cookie_jar", return_value={}):
            with self.assertRaises(QuoteAuthError) as caught:
                auth_token()
        self.assertIn("TRADINGVIEW_SESSIONID", str(caught.exception))

    def test_rejected_cookie_is_distinguished_from_a_transport_error(self):
        response = mock.Mock(status_code=403)
        with mock.patch("src.bidask.tvsocket.cookie_jar", return_value={"sessionid": "x"}), \
             mock.patch("src.bidask.tvsocket.requests.get", return_value=response):
            with self.assertRaises(QuoteAuthError) as caught:
                auth_token()
        self.assertIn("rejected", str(caught.exception))

    def test_an_empty_token_is_refused(self):
        # A 200 carrying nothing would otherwise open a socket that authenticates
        # with an empty string and fails much further downstream.
        response = mock.Mock(status_code=200, text='""')
        response.raise_for_status = mock.Mock()
        with mock.patch("src.bidask.tvsocket.cookie_jar", return_value={"sessionid": "x"}), \
             mock.patch("src.bidask.tvsocket.requests.get", return_value=response):
            with self.assertRaises(QuoteAuthError) as caught:
                auth_token()
        self.assertIn("empty", str(caught.exception))


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
        # Chart and quote payloads both nest objects. A non-greedy brace match
        # would stop at the first inner `}` and silently truncate every message.
        body = '{"m":"qsd","p":["qs",{"n":"NASDAQ:AAPL","v":{"bid":1.0,"ask":2.0}}]}'
        [got] = list(iter_frames(f"~m~{len(body)}~m~{body}"))
        self.assertEqual(got, body)

    def test_iter_frames_yields_heartbeats(self):
        self.assertEqual(list(iter_frames("~m~4~m~~h~7")), ["~h~7"])

    def test_iter_frames_stops_on_garbage(self):
        self.assertEqual(list(iter_frames("not a frame")), [])


class TestOneCopyOfTheCodec(unittest.TestCase):
    """`tvbars` imports the codec; it never restates it.

    Two `~m~<len>~m~` parsers would drift, and the copy would lose the reason
    this one slices by declared length rather than matching braces.
    """

    def test_tvbars_imports_the_shared_helpers(self):
        from pathlib import Path
        source = (Path(__file__).resolve().parents[1] / "src" / "bidask"
                  / "tvbars.py").read_text(encoding="utf-8")
        self.assertIn("from src.bidask.tvsocket import", source)
        self.assertNotIn("_FRAME_HEAD", source)


if __name__ == "__main__":
    unittest.main()
