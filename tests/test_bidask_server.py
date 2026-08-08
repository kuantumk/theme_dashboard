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


if __name__ == "__main__":
    unittest.main()
