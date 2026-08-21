"""Markup pins for the column truncation counter.

The counter spans three files — the span in `index.html`, the render in
`app.js`, the two colours in `style.css` — and nothing on screen says so. These
pin the joins that fail silently.
"""

import re
import unittest
from pathlib import Path

WEB = Path(__file__).resolve().parents[1] / "src" / "bidask" / "web"
HTML = (WEB / "index.html").read_text(encoding="utf-8")
APP = (WEB / "app.js").read_text(encoding="utf-8")
CSS = (WEB / "style.css").read_text(encoding="utf-8")


class TestCounterElementsExist(unittest.TestCase):
    def test_both_column_heads_carry_a_meta_span(self):
        for side in ("strong", "weak"):
            self.assertIn(f'class="column-meta" id="{side}-meta"', HTML,
                          f"{side} column lost its truncation counter")

    def test_app_looks_the_elements_up(self):
        for side in ("strong", "weak"):
            self.assertIn(f"getElementById('{side}-meta')", APP)

    def test_the_payload_key_matches_the_server(self):
        """`build_columns` publishes the counts under `truncated`."""
        self.assertIn("columns.truncated", APP.replace("cols.truncated",
                                                       "columns.truncated"))


class TestWindowPill(unittest.TestCase):
    """The horizon has to be on screen.

    The chips show raw hit counts. Those counts used to run from the session
    open and now cover a trailing window, and nothing else on the page says so —
    a reader who assumes "since open" reads a 30-minute figure as a whole
    session. Same three-file join as the counter above: the span, the render,
    and the server key that feeds it.
    """

    def test_the_status_bar_carries_the_pill(self):
        self.assertIn('id="window-pill"', HTML)

    def test_app_looks_it_up_and_renders_both_states(self):
        self.assertIn("getElementById('window-pill')", APP)
        self.assertIn("hit_window_minutes", APP)
        # A disabled window is a real configuration, not an error state.
        self.assertIn("since open", APP)

    def test_the_server_publishes_the_key_the_client_reads(self):
        server = (Path(__file__).resolve().parents[1] / "src" / "bidask"
                  / "server.py").read_text(encoding="utf-8")
        self.assertIn('"hit_window_minutes": self.cfg.hit_window_minutes', server)


class TestEmptyBranchResetsTheClass(unittest.TestCase):
    def test_the_no_data_branch_clears_the_hiding_state(self):
        """Regression: the tabs share one element.

        Rendering equity (truncated, amber) then crypto (no data) left the
        `hiding` class on an empty label, because the early return set only the
        text. Invisible with no text, wrong the moment `.hiding` gains a border
        or a background.
        """
        # The branch taken when the payload carries no counts, not the
        # `if (!el)` element guard that precedes it.
        branch = re.search(r"if \(!meta \|\| !meta\.groups_total\) \{(.*?)\n    \}",
                           APP, re.S)
        self.assertIsNotNone(branch, "the no-data branch of renderColumnMeta is gone")
        self.assertIn("className = 'column-meta'", branch.group(1),
                      "the empty branch must reset className, not only textContent")


class TestStyling(unittest.TestCase):
    def test_both_states_are_defined(self):
        self.assertIn(".column-meta {", CSS)
        self.assertIn(".column-meta.hiding", CSS)

    def test_the_hiding_state_is_visually_distinct(self):
        """Amber, matching the delayed-feed pill: something needs attention."""
        rule = CSS.split(".column-meta.hiding", 1)[1].split("}", 1)[0]
        self.assertIn("--amber", rule)

    def test_the_head_lays_the_counter_out_beside_the_title(self):
        head = CSS.split(".column-head {", 1)[1].split("}", 1)[0]
        self.assertIn("display: flex", head)
        self.assertIn("justify-content: space-between", head)


if __name__ == "__main__":
    unittest.main()
