"""Markup pins for the tape board's rendered surfaces.

Every join here spans two or three files — a span in `index.html`, a render in
`app.js`, a rule in `style.css` — and every one of them fails silently. A pill
whose element was renamed simply stops updating; a label whose field left the
payload renders `undefined` or nothing at all.

These pins cannot catch an inverted sort or an unreadable label. The plan's
Verification Contract covers those with a live board; this file covers the
joins, the retired surfaces, and the two constants the browser had to copy from
the Python because the payload does not carry them.
"""

import re
import unittest
from pathlib import Path

from src.bidask.crypto_state import CHANGE_FIELD, CRYPTO, REF_24H
from src.bidask.grouping import DEFAULT_RVOL_CAP, RVOL_FIELD, SIDES_FIELD, TOP_MEMBERS
from src.bidask.session_state import REFERENCE_FIELDS

WEB = Path(__file__).resolve().parents[1] / "src" / "bidask" / "web"
HTML = (WEB / "index.html").read_text(encoding="utf-8")
APP = (WEB / "app.js").read_text(encoding="utf-8")
CSS = (WEB / "style.css").read_text(encoding="utf-8")

# What actually reaches a reader: the markup plus the strings the script writes
# into it. Comments are stripped because a comment recording why a surface was
# removed is the opposite of that surface surviving, and a prose check that
# cannot tell the two apart would punish the note that stops the next reader
# rebuilding it. `app.js` carries no `//` inside a string literal, which is what
# makes this one-line strip safe.
APP_COPY = re.sub(r"//.*", "", APP)


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


class TestRetiredSurfacesAreGone(unittest.TestCase):
    """Nothing on screen may read a counter the stateless board cannot produce.

    Each of these failed OPEN rather than loudly. The pressure bar froze at a
    50/50 split, which reads as balanced flow rather than as absent data; the
    sliders and checkboxes compared `undefined` and passed every row, so they
    sat on screen responding to nothing.
    """

    RETIRED_FIELDS = ("total_hits", "ask_hits", "bid_hits", "imbalance",
                      "uncertain", "divergent", "margin", "ask_side", "bid_side",
                      "hit_window_minutes")

    def test_no_retired_member_field_is_read(self):
        for name in self.RETIRED_FIELDS:
            self.assertNotIn(name, APP, f"app.js still reads the retired `{name}`")

    def test_no_retired_control_or_pill_survives(self):
        for element in ("min-hits", "hide-uncertain", "hide-divergent",
                        "window-pill", "polls-pill", "quotes-pill",
                        "pressure-fill"):
            self.assertNotIn(element, HTML, f"index.html still carries #{element}")
            self.assertNotIn(element, APP, f"app.js still looks up #{element}")

    def test_the_pressure_bar_markup_and_styles_are_gone(self):
        self.assertNotIn('class="pressure', HTML)
        self.assertNotIn(".pressure-track", CSS)
        self.assertNotIn(".pressure-fill", CSS)

    def test_the_divergence_marker_is_gone(self):
        """It marked count-vs-volume disagreement; neither signal exists."""
        self.assertNotIn(".chip.divergent", CSS)
        self.assertNotIn(".chip .warn", CSS)


class TestRetiredCopyIsGone(unittest.TestCase):
    """R19 applies to the board's own words before it applies to any file.

    The headings, the footnote and the truncation tooltip all asserted trade-side
    classification and a summed-margin score — in the one place a user reads.
    """

    PROSE = HTML + APP_COPY

    def test_no_surface_describes_trade_side_classification(self):
        for phrase in ("offers being lifted", "bids being hit", "Hit counts",
                       "hit counts", "bid/ask", "ask-side", "bid-side",
                       "classified"):
            self.assertNotIn(phrase, self.PROSE,
                             f"retired mechanism still described: {phrase!r}")

    def test_the_truncation_tooltip_no_longer_claims_a_summed_score(self):
        self.assertNotIn("SUM of member margins", APP)
        self.assertIn("MEAN", APP, "the tooltip must state the score it actually ranks by")

    def test_the_column_heads_name_the_price_and_volume_test(self):
        for word in ("above its reference", "below its reference", "unusual volume"):
            self.assertIn(word, HTML, f"the column heads no longer say {word!r}")


class TestChipsShowRelativeVolumeAndMove(unittest.TestCase):
    def test_the_chip_reads_the_ranking_field(self):
        self.assertIn(f"'{RVOL_FIELD}'", APP,
                      "the chip must read the field the server ranks on")

    def test_the_chip_renders_both_figures(self):
        self.assertIn('class="rvol"', APP)
        self.assertIn('class="move"', APP)
        for selector in (".chip .rvol", ".chip .move"):
            self.assertIn(selector, CSS, f"{selector} has no style")


class TestReferenceMarkerIsATextLabel(unittest.TestCase):
    """R5 and AE1: the marker NAMES the reference, so it cannot be a tint.

    A gapped-down recovering stock appears in both columns. The column supplies
    the colour on both sides, so only the label distinguishes "above today's
    open" from "below yesterday's close".
    """

    def test_the_marker_reads_the_sides_field(self):
        self.assertIn(f"'{SIDES_FIELD}'", APP)
        self.assertIn("referenceMarks", APP)

    def test_the_marker_renders_the_reference_name_as_text(self):
        self.assertIn('class="ref"', APP)
        self.assertIn("mark.reference", APP)

    def test_the_marker_style_carries_no_background(self):
        rule = CSS.split(".chip .ref {", 1)[1].split("}", 1)[0]
        self.assertNotIn("background", rule,
                         "the R5 marker is a label, not a tint")

    def test_every_reference_the_server_emits_has_a_change_field_here(self):
        """The browser copies `REFERENCE_FIELDS`; a state added there and not
        here renders a bare label with no move beside it."""
        for state, pairs in REFERENCE_FIELDS.items():
            for field, reference in pairs:
                self.assertIn(f"'{reference}': '{field}'", APP,
                              f"{state}: no change field mapped for {reference!r}")

    def test_the_crypto_reference_is_mapped_too(self):
        self.assertIn(f"'{REF_24H}': '{CHANGE_FIELD}'", APP)
        self.assertIn(f"{CRYPTO}: {{", APP,
                      "the crypto session state is missing from the move table")


class TestScoreConstantsMatchThePython(unittest.TestCase):
    """The payload carries neither, so the browser holds a copy.

    Without the cap the client sort is decided by the tail the server caps away:
    one measured row read 2090.8x, three orders of magnitude above every rival.
    """

    def test_the_cap_matches(self):
        match = re.search(r"const RVOL_CAP = ([\d.]+);", APP)
        self.assertIsNotNone(match, "app.js lost its relative-volume cap")
        self.assertAlmostEqual(float(match.group(1)), DEFAULT_RVOL_CAP)

    def test_the_leader_count_matches(self):
        match = re.search(r"const TOP_MEMBERS = (\d+);", APP)
        self.assertIsNotNone(match, "app.js lost its top-member count")
        self.assertEqual(int(match.group(1)), TOP_MEMBERS)

    def test_the_client_caps_before_it_means(self):
        """Capping the mean instead lets one extreme member carry two quiet
        ones to the ceiling — a different operation, not a simplification."""
        body = APP.split("function intensity(", 1)[1].split("\n  }", 1)[0]
        self.assertIn("Math.min(memberValue(m), RVOL_CAP)", body)

    def test_both_columns_sort_descending(self):
        """The score is never negative now, so an ascending weak column would
        lead with the least active theme. A markup pin cannot prove the order on
        screen; it can prove no `side === 'strong'` ternary decides it."""
        sort = re.search(r"kept\.sort\((.*?)\);", APP, re.S)
        self.assertIsNotNone(sort, "renderColumn no longer re-sorts after the sliders")
        self.assertIn("b.score - a.score", sort.group(1))
        self.assertNotIn("a.score - b.score", sort.group(1))
        self.assertNotIn("side ===", sort.group(1))


class TestHonestyPills(unittest.TestCase):
    """R18: the board names its own cause, every poll rather than only when a
    column empties. Partial nulls are routine, and a board that quietly shrank
    to a third of the market must not read as a calm one."""

    def test_the_coverage_pill_exists_and_reads_the_rvol_pair(self):
        self.assertIn('id="coverage-pill"', HTML)
        self.assertIn("getElementById('coverage-pill')", APP)
        for key in ("r.scored", "r.polled"):
            self.assertIn(key, APP, f"the coverage pill does not read {key}")

    def test_the_floor_pill_names_the_floor_and_the_state(self):
        self.assertIn('id="floor-pill"', HTML)
        self.assertIn("getElementById('floor-pill')", APP)
        self.assertIn("r.floor", APP)
        self.assertIn("r.session_state", APP)

    def test_the_reference_pill_labels_the_crypto_measure(self):
        """R15: unlabelled, the 24-hour reading reads as the equity session
        measure taken at a strange hour."""
        self.assertIn('id="reference-pill"', HTML)
        self.assertIn("getElementById('reference-pill')", APP)
        self.assertIn("r.reference", APP)
        self.assertIn("r.anchor", APP)

    def test_empty_reason_separates_the_four_causes(self):
        reason = APP.split("function emptyReason(", 1)[1].split("\n  }", 1)[0]
        self.assertIn("view.error", reason, "a failed fetch has no branch")
        self.assertIn("r.unavailable", reason, "an unavailable source has no branch")
        self.assertIn("'pending'", reason, "a warm-up in progress has no branch")
        self.assertIn("r.polled", reason, "a quiet market has no branch")

    def test_the_quiet_market_branch_says_the_source_is_working(self):
        """AE5's other half: a quiet market and a dead source must not read the
        same. The quiet branch quotes the coverage pair to prove which it is."""
        reason = APP.split("function emptyReason(", 1)[1].split("\n  }", 1)[0]
        tail = reason.rsplit("return", 1)[1]
        self.assertIn("r.scored", tail)
        self.assertIn("r.polled", tail)


if __name__ == "__main__":
    unittest.main()
