"""Guards for the Market Breadth & Sentiment tiles in docs/index.html.

Two tiles here carry a weekly survey date rather than a daily figure -- AAII
and NAAIM -- and both render more than a bare number, so both have their own
element ids. A missing id is the failure mode worth guarding: `loadBreadthData`
writes into whatever `getElementById` returns and a miss is `null`, so the tile
silently keeps its placeholder em dashes with no console error and no visual
cue that anything broke. Nothing on screen distinguishes that from a week the
survey did not publish -- which for NAAIM is exactly how the previous tile sat
frozen at 79.70% for weeks before it was retired.

The repo has no JavaScript test tooling, so a Python parser test is the only
automated guard available for these files.
"""

import html as html_module
import re
import unittest
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
INDEX_HTML = _ROOT / "docs" / "index.html"
APP_JS = _ROOT / "docs" / "app.js"

# Element IDs the AAII branch of loadBreadthData writes into.
AAII_IDS = ("aaii-bull", "aaii-neut", "aaii-bear", "aaii-week")

# Element IDs the NAAIM branch writes into.
NAAIM_IDS = ("naaim-value", "naaim-as-of")

# The grid is two columns carrying four rows:
#
#     Fear & Greed | NAAIM
#     AAII (spans both)
#     NCFD         | MMTW
#     MMFI         | MMTH
#
# Every row totalling two columns is the invariant -- not "an even tile count",
# which is what the six-tile version of this constant encoded. Seven tiles fill
# four rows exactly *because* one of them spans, so the count and the span are
# one fact, not two.
EXPECTED_BREADTH_TILES = 7
EXPECTED_COLUMNS = 2
EXPECTED_WIDE_TILES = 1

# Document order, which IS row order for this grid.
EXPECTED_TILE_ORDER = (
    "CNN Fear & Greed", "NAAIM Exposure", "AAII Sentiment",
    "NCFD", "MMTW", "MMFI", "MMTH",
)

# One tile carries a span class (`class="breadth-item bi-wide"`), so an
# exact match on `class="breadth-item"` misses it. An earlier exact-match
# pattern matched zero elements outright, and that failure is silent in the
# wrong direction -- a count of 0 reads as a missing grid rather than a stale
# regex, which is what test_the_tile_regex_matches_the_markup_it_counts pins.
TILE_RE = re.compile(r'class="breadth-item[^"]*"')


class BreadthMarkupTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.html = INDEX_HTML.read_text(encoding="utf-8")
        cls.js = APP_JS.read_text(encoding="utf-8")

    def test_every_aaii_id_written_by_app_js_exists_in_the_markup(self) -> None:
        for element_id in AAII_IDS:
            with self.subTest(element_id=element_id):
                self.assertIn(
                    f'id="{element_id}"',
                    self.html,
                    f"docs/app.js writes #{element_id} but docs/index.html has no "
                    "such element; the tile would keep its placeholder em dashes "
                    "with no error anywhere",
                )

    def test_app_js_writes_every_aaii_element(self) -> None:
        """The reverse direction: markup with no writer renders a dead em dash."""
        for element_id in AAII_IDS:
            with self.subTest(element_id=element_id):
                self.assertIn(
                    element_id,
                    self.js,
                    f"docs/index.html declares #{element_id} but docs/app.js never "
                    "writes it",
                )

    def test_every_naaim_id_written_by_app_js_exists_in_the_markup(self) -> None:
        """Same pairing as AAII above, and the same silent failure it guards:
        loadBreadthData writes into whatever getElementById returns, so a typo
        leaves the tile on its placeholder em dashes with no console error."""
        for element_id in NAAIM_IDS:
            with self.subTest(element_id=element_id):
                self.assertIn(
                    f'id="{element_id}"',
                    self.html,
                    f"docs/app.js writes #{element_id} but docs/index.html has "
                    "no such element",
                )

    def test_app_js_writes_every_naaim_element(self) -> None:
        for element_id in NAAIM_IDS:
            with self.subTest(element_id=element_id):
                self.assertIn(
                    element_id,
                    self.js,
                    f"docs/index.html declares #{element_id} but docs/app.js "
                    "never writes it",
                )

    def test_every_grid_row_is_full(self) -> None:
        """Each row must total two columns. Seven tiles only fill four rows
        because AAII spans both, so a dropped span or an added tile leaves the
        ragged half-row the old six-tile count guarded.

        The spans are walked from the MARKUP in document order. An earlier
        version compared module constants to literals (`3 * (3 - 1) + 6 == 12`),
        which could not fail from any production change at all.
        """
        spans = self._tile_spans()
        self.assertEqual(len(spans), EXPECTED_BREADTH_TILES)
        self.assertEqual(spans.count(EXPECTED_COLUMNS), EXPECTED_WIDE_TILES)

        rows, row = [], 0
        for span in spans:
            row += span
            self.assertLessEqual(
                row, EXPECTED_COLUMNS, f"a tile overflows its row: {spans}"
            )
            if row == EXPECTED_COLUMNS:
                rows.append(row)
                row = 0
        self.assertEqual(
            row, 0, f"the last row is {row}/{EXPECTED_COLUMNS} short: {spans}"
        )
        self.assertEqual(len(rows), 4)

    def test_the_wide_tile_is_aaii(self) -> None:
        """The full-width span belongs to AAII specifically. It is the one tile
        rendering three figures instead of one, so it is the tile the extra room
        is for; the row arithmetic above stays green if the span moves to any
        other tile."""
        self.assertIn("bi-wide", self._tile_block_for("AAII Sentiment"))

    def test_the_tiles_are_in_the_intended_row_order(self) -> None:
        """DOM order is row order for this grid, so the layout the card renders
        is a fact about the markup's sequence and nothing else pins it."""
        labels = re.findall(r'class="breadth-label">([^<]+)<', self.html)
        self.assertEqual(tuple(html_module.unescape(x) for x in labels),
                         EXPECTED_TILE_ORDER)

    def _tile_spans(self) -> list:
        """Column span of each breadth tile, in document order."""
        return [
            EXPECTED_COLUMNS if "bi-wide" in cls else 1
            for cls in TILE_RE.findall(self.html)
        ]

    def _tile_block_for(self, label: str) -> str:
        """The markup of the tile carrying `label`, back to its opening div."""
        at = self.html.find(f">{label}<")
        self.assertNotEqual(at, -1, f"no tile labelled {label!r} in the markup")
        start = self.html.rfind('class="breadth-item', 0, at)
        self.assertNotEqual(start, -1, f"{label!r} is not inside a breadth tile")
        return self.html[start:at]

    def test_the_tile_regex_matches_the_markup_it_counts(self) -> None:
        """Guards the guard. The previous exact-match pattern silently matched
        zero once tiles gained span classes, and a zero count reads as a
        missing grid rather than a stale test."""
        self.assertGreater(len(TILE_RE.findall(self.html)), 0)

    def test_the_naaim_figure_is_not_tinted(self) -> None:
        """Same rule as AAII, different reason to reach for it: high exposure
        is contrarian-bearish, but no NAAIM threshold has been calibrated in
        this repo, and every tinted tile here tints off a level that has been.
        """
        block = self._naaim_render_block()
        for colour_class in ("'up'", "'dn'", '"up"', '"dn"', "--green", "--red"):
            with self.subTest(colour=colour_class):
                self.assertNotIn(
                    colour_class,
                    block,
                    "the NAAIM render block assigns a directional colour",
                )

    def test_the_aaii_figures_are_not_tinted_by_sentiment(self) -> None:
        """The card's other tiles tint by contrarian meaning -- a low NCFD is
        green. Applying that to AAII paints high bearish sentiment green, which
        contradicts the label printed beside it. The figures stay neutral and
        the labels carry the meaning.
        """
        aaii_block = self._aaii_render_block()
        for colour_class in ("'up'", "'dn'", '"up"', '"dn"', "--green", "--red"):
            with self.subTest(colour=colour_class):
                self.assertNotIn(
                    colour_class,
                    aaii_block,
                    "the AAII render block assigns a directional colour; see the "
                    ".aaii-parts comment in docs/style.css for why it must not",
                )

    def _naaim_render_block(self) -> str:
        return self._render_block("naaim")

    def _aaii_render_block(self) -> str:
        return self._render_block("aaii")

    def _render_block(self, key: str) -> str:
        """The `if (data.<key>) { ... }` branch of loadBreadthData.

        Bounded by its OWN braces, not by the next tile's start token. The
        earlier version sliced from one branch's marker to the next branch's
        marker, which coupled two independent guards in two ways: it forced a
        source order between the tiles for no functional reason, and it meant
        any comment that merely spelled the neighbouring token silently moved
        the boundary. Both bit during development -- a comment inside the NAAIM
        branch mentioning the AAII token swallowed the whole branch into the
        guard named for AAII.
        """
        marker = f"if (data.{key})"
        start = self.js.find(marker)
        self.assertNotEqual(start, -1, f"docs/app.js has no `{marker}` branch")

        open_at = self.js.find("{", start)
        self.assertNotEqual(open_at, -1, f"`{marker}` has no opening brace")

        depth = 0
        for i in range(open_at, len(self.js)):
            if self.js[i] == "{":
                depth += 1
            elif self.js[i] == "}":
                depth -= 1
                if depth == 0:
                    return self.js[start:i + 1]
        self.fail(f"unbalanced braces after `{marker}`")


if __name__ == "__main__":
    unittest.main()
