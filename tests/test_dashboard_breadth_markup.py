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

# Above the container-query threshold the grid is twelve columns carrying two
# rows: three sentiment tiles (3 + 6 + 3) then four breadth percentages
# (3 x 4). Both rows total twelve, which is the invariant -- not "an even
# count", which is what the six-tile version of this constant encoded.
EXPECTED_BREADTH_TILES = 7
EXPECTED_SENTIMENT_TILES = 3
EXPECTED_WIDE_TILES = 1

# Tiles carry span classes now (`class="breadth-item bi-narrow"`), so the
# exact-match pattern this file used matches zero elements. That failure is
# silent in the wrong direction -- a count of 0 looks like a missing grid
# rather than a stale regex.
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

    def test_the_breadth_grid_fills_both_rows_exactly(self) -> None:
        """Twelve columns per row is the invariant. Three sentiment tiles at
        3 + 6 + 3, then four breadth tiles at 3 each -- either row coming up
        short leaves the ragged half-row the old six-tile count guarded."""
        tiles = len(TILE_RE.findall(self.html))
        self.assertEqual(tiles, EXPECTED_BREADTH_TILES)

        wide = len(re.findall(r'class="breadth-item bi-wide"', self.html))
        narrow = len(re.findall(r'class="breadth-item bi-narrow"', self.html))
        self.assertEqual(wide, EXPECTED_WIDE_TILES)
        self.assertEqual(narrow, EXPECTED_BREADTH_TILES - EXPECTED_WIDE_TILES)

        # Row 1: 3 + 6 + 3. Row 2: 3 x 4. Both twelve.
        self.assertEqual(3 * (EXPECTED_SENTIMENT_TILES - 1) + 6, 12)
        self.assertEqual(3 * (EXPECTED_BREADTH_TILES - EXPECTED_SENTIMENT_TILES), 12)

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

    def test_the_naaim_branch_precedes_the_aaii_branch(self) -> None:
        """_aaii_render_block slices from `data.aaii` to the ncfd loop, so a
        NAAIM branch placed between them would fall inside a guard named for
        AAII -- passing today, and failing under an AAII heading the first time
        anyone tints NAAIM."""
        self.assertLess(
            self.js.find("data.naaim"),
            self.js.find("data.aaii"),
            "docs/app.js renders NAAIM after AAII; move it above so the AAII "
            "block guard keeps covering only AAII",
        )

    def _naaim_render_block(self) -> str:
        start = self.js.find("data.naaim")
        self.assertNotEqual(start, -1, "docs/app.js never reads data.naaim")
        end = self.js.find("data.aaii", start)
        self.assertNotEqual(end, -1, "could not find the end of the NAAIM block")
        return self.js[start:end]

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

    def _aaii_render_block(self) -> str:
        """The AAII branch of loadBreadthData, up to the next tile's block."""
        start = self.js.find("data.aaii")
        self.assertNotEqual(start, -1, "docs/app.js never reads data.aaii")
        # The breadth loop over the barchart tiles follows the AAII branch.
        end = self.js.find("['ncfd'", start)
        self.assertNotEqual(
            end, -1, "could not find the end of the AAII render block"
        )
        return self.js[start:end]


if __name__ == "__main__":
    unittest.main()
