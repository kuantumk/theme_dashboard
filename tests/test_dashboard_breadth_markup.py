"""Guards for the Market Breadth & Sentiment tiles in docs/index.html.

The AAII tile is the one tile in the card that renders three figures instead of
one, so it carries its own element ids and its own type scale. A missing id is
the failure mode worth guarding: `loadBreadthData` writes into whatever
`getElementById` returns and a miss is `null`, so the tile silently keeps its
placeholder em dashes with no console error and no visual cue that anything
broke. Nothing on screen distinguishes that from a week AAII did not publish.

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

# The breadth grid is `grid-template-columns: 1fr 1fr`, so an odd tile count
# leaves a ragged half-row. Six is what keeps it three even rows.
EXPECTED_BREADTH_TILES = 6


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

    def test_the_retired_naaim_tile_is_gone(self) -> None:
        """NAAIM's reading moved behind a membership wall in 2026-08. Any
        identifier reappearing means the tile was restored or duplicated."""
        for haystack, name in ((self.html, "docs/index.html"), (self.js, "docs/app.js")):
            with self.subTest(file=name):
                self.assertNotIn(
                    "naaim",
                    haystack.lower(),
                    f"{name} still references NAAIM; its source is paywalled and "
                    "the export no longer publishes the key",
                )

    def test_the_breadth_grid_keeps_an_even_tile_count(self) -> None:
        tiles = len(re.findall(r'class="breadth-item"', self.html))
        self.assertEqual(
            tiles,
            EXPECTED_BREADTH_TILES,
            "the breadth grid is two columns, so an odd tile count leaves a "
            "ragged half-row at the bottom of the card",
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
