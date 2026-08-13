"""Guards for the NASI crosshair readout in docs/index.html and docs/app.js.

Two invariants live here, and only one of them is visible on screen.

The load-bearing one is that the readout never prints the summation *level*.
The Nasdaq McClellan summation is a running total with an arbitrary origin --
ours reads about -6,400 where StockCharts reads -139 for the same session -- so
it can never match a chart a user compares against. The panel header has always
shown the oscillator and the RSI for exactly that reason, and the crosshair
mirrors that pair. Nothing on screen tells a future editor this; hovering the
white summation line and printing its value is the obvious-looking change, and
it would be wrong in a way no reviewer catches by looking at the dashboard. See
the NASI section of CLAUDE.md.

The second is that the oscillator is derived, not read. `docs/data/nasi.json`
history points carry only date/summation/summation_ma/rsi -- `oscillator` exists
on `current` alone -- so `pt.oscillator` is silently `undefined` rather than an
error, and the readout would render an em dash forever.

The repo has no JavaScript test tooling, so a Python parser test is the only
automated guard available for these files.
"""

import re
import unittest
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
INDEX_HTML = _ROOT / "docs" / "index.html"
APP_JS = _ROOT / "docs" / "app.js"
STYLE_CSS = _ROOT / "docs" / "style.css"

# Element IDs the readout writes into.
READOUT_IDS = ("nasi-ro-date", "nasi-ro-osc", "nasi-ro-rsi")

# Footer text the crosshair replaced. Any of these reappearing means the row
# was reverted or duplicated.
RETIRED_FOOTER_STRINGS = (
    "summation shape only",
    "major-low band",
    "10-day MA",
    'id="nasi-range"',
)


class NasiCrosshairMarkupTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.html = INDEX_HTML.read_text(encoding="utf-8")
        cls.js = APP_JS.read_text(encoding="utf-8")
        cls.css = STYLE_CSS.read_text(encoding="utf-8")

    def test_readout_elements_exist(self) -> None:
        for element_id in READOUT_IDS:
            with self.subTest(element_id=element_id):
                self.assertIn(
                    f'id="{element_id}"',
                    self.html,
                    f"readout element #{element_id} is missing from docs/index.html",
                )

    def test_app_js_writes_every_readout_element(self) -> None:
        for element_id in READOUT_IDS:
            with self.subTest(element_id=element_id):
                self.assertIn(
                    f"getElementById('{element_id}')",
                    self.js,
                    f"docs/app.js never resolves #{element_id}; the readout would stay blank",
                )

    def test_retired_footer_text_is_gone(self) -> None:
        for needle in RETIRED_FOOTER_STRINGS:
            with self.subTest(needle=needle):
                self.assertNotIn(
                    needle,
                    self.html,
                    f"{needle!r} still in docs/index.html; the crosshair readout replaced that row",
                )

    def test_app_js_does_not_write_summation_into_the_readout(self) -> None:
        """The origin-relative level must never reach a readout element.

        Matches an assignment of any `summation`-bearing expression into a
        `nasi-ro-*` element, allowing for a chained `.toFixed(...)`.
        """
        offender = re.compile(
            r"nasi-?[Rr]o[A-Za-z]*El\s*\.\s*textContent\s*=[^;\n]*\bsummation\b"
        )
        match = offender.search(self.js)
        self.assertIsNone(
            match,
            "docs/app.js writes a summation value into the crosshair readout: "
            f"{match.group(0) if match else ''!r}. The summation level is "
            "origin-relative and is never surfaced as a figure -- show the "
            "oscillator instead (see NASI in CLAUDE.md).",
        )

    def test_oscillator_is_derived_from_consecutive_summations(self) -> None:
        self.assertIn(
            "deriveNasiOscillator",
            self.js,
            "the oscillator derivation helper is missing from docs/app.js",
        )
        self.assertRegex(
            self.js,
            r"summation\s*-\s*history\[\s*i\s*-\s*1\s*\]\.summation",
            "the oscillator is not derived by differencing consecutive "
            "summations; nasi.json history points carry no `oscillator` key, "
            "so reading one yields undefined",
        )

    def test_derived_oscillator_is_rounded_to_two_decimals(self) -> None:
        """Float noise would desync the readout from the header.

        -6304.07 - -6328.92 evaluates to 24.849999999999454, and the header
        renders 24.85 for that same session.
        """
        derivation = self._extract_function("deriveNasiOscillator")
        self.assertRegex(
            derivation,
            r"Math\.round\(.*\*\s*100\s*\)\s*/\s*100|toFixed\(\s*2\s*\)",
            "deriveNasiOscillator does not round to 2 decimals; the readout "
            "would show float noise where the header shows two decimals",
        )

    def test_crosshair_is_recreated_by_the_renderer(self) -> None:
        """renderNasiChart clears the SVG, so the line cannot be appended once."""
        render = self._extract_function("renderNasiChart")
        self.assertIn(
            "nasiCrosshair",
            render,
            "renderNasiChart does not create the crosshair line; because it "
            "resets svg.innerHTML, a line appended anywhere else is destroyed "
            "on the next panel drag or window resize",
        )

    def test_pointer_handlers_guard_unloaded_history(self) -> None:
        init = self._extract_function("initNasiCrosshair")
        self.assertIn(
            "nasiHistory",
            init,
            "the pointer handler does not guard on nasiHistory; the panel "
            "renders before nasi.json resolves and a hover would throw",
        )

    def test_svg_box_is_fully_hittable(self) -> None:
        """Plotted paths are fill:none, so only the box makes the chart hittable."""
        self.assertRegex(
            self.css,
            r"\.nasi-chart\s*\{[^}]*pointer-events\s*:\s*all",
            "docs/style.css does not give .nasi-chart pointer-events: all; the "
            "readout would drop out over the gap between the two panes",
        )

    def test_readout_date_is_yellow_and_values_are_bright(self) -> None:
        self.assertRegex(
            self.css,
            r"\.nasi-ro-date\s*\{[^}]*color\s*:\s*var\(--yellow\)",
            ".nasi-ro-date is not coloured var(--yellow)",
        )
        self.assertRegex(
            self.css,
            r"\.nasi-ro-val\s*\{[^}]*color\s*:\s*var\(--text\)",
            ".nasi-ro-val is not coloured var(--text)",
        )

    def test_svg_does_not_claim_a_noninteractive_role(self) -> None:
        svg = re.search(r"<svg[^>]*id=\"nasi-chart\"[^>]*>", self.html)
        self.assertIsNotNone(svg, "the #nasi-chart <svg> tag was not found")
        self.assertNotIn(
            'role="img"',
            svg.group(0),
            'the chart takes pointer input, so role="img" (non-interactive) is stale',
        )
        self.assertIn(
            "aria-label=",
            svg.group(0),
            "the chart lost its aria-label; it is the only description "
            "assistive tech gets for this panel",
        )

    def _extract_function(self, name: str) -> str:
        """Return the balanced body of `function <name>(...) { ... }`."""
        marker = re.search(rf"function\s+{re.escape(name)}\s*\([^)]*\)\s*\{{", self.js)
        self.assertIsNotNone(marker, f"function {name} not found in docs/app.js")
        start = marker.end() - 1
        depth = 0
        for offset, char in enumerate(self.js[start:]):
            if char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    return self.js[start : start + offset + 1]
        raise AssertionError(f"unbalanced braces in function {name}")


if __name__ == "__main__":
    unittest.main()
