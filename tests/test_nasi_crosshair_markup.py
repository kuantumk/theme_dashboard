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
COMPUTE_NASI_PY = _ROOT / "src" / "data_collection" / "compute_nasi.py"

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
        cls.compute_nasi = COMPUTE_NASI_PY.read_text(encoding="utf-8")

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

    # ── Chart window ────────────────────────────────────────────────
    #
    # The panel plots the newest NASI_CHART_SESSIONS sessions out of a longer
    # exported payload. Every guard below defends one shared index space: the
    # renderer, the pointer-to-index mapper, the crosshair handler and the
    # readout must all count the same number of sessions. A mismatch has no
    # symptom on screen -- the crosshair just names a neighbouring session.

    def test_chart_window_is_a_named_constant(self) -> None:
        self.assertRegex(
            self.js,
            r"const\s+NASI_CHART_SESSIONS\s*=\s*\d+",
            "docs/app.js does not declare NASI_CHART_SESSIONS; the plotted "
            "window must be a named constant, not a literal at the slice site",
        )

    def test_history_and_oscillator_share_one_window(self) -> None:
        """nasiOsc is indexed in parallel with nasiHistory, so both are sliced."""
        load = self._extract_function("loadNasiData")
        self.assertRegex(
            load,
            r"nasiHistory\s*=\s*\w+\.slice\(\s*-\s*NASI_CHART_SESSIONS\s*\)",
            "loadNasiData does not bound nasiHistory by NASI_CHART_SESSIONS",
        )
        self.assertRegex(
            load,
            r"nasiOsc\s*=\s*deriveNasiOscillator\([^)]*\)"
            r"\.slice\(\s*-\s*NASI_CHART_SESSIONS\s*\)",
            "loadNasiData does not bound nasiOsc by the same window as "
            "nasiHistory; the two arrays are indexed in parallel, so slicing "
            "only one desyncs the crosshair readout from the plotted session",
        )

    def test_oscillator_is_derived_before_the_slice(self) -> None:
        """Deriving after the slice costs the oldest visible session its value.

        `deriveNasiOscillator` differences consecutive summations, so index 0
        has no predecessor. Running it on the full payload and slicing the
        result keeps a real oscillator for every plotted session.
        """
        load = self._extract_function("loadNasiData")
        self.assertRegex(
            load,
            r"deriveNasiOscillator\(\s*\w+\s*\)",
            "deriveNasiOscillator is not called on the whole payload; deriving "
            "from an already-sliced array leaves the oldest plotted session "
            "showing an em dash for OSC",
        )

    def test_render_and_first_readout_use_the_sliced_history(self) -> None:
        """The two call sites that take an argument, not the module variable.

        `renderNasiChart` receives its history as a parameter and the
        first-paint readout receives an index, so neither inherits the window
        from `nasiHistory` the way `nasiIndexAt` and `initNasiCrosshair` do.
        Leaving either on the fetched payload plots the full export against a
        windowed pointer space, and nothing on screen says so.
        """
        load = self._extract_function("loadNasiData")
        self.assertRegex(
            load,
            r"renderNasiChart\(\s*nasiHistory\s*\)",
            "loadNasiData does not pass the sliced history to renderNasiChart",
        )
        self.assertRegex(
            load,
            r"showNasiReadout\(\s*nasiHistory\.length\s*-\s*1\s*\)",
            "loadNasiData does not derive the first-paint readout index from "
            "the sliced history",
        )

    def test_exporter_window_is_untouched(self) -> None:
        """The chart window is a view decision; the export is retention.

        Trimming EXPORT_SESSIONS instead would leave the deployed chart at the
        old span until the next daily workflow run, because code PRs reset
        docs/data/. Changing this deliberately means updating this test too.
        """
        self.assertRegex(
            self.compute_nasi,
            r"EXPORT_SESSIONS\s*=\s*378",
            "src/data_collection/compute_nasi.py no longer exports 378 "
            "sessions; the chart window is applied client-side and the export "
            "is deliberately wider (see NASI in CLAUDE.md)",
        )

    # ── Band rails and markers ──────────────────────────────────────
    #
    # The RSI pane marks both ends of the series: oversold at or below 10,
    # overbought at or above 80. Nothing on screen records why these are
    # *level* tests rather than crossing tests, or why the rails are not the
    # marker colour, so the guards below carry that.

    def test_rsi_pane_draws_an_overbought_rail(self) -> None:
        render = self._extract_function("renderNasiChart")
        self.assertRegex(
            render,
            r"\[\s*NASI_OVERBOUGHT\s*,",
            "renderNasiChart draws no rail at NASI_OVERBOUGHT; the overbought "
            "markers would sit on an unlabelled stretch of the pane",
        )

    def test_rails_are_not_the_marker_colour(self) -> None:
        """A red rail would sit under the red markers it labels.

        The 80 rail lands at y 116 and its markers span y 112.9-117.5, so the
        pair would read as one thickened line. Rails stay amber on both sides;
        the markers carry the signal colour.
        """
        render = self._extract_function("renderNasiChart")
        self.assertRegex(
            render,
            r"\[\s*NASI_OVERBOUGHT\s*,\s*'var\(--amber\)'",
            "the NASI_OVERBOUGHT rail is not amber; a rail drawn in the "
            "marker colour merges with the markers it is meant to label",
        )

    def test_both_bands_are_level_tests_not_crossing_tests(self) -> None:
        """The panel reports a phase, so a band must read as a run.

        In the plotted year 18 consecutive sessions sat at or above 80, where a
        crossing test would have drawn a single marker. The oversold side has
        always been a level test; this pins the overbought side to match.
        """
        render = self._extract_function("renderNasiChart")
        self.assertRegex(
            render,
            r"pt\.rsi\s*<=\s*NASI_OVERSOLD",
            "the oversold marker rule is not a level test",
        )
        self.assertRegex(
            render,
            r"pt\.rsi\s*>=\s*NASI_OVERBOUGHT",
            "the overbought marker rule is not a level test against the "
            "session's own RSI",
        )
        self.assertNotRegex(
            render,
            r"history\[\s*i\s*[-+]\s*1\s*\]",
            "renderNasiChart looks at an adjacent session; the band markers "
            "are level tests, not crossing tests, because the panel reports a "
            "phase rather than a signal date (see NASI in CLAUDE.md)",
        )

    def test_one_marker_loop_emits_both_band_colours(self) -> None:
        """Two loops would let the two band rules drift apart."""
        render = self._extract_function("renderNasiChart")
        for colour in ("var(--green)", "var(--red)"):
            with self.subTest(colour=colour):
                self.assertIn(
                    colour,
                    render,
                    f"renderNasiChart never emits {colour}; both bands are "
                    "marked from one loop so neither rule can change unseen",
                )

    def test_markers_are_scale_corrected_ellipses(self) -> None:
        """A <circle> deforms under preserveAspectRatio="none".

        Its `r` is a viewBox length, so it renders as an ellipse whose shape
        drifts with panel width. Matching the creation call rather than the
        bare word matters: the geometry comment this guard protects names
        `circle` twice, and `_extract_function` returns comments verbatim.
        """
        render = self._extract_function("renderNasiChart")
        self.assertRegex(
            render,
            r"add\(\s*'ellipse'",
            "band markers are not <ellipse> elements",
        )
        self.assertRegex(
            render,
            r"rx:\s*\(\s*2\s*/\s*sx\s*\)",
            "marker rx is not divided by the measured x-scale, so markers "
            "stretch as the panel is dragged",
        )
        self.assertNotRegex(
            render,
            r"""add\(\s*['"]circle['"]""",
            "renderNasiChart creates a <circle>; under "
            'preserveAspectRatio="none" its r is a viewBox length, so it '
            "renders as an ellipse whose shape drifts with panel width",
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
