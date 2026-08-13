"""Guards for the chart-left / list-right panel layout in docs/.

Three invariants, none of which a screenshot would catch.

**Order.** The chart pane renders left and the list pane right via flex
`order`, not DOM position -- the DOM is still list, handle, chart. So
`.left-panel` and `.right-panel` name DOM position, not screen position. Any
edit that drops or reshuffles those three `order` values silently reverts the
layout.

**Drag direction.** `initResizablePanels` sizes the list pane. When it sat left
of the handle, `startWidth + dx` was right. Now that it sits right, dragging
right must *shrink* it, so the term is `startWidth - dx`. A plus makes the
divider run away from the pointer -- it reads as a broken handle rather than a
sign error, and it is invisible in every static screenshot.

**Mobile order reset.** Below 1100px the panes stack and the handle is hidden.
Without resetting `order`, the desktop values apply to the vertical stack and
bury the ticker list under a full-height chart.

The per-tab widths are asserted here too, since they are only meaningful once
the pane has moved. The repo has no JavaScript test tooling, so a Python parser
test is the only automated guard available for these files.
"""

import re
import unittest
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
STYLE_CSS = _ROOT / "docs" / "style.css"
APP_JS = _ROOT / "docs" / "app.js"

# The floor the time-travel bar needs to stay on one row (measured; see the
# comment block above .left-panel in style.css).
TIME_TRAVEL_BAR_FLOOR = 385

# Per-tab list-pane widths, keyed by selector. Each is the tab's measured
# natural table width plus panel padding, scrollbar and card border.
PER_TAB_WIDTHS = {
    "#volume-left": 610,
    "#etf-left": 585,
    "#industry-left": 570,
    "#ep-left": 500,
    "#vars-left": 490,
    "#momentum-left": 440,
}

RESPONSIVE_BREAKPOINT = "max-width: 1100px"


class DashboardPanelLayoutTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        # Strip comments before parsing. This stylesheet documents nearly every
        # non-obvious rule, and a comment sitting directly above a selector
        # otherwise lands inside the selector capture and stops it matching.
        raw = STYLE_CSS.read_text(encoding="utf-8")
        cls.css = re.sub(r"/\*.*?\*/", "", raw, flags=re.DOTALL)
        cls.js = APP_JS.read_text(encoding="utf-8")
        cls.desktop_css, cls.responsive_css = cls._split_at_breakpoint(cls.css)

    # ── order ────────────────────────────────────────────────────────────
    def test_chart_pane_orders_before_handle_before_list(self) -> None:
        """Asserted as a relation, so renumbering the values stays valid."""
        chart = self._order_value(self.desktop_css, ".right-panel")
        handle = self._order_value(self.desktop_css, ".resize-handle")
        listing = self._order_value(self.desktop_css, ".left-panel")

        missing = [
            name for name, value in
            (("right-panel", chart), ("resize-handle", handle), ("left-panel", listing))
            if value is None
        ]
        self.assertEqual(
            missing, [],
            f"{missing} declare no `order` in docs/style.css; without all three "
            "the panes fall back to DOM order and the list returns to the left",
        )

        self.assertLess(
            chart, handle,
            "the chart pane must order before the resize handle (chart on the left)",
        )
        self.assertLess(
            handle, listing,
            "the resize handle must order before the list pane (list on the right)",
        )

    def test_responsive_block_resets_order_for_all_three_panes(self) -> None:
        for selector in (".left-panel", ".right-panel", ".resize-handle"):
            with self.subTest(selector=selector):
                self.assertIsNotNone(
                    self._order_value(self.responsive_css, selector),
                    f"{selector} has no `order` reset inside {RESPONSIVE_BREAKPOINT}; "
                    "stacked mobile would inherit the desktop order and put the "
                    "chart above the ticker list",
                )

    # ── divider side ─────────────────────────────────────────────────────
    def test_list_pane_divider_is_on_its_left(self) -> None:
        body = self._rule_body(self.desktop_css, ".left-panel", require="border")
        self.assertIn(
            "border-left",
            body,
            "the list pane sits right of the chart, so its divider belongs on "
            "its left edge",
        )
        self.assertNotIn(
            "border-right",
            body,
            "the list pane still declares border-right; that edge now faces the "
            "window, not the chart",
        )

    # ── drag direction ───────────────────────────────────────────────────
    def test_drag_subtracts_the_pointer_delta(self) -> None:
        body = self._extract_function("initResizablePanels")
        self.assertRegex(
            body,
            r"startWidth\s*-\s*dx",
            "initResizablePanels does not subtract dx. The list pane sits right "
            "of the handle, so dragging right must shrink it; `startWidth + dx` "
            "makes the divider run away from the pointer.",
        )
        self.assertNotRegex(
            body,
            r"startWidth\s*\+\s*dx",
            "initResizablePanels still adds dx somewhere; that is the pre-swap "
            "arithmetic and inverts the handle",
        )

    # ── widths ───────────────────────────────────────────────────────────
    def test_each_widened_tab_declares_its_measured_width(self) -> None:
        for selector, expected in PER_TAB_WIDTHS.items():
            with self.subTest(selector=selector):
                width = self._width_px(self.desktop_css, selector)
                self.assertIsNotNone(
                    width,
                    f"{selector} declares no explicit width; its table would be "
                    "clipped at the shared default",
                )
                self.assertEqual(
                    width, expected,
                    f"{selector} is {width}px, expected {expected}px. If a column "
                    "was added or removed, re-measure the table and update both "
                    "this test and style.css.",
                )

    def test_widened_tabs_clear_the_time_travel_bar_floor(self) -> None:
        for selector in PER_TAB_WIDTHS:
            with self.subTest(selector=selector):
                width = self._width_px(self.desktop_css, selector)
                self.assertGreaterEqual(
                    width, TIME_TRAVEL_BAR_FLOOR,
                    f"{selector} is narrower than the {TIME_TRAVEL_BAR_FLOOR}px "
                    "floor; the time-travel bar would wrap onto a second row",
                )

    def test_list_pane_keeps_its_low_drag_floor(self) -> None:
        body = self._rule_body(self.desktop_css, ".left-panel", require="min-width")
        self.assertRegex(
            body,
            r"min-width\s*:\s*256px",
            "the list pane lost its 256px min-width; that floor is what lets the "
            "resize handle trade list width for chart width, and it is "
            "deliberately far below the per-tab defaults",
        )

    # ── helpers ──────────────────────────────────────────────────────────
    @staticmethod
    def _split_at_breakpoint(css: str) -> tuple[str, str]:
        """Return (css before the responsive block, css inside it)."""
        start = css.index(RESPONSIVE_BREAKPOINT)
        brace = css.index("{", start)
        depth = 0
        for offset, char in enumerate(css[brace:]):
            if char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    end = brace + offset
                    return css[:start], css[brace:end]
        raise AssertionError(f"unbalanced braces in the {RESPONSIVE_BREAKPOINT} block")

    @staticmethod
    def _rule_bodies(css: str, selector: str) -> list[str]:
        """Every rule body whose selector list contains `selector` exactly."""
        bodies = []
        for match in re.finditer(r"([^{}]+)\{([^{}]*)\}", css):
            selectors = {s.strip() for s in match.group(1).split(",")}
            if selector in selectors:
                bodies.append(match.group(2))
        return bodies

    def _rule_body(self, css: str, selector: str, *, require: str) -> str:
        for body in self._rule_bodies(css, selector):
            if require in body:
                return body
        self.fail(f"no rule for {selector} declaring {require!r}")

    def _order_value(self, css: str, selector: str) -> int | None:
        for body in self._rule_bodies(css, selector):
            match = re.search(r"\border\s*:\s*(-?\d+)", body)
            if match:
                return int(match.group(1))
        return None

    def _width_px(self, css: str, selector: str) -> int | None:
        for body in self._rule_bodies(css, selector):
            match = re.search(r"\bwidth\s*:\s*(\d+)px", body)
            if match:
                return int(match.group(1))
        return None

    def _extract_function(self, name: str) -> str:
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
