"""Guards for the V/A cutoff dropdowns in docs/index.html.

The five stock-list tabs each carry their own copy of the cutoff markup, on
purpose: keeping it in HTML is what makes "which tabs have filters" greppable
rather than encoded in a JS tab allowlist. The cost of that choice is five
copies that must stay identical, because `initTickerFilters` in docs/app.js
syncs a cutoff change across every bar by assigning the same `value` string to
each copy:

    document.querySelectorAll(`.tt-filter-select[data-filter="${key}"]`)
      .forEach(s => { s.value = select.value; });

An `<option value>` that exists on one bar but not another makes that
assignment a silent no-op there — the select falls back to its first option, so
one tab quietly filters at a different cutoff than the rest with nothing on
screen to say so. Browser verification would only catch that if the tester
happened to change a cutoff and then visit the odd tab.

These tests pin the invariant the sync depends on. They are deliberately cheap
string/DOM checks: the repo has no JavaScript test tooling, so a Python parser
test is the only automated guard available for this file.
"""

import re
import unittest
from html.parser import HTMLParser
from pathlib import Path

INDEX_HTML = Path(__file__).resolve().parents[1] / "docs" / "index.html"

# Tabs whose rows come from the screening parquet and therefore carry cutoffs.
EXPECTED_BAR_COUNT = 5

EXPECTED_VOL_OPTIONS = [("10000000", "$10M"), ("50000000", "$50M"), ("100000000", "$100M")]
EXPECTED_ADR_OPTIONS = [
    ("0.025", "2.5%"), ("0.03", "3%"), ("0.035", "3.5%"),
    ("0.04", "4%"), ("0.045", "4.5%"), ("0.05", "5%"),
]
EXPECTED_DEFAULTS = {"vol": "50000000", "adr": "0.04"}


class _FilterSelectParser(HTMLParser):
    """Collect every `.tt-filter-select` as (data-filter, [(value, label)], default)."""

    def __init__(self):
        super().__init__()
        self.selects = []
        self._current = None
        self._pending_option = None

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == "select" and "tt-filter-select" in (a.get("class") or ""):
            self._current = {"filter": a.get("data-filter"), "options": [], "default": None}
        elif tag == "option" and self._current is not None:
            self._pending_option = (a.get("value"), "selected" in a)

    def handle_data(self, data):
        if self._pending_option is not None:
            value, is_default = self._pending_option
            self._current["options"].append((value, data.strip()))
            if is_default:
                self._current["default"] = value
            self._pending_option = None

    def handle_endtag(self, tag):
        if tag == "select" and self._current is not None:
            self.selects.append(self._current)
            self._current = None


def _parse_selects():
    parser = _FilterSelectParser()
    parser.feed(INDEX_HTML.read_text(encoding="utf-8"))
    return parser.selects


class FilterMarkupTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.selects = _parse_selects()
        cls.by_filter = {"vol": [], "adr": []}
        for s in cls.selects:
            if s["filter"] in cls.by_filter:
                cls.by_filter[s["filter"]].append(s)

    def test_every_bar_carries_both_cutoffs(self):
        self.assertEqual(len(self.selects), EXPECTED_BAR_COUNT * 2)
        self.assertEqual(len(self.by_filter["vol"]), EXPECTED_BAR_COUNT)
        self.assertEqual(len(self.by_filter["adr"]), EXPECTED_BAR_COUNT)

    def test_option_sets_are_identical_across_bars(self):
        # The cross-bar sync assigns one value string to every copy; a value
        # missing from one bar makes that assignment silently do nothing there.
        for key in ("vol", "adr"):
            option_sets = {tuple(s["options"]) for s in self.by_filter[key]}
            self.assertEqual(
                len(option_sets), 1,
                f"{key} cutoff options differ between bars: {option_sets}")

    def test_defaults_are_identical_and_correct(self):
        for key, expected in EXPECTED_DEFAULTS.items():
            defaults = {s["default"] for s in self.by_filter[key]}
            self.assertEqual(
                defaults, {expected},
                f"{key} default must be {expected} on every bar, got {defaults}")

    def test_option_values_and_labels_match_the_documented_cutoffs(self):
        self.assertEqual(self.by_filter["vol"][0]["options"], EXPECTED_VOL_OPTIONS)
        self.assertEqual(self.by_filter["adr"][0]["options"], EXPECTED_ADR_OPTIONS)

    def test_every_option_value_parses_as_a_number(self):
        # applyTickerFilters does parseFloat(select.value) and ignores a
        # non-finite result, which would leave that cutoff silently unchanged.
        for s in self.selects:
            for value, label in s["options"]:
                self.assertRegex(value, r"^\d+(\.\d+)?$", f"{label} -> {value!r}")

    def test_no_toggle_buttons_survive_from_the_old_control(self):
        # The dropdowns replaced on/off buttons; a leftover button would still
        # match the old delegated click handler shape if one were reintroduced.
        self.assertNotIn("tt-filter-btn", INDEX_HTML.read_text(encoding="utf-8"))


class TimeTravelBarTests(unittest.TestCase):
    def test_filters_sit_inside_a_time_travel_bar(self):
        # .tt-filters uses margin-left:auto to bear right within the bar's flex
        # flow; outside that bar the dates-left/cutoffs-right split collapses.
        html = INDEX_HTML.read_text(encoding="utf-8")
        bars = re.findall(
            r'<div class="time-travel-bar".*?</div>\s*</div>', html, re.S)
        with_filters = [b for b in bars if "tt-filters" in b]
        self.assertGreaterEqual(len(with_filters), 1)
        for bar in with_filters:
            self.assertIn("time-travel-dates", bar)


if __name__ == "__main__":
    unittest.main()
