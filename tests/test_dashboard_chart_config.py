"""Guards for the TradingView chart constructor in docs/app.js.

The free ``tv.js`` embed applies only the FIRST 5 entries of ``studies`` and
silently discards the rest -- no console error, no exception. That cap has cost
this repo the volume pane twice (PR #23 via a type bug, PR #72 by pushing the
array to 6 entries). These tests encode the hard rule that keeps it from
happening a third time: ``STD;Volume`` is pinned to index 0 and the array never
exceeds the cap.
"""

import json
import unittest
from pathlib import Path


APP_JS = Path(__file__).resolve().parents[1] / "docs" / "app.js"

# TradingView free Advanced Chart embed: entries past this many are dropped.
MAX_STUDIES = 5
VOLUME_STUDY = "STD;Volume"


def _extract_json_array(source: str, key: str) -> str:
    """Return the balanced ``[...]`` literal that follows ``"<key>":``."""
    marker = f'"{key}": ['
    start = source.index(marker) + len(marker) - 1
    depth = 0
    for offset, char in enumerate(source[start:]):
        if char == "[":
            depth += 1
        elif char == "]":
            depth -= 1
            if depth == 0:
                return source[start : start + offset + 1]
    raise AssertionError(f'unbalanced brackets in "{key}" array')


class DashboardChartConfigTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        source = APP_JS.read_text(encoding="utf-8")
        widget_start = source.index("new TradingView.widget({")
        widget_end = source.index("\n      });", widget_start)
        cls.widget_config = source[widget_start:widget_end]
        cls.studies_src = _extract_json_array(cls.widget_config, "studies")
        # The array is plain JSON, so a parse failure here means someone
        # introduced a trailing comma, comment, or JS expression.
        cls.studies = json.loads(cls.studies_src)

    def test_volume_study_is_first(self) -> None:
        """HARD RULE: the volume study is never removed and never demoted.

        Index 0 is the only position the 5-study cap can never reach, so
        pinning it there makes the volume pane structurally safe no matter what
        else is added later.
        """
        ids = [entry["id"] for entry in self.studies]
        self.assertIn(
            VOLUME_STUDY,
            ids,
            f"{VOLUME_STUDY} was removed from the chart studies. It renders the "
            "volume bars AND the average-volume overlay and must never be dropped.",
        )
        self.assertEqual(
            ids[0],
            VOLUME_STUDY,
            f"{VOLUME_STUDY} must be the FIRST study (found at index "
            f"{ids.index(VOLUME_STUDY)}). The free tv.js embed keeps only the "
            f"first {MAX_STUDIES} studies; any later position can be silently "
            "dropped by a future addition.",
        )

    def test_studies_within_free_embed_cap(self) -> None:
        """The free embed silently discards studies past the cap."""
        self.assertLessEqual(
            len(self.studies),
            MAX_STUDIES,
            f"{len(self.studies)} studies configured but the free tv.js embed "
            f"applies only the first {MAX_STUDIES}. Entries past the cap render "
            "nothing and fail silently -- drop one instead of appending.",
        )

    def test_studies_remain_object_form(self) -> None:
        """Mixing bare-string ids with object entries silently drops the strings."""
        for index, entry in enumerate(self.studies):
            self.assertIsInstance(
                entry,
                dict,
                f"studies[{index}] must be object form ({{ \"id\": ... }}); a bare "
                "string is silently dropped when any other entry is an object.",
            )
            self.assertIn("id", entry, f"studies[{index}] is missing its \"id\" key")

    def test_volume_readouts_stay_in_legend(self) -> None:
        """The legend overrides must not hide the volume/OHLC readouts."""
        self.assertIn('"hide_legend": false', self.widget_config)
        self.assertIn(
            '"paneProperties.legendProperties.showStudyTitles": false',
            self.widget_config,
        )

    def test_no_unsupported_corporate_event_studies(self) -> None:
        """Earnings/dividends/splits studies cost a slot the volume pane needs.

        ``Earnings@tv-basicstudies`` renders markers but consumes one of the 5
        slots; adding it in PR #72 pushed ``STD;Volume`` off the cap. Re-adding
        any of these requires giving up a moving average first -- do that
        deliberately, not by appending.
        """
        ids = [entry["id"] for entry in self.studies]
        for unsupported in (
            "Earnings@tv-basicstudies",
            "Dividends@tv-basicstudies",
            "Splits@tv-basicstudies",
        ):
            self.assertNotIn(unsupported, ids)


if __name__ == "__main__":
    unittest.main()
