import re
import unittest
from pathlib import Path


APP_JS = Path(__file__).resolve().parents[1] / "docs" / "app.js"


class DashboardChartConfigTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        source = APP_JS.read_text(encoding="utf-8")
        widget_start = source.index("new TradingView.widget({")
        widget_end = source.index("\n      });", widget_start)
        cls.widget_config = source[widget_start:widget_end]

    def test_chart_enables_native_earnings_markers(self) -> None:
        self.assertIn('{ "id": "Earnings@tv-basicstudies" }', self.widget_config)
        self.assertIn('"mainSeriesProperties.esdShowEarnings": true', self.widget_config)
        self.assertIn('"calendar": false', self.widget_config)
        self.assertNotIn("Dividends@tv-basicstudies", self.widget_config)
        self.assertNotIn("Splits@tv-basicstudies", self.widget_config)

    def test_chart_studies_remain_object_form(self) -> None:
        studies_match = re.search(
            r'"studies": \[(.*?)\],\s*"studies_overrides"',
            self.widget_config,
            re.DOTALL,
        )
        self.assertIsNotNone(studies_match)
        studies = studies_match.group(1)

        self.assertIn('{ "id": "STD;Volume" }', studies)
        self.assertNotRegex(studies, r'(?m)^\s*"[^"]+"\s*,?\s*$')
        self.assertIn('"hide_legend": false', self.widget_config)
        self.assertIn(
            '"paneProperties.legendProperties.showStudyTitles": false',
            self.widget_config,
        )


if __name__ == "__main__":
    unittest.main()
