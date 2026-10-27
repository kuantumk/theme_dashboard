"""Breadth is scraped once per run.

Step 3 of the daily workflow writes data/market_breadth_latest.json; the
export reuses that reading when it is complete and fresh, and scrapes again
only when it is not (a standalone export, or step 3 missed a key).
"""

import json
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import src.reporting.export_dashboard_data as edd
from src.data_collection.barchart_breadth import (
    MAX_READING_AGE,
    breadth_reading_is_fresh,
    get_market_breadth,
)

NOW = datetime(2026, 10, 27, 16, 0)
FULL = {"ncfd": 49.25, "mmfi": 33.2, "mmtw": 41.0, "mmth": 55.5}


def reading(age=timedelta(minutes=30), **values):
    return {**FULL, **values, "timestamp": (NOW - age).isoformat()}


class FreshnessTests(unittest.TestCase):
    def test_a_complete_recent_reading_is_fresh(self):
        self.assertTrue(breadth_reading_is_fresh(reading(), now=NOW))

    def test_a_missing_key_is_not_fresh(self):
        self.assertFalse(breadth_reading_is_fresh(reading(mmth=None), now=NOW))

    def test_an_old_reading_is_not_fresh(self):
        self.assertFalse(breadth_reading_is_fresh(
            reading(age=MAX_READING_AGE + timedelta(minutes=1)), now=NOW))

    def test_an_undated_reading_is_not_fresh(self):
        self.assertFalse(breadth_reading_is_fresh(FULL, now=NOW))
        self.assertFalse(breadth_reading_is_fresh({**FULL, "timestamp": "yesterday"}, now=NOW))


class StepThreeTests(unittest.TestCase):
    def test_step3_carries_all_four_keys_even_when_one_is_missed(self):
        with patch("src.data_collection.barchart_breadth.fetch_barchart_breadth",
                   return_value={"ncfd": 50.0, "mmfi": 40.0, "mmtw": 45.0}):
            out = get_market_breadth()
        self.assertEqual({k: out[k] for k in FULL},
                         {"ncfd": 50.0, "mmfi": 40.0, "mmtw": 45.0, "mmth": None})
        self.assertIn("timestamp", out)


class ExportReuseTests(unittest.TestCase):
    def run_export(self, latest, scrape):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            history_file = out / "market_breadth.json"
            latest_file = out / "market_breadth_latest.json"
            latest_file.write_text(json.dumps(latest), encoding="utf-8")
            with patch.object(edd, "OUTPUT_DIR", out), \
                 patch.object(edd, "BREADTH_HISTORY_FILE", history_file), \
                 patch.object(edd, "BREADTH_FILE", latest_file), \
                 patch.object(edd, "fetch_barchart_breadth", return_value=scrape) as fetch, \
                 patch.object(edd, "fetch_cnn_fear_greed", return_value=None), \
                 patch.object(edd, "fetch_naaim_exposure", return_value=None), \
                 patch.object(edd, "fetch_aaii_sentiment", return_value=None):
                edd.update_breadth_history("2026-10-27")
            return json.loads(history_file.read_text(encoding="utf-8")), fetch

    def test_a_fresh_step3_reading_is_not_scraped_again(self):
        latest = {**FULL, "timestamp": datetime.now().isoformat()}
        history, fetch = self.run_export(latest, {"mmfi": 99.0})
        fetch.assert_not_called()
        self.assertEqual({k: history[k]["current"] for k in FULL}, FULL)

    def test_an_incomplete_step3_reading_is_scraped_again(self):
        latest = {**FULL, "mmth": None, "timestamp": datetime.now().isoformat()}
        history, fetch = self.run_export(latest, {"mmth": 60.0})
        fetch.assert_called_once()
        self.assertEqual(history["mmth"]["current"], 60.0)
        self.assertEqual(history["ncfd"]["current"], 49.25)


if __name__ == "__main__":
    unittest.main()
