"""NCFD/MMFI/MMTW/MMTH histories hold one reading per session.

winner_research's backtests read these histories. Two scrapers (the step-3
Selenium file and the export's Playwright fetch) both used to append, and
they round differently, so every run wrote a near-duplicate pair.
"""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import src.reporting.export_dashboard_data as edd


def run(tmp, existing, latest, barchart, session):
    out = Path(tmp)
    history_file = out / "market_breadth.json"
    history_file.write_text(json.dumps(existing), encoding="utf-8")
    latest_file = out / "market_breadth_latest.json"
    latest_file.write_text(json.dumps(latest), encoding="utf-8")
    with patch.object(edd, "OUTPUT_DIR", out), \
         patch.object(edd, "BREADTH_HISTORY_FILE", history_file), \
         patch.object(edd, "BREADTH_FILE", latest_file), \
         patch.object(edd, "fetch_barchart_breadth", return_value=barchart), \
         patch.object(edd, "fetch_cnn_fear_greed", return_value=None), \
         patch.object(edd, "fetch_naaim_exposure", return_value=None), \
         patch.object(edd, "fetch_aaii_sentiment", return_value=None):
        edd.update_breadth_history(session)
    return json.loads(history_file.read_text(encoding="utf-8"))


class BreadthHistoryTests(unittest.TestCase):
    def test_two_scrapers_add_one_reading(self):
        existing = {"mmfi": {"current": 30.0, "history": [30.0]}}
        with tempfile.TemporaryDirectory() as tmp:
            out = run(tmp, existing, {"mmfi": 33.2}, {"mmfi": 33.21}, "2026-10-27")
        self.assertEqual(out["mmfi"]["history"], [30.0, 33.21])
        self.assertEqual(out["mmfi"]["current"], 33.21)

    def test_the_step3_file_fills_what_barchart_missed(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = run(tmp, {}, {"ncfd": 49.25}, {"ncfd": None, "mmfi": 40.0}, "2026-10-27")
        self.assertEqual(out["ncfd"]["history"], [49.25])
        self.assertEqual(out["mmfi"]["history"], [40.0])

    def test_a_rerun_of_the_same_session_replaces_its_reading(self):
        with tempfile.TemporaryDirectory() as tmp:
            first = run(tmp, {"mmth": {"current": 40.0, "history": [40.0]}},
                        {}, {"mmth": 43.78}, "2026-10-27")
            second = run(tmp, first, {}, {"mmth": 43.8}, "2026-10-27")
        self.assertEqual(second["mmth"]["history"], [40.0, 43.8])

    def test_an_unchanged_reading_on_a_new_session_still_counts(self):
        existing = {"ncfd": {"current": 50.0, "history": [50.0], "as_of": "2026-10-26"}}
        with tempfile.TemporaryDirectory() as tmp:
            out = run(tmp, existing, {}, {"ncfd": 50.0}, "2026-10-27")
        self.assertEqual(out["ncfd"]["history"], [50.0, 50.0])



class HistoryFileTests(unittest.TestCase):
    def test_an_unreadable_history_is_left_alone_not_wiped(self):
        broken = '[{"report_date": "2026-10-24"}, <<<<<<< HEAD'
        with tempfile.TemporaryDirectory() as tmp:
            f = Path(tmp) / "si_history.json"
            f.write_text(broken, encoding="utf-8")
            edd._update_history_file(f, "2026-10-27", {"report_date": "2026-10-27"})
            self.assertEqual(f.read_text(encoding="utf-8"), broken)

    def test_a_write_leaves_no_temp_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            f = Path(tmp) / "si_history.json"
            edd._update_history_file(f, "2026-10-27", {"report_date": "2026-10-27"})
            self.assertEqual(json.loads(f.read_text(encoding="utf-8")),
                             [{"report_date": "2026-10-27"}])
            self.assertEqual(sorted(p.name for p in Path(tmp).iterdir()), ["si_history.json"])

if __name__ == "__main__":
    unittest.main()
