import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.reporting import export_dashboard_data


class VarsArtifactTests(unittest.TestCase):
    def test_write_vars_artifact_uses_report_date_filename(self) -> None:
        snapshot = {
            "report_date": "2026-05-08",
            "themes": [
                {
                    "name": "AI - Data Center",
                    "avg_vars": 7.5,
                    "tickers": [{"ticker": "NVDA", "vars": 8.0}],
                }
            ],
        }

        with tempfile.TemporaryDirectory() as tmp:
            out = export_dashboard_data.write_vars_artifact(snapshot, Path(tmp))

            self.assertEqual(out.name, "vars_2026-05-08.json")
            self.assertEqual(json.loads(out.read_text(encoding="utf-8")), snapshot)

    def test_export_vars_writes_current_snapshot_artifact(self) -> None:
        latest = {
            "report_date": "2026-05-08",
            "themes": [
                {
                    "name": "AI - Data Center",
                    "avg_vars": 7.5,
                    "tickers": [{"ticker": "NVDA", "vars": 8.0}],
                }
            ],
        }
        previous = {
            "report_date": "2026-05-07",
            "themes": [
                {
                    "name": "Nuclear Energy",
                    "avg_vars": 6.5,
                    "tickers": [{"ticker": "CEG", "vars": 7.0}],
                }
            ],
        }

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            screening_dir = root / "screening_output"
            vars_dir = screening_dir / "vars"
            vars_dir.mkdir(parents=True)
            (vars_dir / "vars_2026-05-07.parquet").write_text("placeholder", encoding="utf-8")
            (vars_dir / "vars_2026-05-08.parquet").write_text("placeholder", encoding="utf-8")

            output_dir = root / "docs" / "data"
            output_dir.mkdir(parents=True)
            artifact_dir = root / "artifacts" / "vars"

            snapshots = {
                "vars_2026-05-08.parquet": latest,
                "vars_2026-05-07.parquet": previous,
            }

            def fake_build_snapshot(csv_file, day_flags):
                return snapshots[csv_file.name]

            with (
                patch.object(export_dashboard_data, "SCREENING_OUTPUT_DIR", screening_dir),
                patch.object(export_dashboard_data, "OUTPUT_DIR", output_dir),
                patch.object(export_dashboard_data, "VARS_ARTIFACT_DIR", artifact_dir),
                patch.object(export_dashboard_data, "_build_vars_snapshot", fake_build_snapshot),
            ):
                current = export_dashboard_data.export_vars(day_flags={})

            self.assertEqual(current, latest)
            self.assertEqual(
                json.loads((output_dir / "vars.json").read_text(encoding="utf-8")),
                latest,
            )
            self.assertEqual(
                json.loads((output_dir / "vars_history.json").read_text(encoding="utf-8")),
                [latest, previous],
            )
            self.assertEqual(
                json.loads((artifact_dir / "vars_2026-05-08.json").read_text(encoding="utf-8")),
                latest,
            )


if __name__ == "__main__":
    unittest.main()
