"""Tests for tools/audit_theme_tags.py (mechanical tag audit).

The script is not a package module, so it is loaded via importlib from its
file path. main() is exercised with explicit --themes-file/--union-file
overrides against temp files; the taxonomy is the real repo taxonomy.
"""
import importlib.util
import io
import json
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory

ROOT = Path(__file__).resolve().parents[1]


def _load_audit_module():
    spec = importlib.util.spec_from_file_location(
        "audit_theme_tags", ROOT / "tools" / "audit_theme_tags.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


audit = _load_audit_module()


def _write_union(path: Path, date: str, tickers) -> None:
    path.write_text(json.dumps({"date": date, "tickers": tickers}), encoding="utf-8")


class LoadScreenedUnionTests(unittest.TestCase):
    def test_normalizes_and_sorts_tickers(self) -> None:
        with TemporaryDirectory() as tmp:
            union = Path(tmp) / "screened_union.json"
            _write_union(union, "2026-01-02", ["nvda", "", " TSLA ", "NVDA"])
            date, tickers = audit.load_screened_union(union)
        self.assertEqual(date, "2026-01-02")
        self.assertEqual(tickers, ["NVDA", "TSLA"])

    def test_empty_union_yields_no_findings(self) -> None:
        with TemporaryDirectory() as tmp:
            union = Path(tmp) / "screened_union.json"
            _write_union(union, "2026-01-02", [])
            _, tickers = audit.load_screened_union(union)
            self.assertEqual(tickers, [])
            self.assertEqual(audit.check_untagged_screened({}, []), [])


class CheckUntaggedScreenedTests(unittest.TestCase):
    def test_flags_missing_and_uncategorized_only_not_singleton(self) -> None:
        themes = {
            "UNC": ["Uncategorized"],
            "SGL": ["Singleton"],
            "CAN": ["Space / Launch"],
        }
        union = ["UNC", "SGL", "CAN", "NEW"]
        self.assertEqual(audit.check_untagged_screened(themes, union), ["NEW", "UNC"])


class MainExitCodeTests(unittest.TestCase):
    def _run_main(self, themes: dict, union_tickers) -> tuple[int, str]:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            themes_file = tmp_path / "ticker_themes.json"
            themes_file.write_text(json.dumps(themes), encoding="utf-8")
            union_file = tmp_path / "screened_union.json"
            _write_union(union_file, "2026-01-02", union_tickers)

            buffer = io.StringIO()
            with redirect_stdout(buffer):
                code = audit.main(
                    [
                        "--themes-file",
                        str(themes_file),
                        "--union-file",
                        str(union_file),
                    ]
                )
        return code, buffer.getvalue()

    def test_untagged_alone_exits_zero(self) -> None:
        code, output = self._run_main(
            {"SGL": ["Singleton"], "CAN": ["Space / Launch"]},
            ["NEW", "CAN", "SGL"],
        )
        self.assertEqual(code, 0)
        self.assertIn("[UNTAGGED] Screened tickers awaiting classification: 1", output)
        self.assertIn("NEW", output)

    def test_bug_still_exits_one(self) -> None:
        # Bare-L1 path for an L1 with children (Space) is a [BUG].
        code, output = self._run_main(
            {"BARE": ["Space"], "CAN": ["Space / Launch"]},
            ["CAN"],
        )
        self.assertEqual(code, 1)
        self.assertIn("[BUG]", output)

    def test_missing_union_file_skips_check_and_exits_zero(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            themes_file = tmp_path / "ticker_themes.json"
            themes_file.write_text(
                json.dumps({"CAN": ["Space / Launch"]}), encoding="utf-8"
            )

            buffer = io.StringIO()
            with redirect_stdout(buffer):
                code = audit.main(
                    [
                        "--themes-file",
                        str(themes_file),
                        "--union-file",
                        str(tmp_path / "nonexistent.json"),
                    ]
                )

        self.assertEqual(code, 0)
        self.assertIn("No screened_union.json found", buffer.getvalue())


if __name__ == "__main__":
    unittest.main()
