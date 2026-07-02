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


class FindLatestUnionFileTests(unittest.TestCase):
    def test_picks_newest_by_parsed_date_not_lexical(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            (tmp_path / "_union_12312025.txt").write_text("OLD\n", encoding="utf-8")
            (tmp_path / "_union_01022026.txt").write_text("NEW\n", encoding="utf-8")
            (tmp_path / "_union_garbage.txt").write_text("X\n", encoding="utf-8")
            (tmp_path / "_volspike_01032026.txt").write_text("X\n", encoding="utf-8")

            latest = audit.find_latest_union_file(tmp_path)

        self.assertIsNotNone(latest)
        self.assertEqual(latest.name, "_union_01022026.txt")

    def test_returns_none_when_no_union_files(self) -> None:
        with TemporaryDirectory() as tmp:
            self.assertIsNone(audit.find_latest_union_file(Path(tmp)))

    def test_returns_none_for_missing_directory(self) -> None:
        self.assertIsNone(audit.find_latest_union_file(Path("does/not/exist")))


class UnionTickerTests(unittest.TestCase):
    def test_load_union_tickers_normalizes_and_sorts(self) -> None:
        with TemporaryDirectory() as tmp:
            union = Path(tmp) / "_union_01022026.txt"
            union.write_text("nvda\n\n TSLA \nNVDA\n", encoding="utf-8")
            self.assertEqual(audit.load_union_tickers(union), ["NVDA", "TSLA"])

    def test_empty_union_file_yields_no_findings(self) -> None:
        with TemporaryDirectory() as tmp:
            union = Path(tmp) / "_union_01022026.txt"
            union.write_text("", encoding="utf-8")
            self.assertEqual(audit.load_union_tickers(union), [])
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
    def _run_main(self, themes: dict, union_lines: str) -> tuple[int, str]:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            themes_file = tmp_path / "ticker_themes.json"
            themes_file.write_text(json.dumps(themes), encoding="utf-8")
            union_file = tmp_path / "_union_01022026.txt"
            union_file.write_text(union_lines, encoding="utf-8")

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
            "NEW\nCAN\nSGL\n",
        )
        self.assertEqual(code, 0)
        self.assertIn("[UNTAGGED] Screened tickers awaiting classification: 1", output)
        self.assertIn("NEW", output)

    def test_bug_still_exits_one(self) -> None:
        # Bare-L1 path for an L1 with children (Space) is a [BUG].
        code, output = self._run_main(
            {"BARE": ["Space"], "CAN": ["Space / Launch"]},
            "CAN\n",
        )
        self.assertEqual(code, 1)
        self.assertIn("[BUG]", output)

    def test_missing_union_file_skips_check_and_exits_zero(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            themes_file = tmp_path / "ticker_themes.json"
            themes_file.write_text(json.dumps({"CAN": ["Space / Launch"]}), encoding="utf-8")

            buffer = io.StringIO()
            with redirect_stdout(buffer):
                code = audit.main(
                    [
                        "--themes-file",
                        str(themes_file),
                        "--consolidated-dir",
                        str(tmp_path / "nonexistent"),
                    ]
                )

        self.assertEqual(code, 0)
        self.assertIn("No consolidated union file found", buffer.getvalue())


if __name__ == "__main__":
    unittest.main()
