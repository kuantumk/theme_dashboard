import tempfile
import unittest
from pathlib import Path

import pandas as pd

import src.stock_utils as su
from src.screening.prune_screening_output import prune_screening_output


def _touch_parquet(path: Path) -> None:
    su.save_df_to_parquet(pd.DataFrame({"ticker": ["AAA"]}), path)


class PruneScreeningOutputTests(unittest.TestCase):
    def _make_tree(self, root: Path, subdir: str, dates) -> Path:
        d = root / subdir
        d.mkdir(parents=True, exist_ok=True)
        for ds in dates:
            _touch_parquet(d / f"{subdir}_{ds}.parquet")
        return d

    def test_keeps_newest_and_deletes_older(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            dates = [f"2026-05-{d:02d}" for d in range(1, 26)]  # 25 dates
            d = self._make_tree(root, "master", dates)

            deleted = prune_screening_output(root=root, keep=10)

            self.assertEqual(deleted, 15)
            kept = sorted(p.name for p in d.glob("*.parquet"))
            self.assertEqual(len(kept), 10)
            self.assertIn("master_2026-05-25.parquet", kept)  # newest kept
            self.assertIn("master_2026-05-16.parquet", kept)  # boundary kept
            self.assertNotIn("master_2026-05-15.parquet", kept)  # older dropped

    def test_keep_ge_count_is_noop(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._make_tree(root, "topdog", ["2026-05-01", "2026-05-02"])
            self.assertEqual(prune_screening_output(root=root, keep=10), 0)

    def test_keep_zero_deletes_all_dated(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            d = self._make_tree(root, "vars", ["2026-05-01", "2026-05-02"])
            self.assertEqual(prune_screening_output(root=root, keep=0), 2)
            self.assertEqual(list(d.glob("*.parquet")), [])

    def test_non_dated_and_non_parquet_files_untouched(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            d = root / "master"
            d.mkdir(parents=True)
            (d / ".gitkeep").write_text("")
            _touch_parquet(d / "notdated.parquet")  # no YYYY-MM-DD token
            for ds in [f"2026-05-{x:02d}" for x in range(1, 15)]:
                _touch_parquet(d / f"master_{ds}.parquet")

            prune_screening_output(root=root, keep=5)

            self.assertTrue((d / ".gitkeep").exists())
            self.assertTrue((d / "notdated.parquet").exists())
            self.assertEqual(
                len([p for p in d.glob("master_*.parquet")]), 5
            )

    def test_absent_root_is_noop(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(
                prune_screening_output(root=Path(tmp) / "nope", keep=5), 0
            )


if __name__ == "__main__":
    unittest.main()
