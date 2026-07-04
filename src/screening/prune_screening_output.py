"""Retention prune for screening_output/ (local scratch).

screening_output/ is regenerated in full every run (create_master_table.py
--days 130, run_screener.py --days 130) and read by export_dashboard_data to
rebuild docs/data/*.json; nothing in it is committed. This prune keeps only the
newest N per-day parquet files per subdir so the local working tree stops
accumulating.

Ordering contract: this MUST run only AFTER export_all() has consumed the full
regenerated window — it is called at the tail of
export_dashboard_data.export_all(), never before export. Pruning earlier would
truncate the history the exporter reads.
"""
import re
from pathlib import Path

from config.settings import CONFIG, SCREENING_OUTPUT_DIR

# per-day filename token: <name>_YYYY-MM-DD.parquet
_DATE_RE = re.compile(r'_(\d{4}-\d{2}-\d{2})\.parquet$')


def _retention_default():
    return CONFIG.get('screening_output', {}).get('retention_sessions', 10)


def prune_screening_output(root=SCREENING_OUTPUT_DIR, keep=None):
    """Keep only the newest ``keep`` per-day parquet files in each subdir of
    ``root``; delete older ones. Returns the count deleted.

    ``keep`` defaults to config ``screening_output.retention_sessions`` (10).
    Safe no-op on an empty/absent tree; only touches
    ``<name>_YYYY-MM-DD.parquet`` files (never .gitkeep or other artifacts).
    """
    root = Path(root)
    if keep is None:
        keep = _retention_default()
    if not root.is_dir():
        return 0

    deleted = 0
    for subdir in sorted(root.iterdir()):
        if not subdir.is_dir():
            continue
        dated = [
            (m.group(1), p)
            for p in subdir.glob('*.parquet')
            if (m := _DATE_RE.search(p.name))
        ]
        if len(dated) <= keep:
            continue
        dated.sort(key=lambda t: t[0], reverse=True)  # newest date first
        for _, path in dated[keep:]:
            try:
                path.unlink()
                deleted += 1
            except OSError:
                pass
    return deleted


if __name__ == '__main__':
    n = prune_screening_output()
    print(f"Pruned {n} old screening_output parquet file(s)")
