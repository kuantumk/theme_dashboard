"""
Run stock screeners against the master tables.

All screeners run in one process: each master parquet is read once and every
screener is applied to the same zero-filled frame. Each screener writes its
passing rows to ``<screener>/<screener>_<date>.parquet``.

    # every screener in CONFIG['screeners'], newest 130 sessions (the workflow)
    uv run python src/screening/run_screener.py --all --days 130
    # one or more named screeners
    uv run python src/screening/run_screener.py --screener steady_trend --days 1
    # check one ticker against the newest master table
    uv run python src/screening/run_screener.py --screener steady_trend --test --ticker AAPL
"""

import argparse
import importlib
import re
from pathlib import Path

import src.stock_utils as su
from config.settings import CONFIG, SCREENING_OUTPUT_DIR

MASTER_NAME = re.compile(r'^master_(\d{4}-\d{2}-\d{2})\.parquet$')


def master_files_by_date(master_dir):
    """``{date_str: path}`` for every ``master_<YYYY-MM-DD>.parquet``, oldest first."""
    found = {}
    for path in Path(master_dir).glob('master_*.parquet'):
        match = MASTER_NAME.match(path.name)
        if match:
            found[match.group(1)] = path
    return dict(sorted(found.items()))


def newest_master_dates(num_days, master_dir=None):
    """The newest ``num_days`` master dates, newest first."""
    dates = list(master_files_by_date(master_dir or SCREENING_OUTPUT_DIR / 'master'))
    return dates[::-1][:num_days]


def load_master_table(date_str, master_dir=None):
    """One session's master table, zero-filled the way every screener sees it."""
    path = Path(master_dir or SCREENING_OUTPUT_DIR / 'master') / f'master_{date_str}.parquet'
    return su.load_df_from_parquet(path).fillna(0)


def load_screeners(names):
    return {name: importlib.import_module(f'src.screening.screeners.{name}') for name in names}


def screen_master(filled_master, screeners):
    """``{name: passing rows}`` for one zero-filled master frame.

    Each screener gets its own copy, so one that adds columns to the frame
    it is handed (``coiled_theme`` does) cannot change what the next one
    sees or writes, exactly as when each screener ran in its own process.
    """
    results = {}
    for name, module in screeners.items():
        frame = filled_master.copy()
        results[name] = frame[module.filter_master_table(frame)]
    return results


def run_screeners(names, num_days, output_root=None, master_dir=None):
    """Screen the newest ``num_days`` master tables with every screener in ``names``.

    Returns the session dates screened, newest first.
    """
    output_root = Path(output_root or SCREENING_OUTPUT_DIR)
    screeners = load_screeners(names)
    dates = newest_master_dates(num_days, master_dir)
    if len(dates) < num_days:
        print(f"Warning: {num_days} sessions requested, {len(dates)} master tables found")
    for date_str in dates:
        master_df = load_master_table(date_str, master_dir)
        for name, rows in screen_master(master_df, screeners).items():
            su.save_df_to_parquet(rows, output_root / name / f'{name}_{date_str}.parquet')
        print(f"Screened {date_str}", flush=True)
    return dates


def _test_ticker(screener_name, ticker):
    newest = newest_master_dates(1)
    if not newest:
        print("No master tables found")
        return
    master_df = load_master_table(newest[0])
    row = master_df[master_df['ticker'] == ticker.upper()]
    if row.empty:
        print(f"{ticker.upper()} is not in the newest master table")
        return
    screener = load_screeners([screener_name])[screener_name]
    passed = bool(screener.filter_master_table(row).iloc[0])
    print(f"{ticker.upper()} {'PASSES' if passed else 'fails'} {screener_name} "
          f"on {row['date'].iloc[0]}")


def main(argv=None):
    parser = argparse.ArgumentParser(description='Run screeners against the master tables.')
    which = parser.add_mutually_exclusive_group(required=True)
    which.add_argument('--screener', '-s', nargs='+', help='Screener name(s).')
    which.add_argument('--all', action='store_true',
                       help="Every screener listed in config `screeners`.")
    parser.add_argument('--days', '-d', type=int, default=1, help='Number of run days.')
    parser.add_argument(
        '--test', action='store_true',
        help="Check one --ticker against the newest master table and print whether it passes.")
    parser.add_argument('--ticker', '-t', type=str, default='', help='Testing ticker.')
    args = parser.parse_args(argv)

    names = list(CONFIG['screeners']) if args.all else args.screener
    if args.test:
        if not args.ticker or len(names) != 1:
            parser.error('--test needs one --screener and a --ticker')
        _test_ticker(names[0], args.ticker)
        return
    run_screeners(names, args.days)


if __name__ == '__main__':
    main()
