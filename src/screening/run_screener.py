"""
Run a stock screener against the master table.
"""

import argparse
import importlib

import src.stock_utils as su
from config.settings import SCREENING_OUTPUT_DIR


parser = argparse.ArgumentParser(
    description='Screener name and number of run days for screeners.')
parser.add_argument(
    '--screener', '-s', type=str, required=True, help='Screener name.')
parser.add_argument(
    '--days', '-d', type=int, default=1, help='Number of run days.')
parser.add_argument(
    '--test', action='store_true',
    help="Check one --ticker against the newest master table and print whether it passes.")
parser.add_argument(
    '--ticker', '-t', type=str, default='', help='Testing ticker.')
args = parser.parse_args()


def load_master_table(offset_days):
    file_index = offset_days + 1
    master_file = su.get_latest_file(SCREENING_OUTPUT_DIR / 'master', 'master_*.parquet', file_index)
    return su.load_df_from_parquet(master_file).fillna(0)


if __name__ == '__main__':
    num_run_days = args.days
    screener_name = args.screener
    output_dir = SCREENING_OUTPUT_DIR / screener_name

    screener_module = f'src.screening.screeners.{screener_name}'
    screener = importlib.import_module(screener_module)

    if args.test:
        if not args.ticker:
            parser.error('--test needs --ticker')
        master_df = load_master_table(0)
        row = master_df[master_df['ticker'] == args.ticker.upper()]
        if row.empty:
            print(f"{args.ticker.upper()} is not in the newest master table")
        else:
            passed = bool(screener.filter_master_table(row).iloc[0])
            print(f"{args.ticker.upper()} {'PASSES' if passed else 'fails'} {screener_name} "
                  f"on {row['date'].iloc[0]}")
        raise SystemExit(0)

    for offset_days in range(num_run_days):
        master_df = load_master_table(offset_days)
        filter_conditions = screener.filter_master_table(master_df)

        filtered_master_df = master_df[filter_conditions]

        output_date = master_df['date'].values[0]
        su.save_df_to_parquet(filtered_master_df, output_dir / f'{screener_name}_{output_date}.parquet')
