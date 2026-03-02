"""
Run a stock screener against the master table.
"""

import argparse
import importlib
import pandas as pd
import datetime as dt

import src.stock_utils as su
from config.settings import SCREENING_OUTPUT_DIR


parser = argparse.ArgumentParser(
    description='Screener name and number of run days for screeners.')
parser.add_argument(
    '--screener', '-s', type=str, default='lolr', help='Screener name.')
parser.add_argument(
    '--days', '-d', type=int, default=1, help='Number of run days.')
parser.add_argument(
    '--test', action='store_true', help='Condition testing.')
parser.add_argument(
    '--ticker', '-t', type=str, default='', help='Testing ticker.')
args = parser.parse_args()


def load_master_table(offset_days):
    if args.test:
        master_file = su.get_latest_file(SCREENING_OUTPUT_DIR / 'master_test', 'master_*.csv', 1)
        master_df = pd.read_csv(master_file).fillna(0)
        master_df = master_df[master_df['ticker'] == args.ticker]
    else:
        file_index = offset_days + 1
        master_file = su.get_latest_file(SCREENING_OUTPUT_DIR / 'master', 'master_*.csv', file_index)
        master_df = pd.read_csv(master_file).fillna(0)

    return master_df


if __name__ == '__main__':
    num_run_days = args.days
    screener_name = args.screener
    output_dir = SCREENING_OUTPUT_DIR / screener_name
    output_dir.mkdir(exist_ok=True)
    consolidated_dir = SCREENING_OUTPUT_DIR / 'consolidated'
    consolidated_dir.mkdir(exist_ok=True)

    screener_module = f'src.screening.screeners.{screener_name}'
    screener = importlib.import_module(screener_module)

    for offset_days in range(num_run_days):
        master_df = load_master_table(offset_days)
        filter_conditions = screener.filter_master_table(master_df)
        if args.test:
            print(filter_conditions)
            exit()

        filtered_master_df = master_df[filter_conditions]

        output_date = master_df['date'].values[0]
        filtered_master_df.to_csv(output_dir / f'{screener_name}_{output_date}.csv', index=False)

        txt_date = dt.datetime.strptime(output_date, '%Y-%m-%d').strftime('%m%d%Y')
        output_tickers = filtered_master_df['ticker']
        pd.DataFrame(output_tickers).to_csv(output_dir / f'{screener_name}_{txt_date}.txt', index=False, header=False)
        pd.DataFrame(output_tickers).to_csv(SCREENING_OUTPUT_DIR / 'consolidated' / f'_{screener_name}_{txt_date}.txt', index=False, header=False)
