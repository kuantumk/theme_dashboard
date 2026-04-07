"""
Create master table with RS_STS% and ranking metrics.
"""

import sys
import traceback

try:
    import argparse
    import pandas as pd
    from tqdm import tqdm
    import warnings
    warnings.filterwarnings("ignore")

    import src.stock_utils as su
    from config.settings import PRICE_DATA_TA_FILE, SCREENING_OUTPUT_DIR
    from src.indicators.calculate_rs_score import calculate_rs_sts_for_tickers

except Exception as e:
    print(f"IMPORT ERROR: {e}", flush=True)
    traceback.print_exc()
    sys.exit(1)


parser = argparse.ArgumentParser(description='Create master table.')
parser.add_argument('--days', '-d', type=int, default=1, help='Number of run days.')
args = parser.parse_args()


def create_master_table(offset_days, daily_price, date_list):
    """Create master table with offset days."""
    run_date = date_list[-(offset_days + 1)]

    daily_tickers = daily_price.keys()

    df = daily_price['^GSPC'][:run_date].tail(1).copy()
    df['ticker'] = 'SPX'

    tickers = [t for t in daily_tickers if t not in ['SPY', 'MDY', 'QQQ', 'IWM', '^GSPC']]

    ticker_dfs = []

    for ticker in tqdm(tickers, desc=f"Processing tickers (Day {offset_days})"):
        if ticker in daily_price:
            ticker_df = daily_price[ticker][:run_date].tail(1).copy()
            if not ticker_df.empty:
                ticker_df['ticker'] = ticker
                ticker_dfs.append(ticker_df)

    if ticker_dfs:
        df = pd.concat([df] + ticker_dfs, ignore_index=True)

    # Volume spike rank
    df['vol_ratio'] = df['vol_sma40'] / df['vol_sma252']
    df['vol_ratio_rank'] = pd.qcut(df['vol_ratio'].rank(method='first'), 100, labels=False, duplicates='drop')

    # Relative performance rank
    months = [1, 3, 6]
    for month in months:
        if f'perf_{month}mo' in df.columns:
            df[f'perf_{month}mo_rank'] = pd.qcut(df[f'perf_{month}mo'].rank(method='first'), 100, labels=False, duplicates='drop')
        if f'rela_perf_{month}mo' in df.columns:
            df[f'rela_perf_{month}mo_rank'] = pd.qcut(df[f'rela_perf_{month}mo'].rank(method='first'), 100, labels=False, duplicates='drop')

    # Current price vs past low rank
    lookback_days = [30, 60, 90, 120]
    for lookback in lookback_days:
        if f'min{lookback}' in df.columns:
            df[f'c0_c{lookback}_rank'] = pd.qcut((df['close'] / df[f'min{lookback}']).rank(method='first'), 100, labels=False, duplicates='drop')

    # RS_STS% — filter price data to run_date to avoid lookahead bias
    print("Calculating RS_STS%...")
    price_data_as_of = {t: d[:run_date] for t, d in daily_price.items()}
    rs_sts = calculate_rs_sts_for_tickers(price_data_as_of)
    df['rs_sts_pct'] = df['ticker'].map(rs_sts).fillna(0)

    # Re-arrange columns
    cols_to_pop = ['ticker']
    for col_name in cols_to_pop:
        if col_name in df.columns:
            ticker_col = df.pop(col_name)
            df.insert(df.shape[1], col_name, ticker_col)

    run_date_str = run_date.strftime('%Y-%m-%d')
    print(f"Run date: {run_date_str}")

    df.insert(0, 'date', run_date_str)

    # Save
    output_dir = SCREENING_OUTPUT_DIR / 'master'
    output_dir.mkdir(exist_ok=True, parents=True)

    output_file = output_dir / f'master_{run_date_str}.csv'
    df.to_csv(output_file, index=False, date_format='%Y-%m-%d')
    print(f"OK Master table saved for {run_date_str} to {output_file}")


if __name__ == '__main__':
    num_run_days = args.days

    SCREENING_OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
    (SCREENING_OUTPUT_DIR / 'master').mkdir(exist_ok=True)

    print("Loading technical indicators...", flush=True)
    try:
        daily_price = su.load_object_from_pickle(PRICE_DATA_TA_FILE)
        date_list = daily_price['SPY'].index
    except FileNotFoundError:
        print(f"Error: Could not load {PRICE_DATA_TA_FILE}. Run create_technical_indicators.py first.")
        exit(1)
    except Exception as e:
        print(f"Error loading pickle: {e}")
        exit(1)

    for offset in range(num_run_days):
        print(f"Creating master table for offset {offset}...", flush=True)
        try:
            create_master_table(offset, daily_price, date_list)
        except Exception as e:
            print(f"Msg: ERROR creating master table for offset {offset}: {e}", flush=True)
            exit(1)
