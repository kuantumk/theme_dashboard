"""
Create master table with RS_STS% and ranking metrics.

One row per ticker per session: each ticker's last bar on or before the run
date (``frame[:run_date].tail(1)``), skipped when that bar is more than 7
calendar days old, plus an ``SPX`` row from ^GSPC. Then cross-sectional ranks
and RS_STS%, and one ``master/master_<date>.parquet`` per session.

``create_master_tables`` builds many sessions in one pass: each ticker's rows
for every requested session are taken once, and tickers that share a column
schema are stacked, so a session is a few takes rather than ~8,000 slices and
a concat of ~8,000 one-row frames. tests/test_master_table_parity.py pins
that the result equals the one-session-at-a-time construction value for
value, column for column.
"""

import argparse
import warnings

import pandas as pd

import src.stock_utils as su
from config.settings import PRICE_DATA_TA_FILE, SCREENING_OUTPUT_DIR
from src.indicators.calculate_rs_score import rs_sts_by_date

warnings.filterwarnings("ignore")

INDEX_TICKERS = ('SPY', 'MDY', 'QQQ', 'IWM', '^GSPC')
STALE_DAYS = 7  # a ticker's last bar must be within this many calendar days of the run date


def _schema(frame):
    return (tuple(frame.columns), tuple(frame.dtypes), frame.columns.dtype)


def _session_rows(daily_price, run_dates):
    """Yield ``(run_date, frame, tickers)`` per run date: the session's rows, ``SPX`` first.

    ``frame`` equals ``pd.concat`` of ``^GSPC[:run_date].tail(1)`` and every
    fresh ticker's ``frame[:run_date].tail(1)``, in ``daily_price`` order,
    with ``ignore_index=True``, less the ``ticker`` column; ``tickers`` names
    its rows. Only the stacked rows and one session's frame are held at once.
    """
    run_index = pd.DatetimeIndex(run_dates)
    groups = {}                           # schema -> [row frames]
    sizes = {}                            # schema -> rows stacked so far
    picks = {rd: [] for rd in run_dates}  # rd -> [(ticker, schema, row in group)]

    for ticker, frame in daily_price.items():
        if ticker in INDEX_TICKERS:
            continue
        index = frame.index
        if index.is_monotonic_increasing:
            pos = index.searchsorted(run_index, side='right') - 1
            has_row = pos >= 0
            if not has_row.any():
                continue
            fresh = has_row & ((run_index - index[pos.clip(0)]).days <= STALE_DAYS)
            if not fresh.any():
                continue
            rows = frame.iloc[pos[fresh]]
            row_dates = [rd for rd, ok in zip(run_dates, fresh) if ok]
        else:
            # Label slicing on an unsorted index behaves (or raises) its own
            # way; keep the original expression for such a frame.
            pieces, row_dates = [], []
            for rd in run_dates:
                row = frame[:rd].tail(1)
                if not row.empty and (rd - row.index[0]).days <= STALE_DAYS:
                    pieces.append(row)
                    row_dates.append(rd)
            if not pieces:
                continue
            rows = pd.concat(pieces)
        key = _schema(rows)
        base = sizes.get(key, 0)
        groups.setdefault(key, []).append(rows)
        sizes[key] = base + len(rows)
        for i, rd in enumerate(row_dates):
            picks[rd].append((ticker, key, base + i))

    stacked = {}
    for key in list(groups):
        stacked[key] = pd.concat(groups.pop(key), ignore_index=True)

    for rd in run_dates:
        spx = daily_price['^GSPC'][:rd].tail(1)
        chosen = picks[rd]
        group_order, members = [], {}
        for ticker, key, row in chosen:
            if key not in members:
                group_order.append(key)
                members[key] = []
            members[key].append(row)
        parts = [spx] + [stacked[key].iloc[members[key]] for key in group_order]
        df = pd.concat(parts, ignore_index=True) if chosen else spx.copy()
        if chosen:
            # Rows are grouped by schema; put them back in ticker order.
            at, start = {}, len(spx)
            for key in group_order:
                at[key] = start
                start += len(members[key])
            perm = list(range(len(spx)))
            for _ticker, key, _row in chosen:
                perm.append(at[key])
                at[key] += 1
            df = df.take(perm).reset_index(drop=True)
        yield rd, df, ['SPX'] * len(spx) + [ticker for ticker, _key, _row in chosen]


def _add_ranks(df):
    # Volume spike rank
    df['vol_ratio'] = df['vol_sma40'] / df['vol_sma252']
    df['vol_ratio_rank'] = pd.qcut(df['vol_ratio'].rank(method='first'), 100, labels=False)

    # Relative performance rank
    for month in [1, 3, 6]:
        if f'perf_{month}mo' in df.columns:
            df[f'perf_{month}mo_rank'] = pd.qcut(df[f'perf_{month}mo'].rank(method='first'), 100, labels=False)
        if f'rela_perf_{month}mo' in df.columns:
            df[f'rela_perf_{month}mo_rank'] = pd.qcut(df[f'rela_perf_{month}mo'].rank(method='first'), 100, labels=False)

    # Current price vs past low rank
    for lookback in [30, 60, 90, 120]:
        if f'min{lookback}' in df.columns:
            df[f'c0_c{lookback}_rank'] = pd.qcut((df['close'] / df[f'min{lookback}']).rank(method='first'), 100, labels=False)


def build_master_tables(daily_price, run_dates):
    """Yield ``(run_date_str, master_df)`` for each run date, in the order given."""
    # RS_STS% as of each run date (no look-ahead). No fillna: a ticker without
    # enough RS history stays NaN so downstream consumers can apply their own
    # neutral default (radar maps NaN -> 50).
    rs_sts = rs_sts_by_date(daily_price, run_dates)
    for rd, df, tickers in _session_rows(daily_price, run_dates):
        df['ticker'] = tickers
        _add_ranks(df)
        df['rs_sts_pct'] = df['ticker'].map(rs_sts[rd])

        # ticker last, date first
        ticker_col = df.pop('ticker')
        df.insert(df.shape[1], 'ticker', ticker_col)
        run_date_str = rd.strftime('%Y-%m-%d')
        df.insert(0, 'date', run_date_str)
        yield run_date_str, df


def create_master_tables(daily_price, date_list, offsets):
    """Build and save the master table for each offset (0 = newest session)."""
    run_dates = [date_list[-(offset + 1)] for offset in offsets]
    output_dir = SCREENING_OUTPUT_DIR / 'master'
    output_dir.mkdir(exist_ok=True, parents=True)
    for run_date_str, df in build_master_tables(daily_price, run_dates):
        output_file = output_dir / f'master_{run_date_str}.parquet'
        su.save_df_to_parquet(df, output_file)
        print(f"OK Master table saved for {run_date_str} to {output_file}", flush=True)


def create_master_table(offset_days, daily_price, date_list):
    """Build and save one session's master table (market_history calls this)."""
    create_master_tables(daily_price, date_list, [offset_days])


def main(argv=None):
    parser = argparse.ArgumentParser(description='Create master table.')
    parser.add_argument('--days', '-d', type=int, default=1, help='Number of run days.')
    args = parser.parse_args(argv)

    print("Loading technical indicators...", flush=True)
    daily_price = su.load_object_from_pickle(PRICE_DATA_TA_FILE)
    date_list = daily_price['SPY'].index
    create_master_tables(daily_price, date_list, range(args.days))


if __name__ == '__main__':
    main()
