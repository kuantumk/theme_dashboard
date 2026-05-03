# screener name: parabolic
# description: parabolic short setup with no-overlap session gap and volume expansion

import pandas as pd


def _numeric_col(master_df, column):
    if column not in master_df.columns:
        return pd.Series(float('nan'), index=master_df.index)
    return pd.to_numeric(master_df[column], errors='coerce')


def _atr_multi_50sma(master_df):
    if 'atr_multi_50sma' in master_df.columns:
        return _numeric_col(master_df, 'atr_multi_50sma')

    close = _numeric_col(master_df, 'close')
    sma50 = _numeric_col(master_df, 'sma50')
    if 'atr_pct' in master_df.columns:
        atr_pct = _numeric_col(master_df, 'atr_pct')
    else:
        atr_pct = _numeric_col(master_df, 'atr14') / close

    return (close / sma50 - 1) / atr_pct


def filter_master_table(master_df):
    close = _numeric_col(master_df, 'close')
    high = _numeric_col(master_df, 'high')
    low = _numeric_col(master_df, 'low')
    volume = _numeric_col(master_df, 'volume')
    avg_dollar_vol = _numeric_col(master_df, 'avg_dollar_vol')
    adr_pct = _numeric_col(master_df, 'adr_pct')
    atr_multi = _atr_multi_50sma(master_df)
    previous_session_high = _numeric_col(master_df, 'previous_session_high')
    previous_session_volume = _numeric_col(master_df, 'previous_session_volume')

    filter_conditions = (
        # liquidity
        (avg_dollar_vol >= 10.0e6) &

        # ADR %
        (adr_pct >= 0.04) &

        # price
        (close >= 5.0) &

        # extended from 50 SMA in ATR units
        (atr_multi >= 10) &

        # current session candle does not overlap the previous session candle
        (low >= previous_session_high) &
        (high > low) &

        # volume expansion vs prior session
        (volume > previous_session_volume)
    )

    return filter_conditions
