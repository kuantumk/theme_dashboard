"""Liquidity floor shared by the darvas, gamma, htf and topdog screeners.

Not a screener: the workflow imports only the names listed in config
`screeners`. Each screener keeps its own ADR% line, because the four do not
agree on it (darvas and gamma use `>=`, htf and topdog `>`).
"""


def base_liquidity(master_df):
    """$10M avg dollar volume, 500k shares today, 1M 50-day average, price >= $1."""
    return (
        (master_df['avg_dollar_vol'] >= 10.0e6) &
        (master_df['volume'] >= 500e3) &
        (master_df['vol_sma50'] >= 1.0e6) &
        (master_df['close'] >= 1.0)
    )
