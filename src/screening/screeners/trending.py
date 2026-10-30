# screener name: trending
# description: liquid high-ADR names in a 6-session run of higher lows (up) or lower highs (down)

from config.settings import CONFIG

# Defaults double as documentation; override in workflow_config.yaml `trending_screener:`.
_DEFAULTS = {
    'min_adr_pct': 0.04,
    'min_avg_dollar_vol': 100e6,
    'min_vol_sma50': 800e3,
    'min_streak': 6,
}


def _gates():
    cfg = CONFIG.get('trending_screener') or {}
    return {k: float(cfg.get(k, v)) for k, v in _DEFAULTS.items()}


def trend_sides(master_df):
    """``(up, down)`` masks. A row can be both: six higher lows and six lower
    highs is a contracting range, and it is listed on each side."""
    n = _gates()['min_streak']
    return (master_df['higher_low_streak'] >= n,
            master_df['lower_high_streak'] >= n)


def filter_master_table(master_df):
    g = _gates()
    up, down = trend_sides(master_df)
    return (
        (master_df['adr_pct_partial'] >= g['min_adr_pct']) &
        (master_df['avg_dollar_vol_partial'] >= g['min_avg_dollar_vol']) &
        (master_df['vol_sma50_partial'] >= g['min_vol_sma50']) &
        (up | down)
    )
