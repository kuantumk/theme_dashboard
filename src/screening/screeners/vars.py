# screener name: vars
# description: Volatility-Adjusted Relative Strength leaders, VARS > 2

from config.settings import CONFIG

# Defaults double as documentation; override in workflow_config.yaml `vars_screener:`.
_DEFAULTS = {
    'min_avg_dollar_vol': 40e6,
    'min_vol_sma50': 1e6,
    'min_close': 2.0,
    'min_adr_pct': 0.02,
    'min_vars': 2.0,
}


def _gates():
    cfg = CONFIG.get('vars_screener') or {}
    return {k: float(cfg.get(k, v)) for k, v in _DEFAULTS.items()}


def filter_master_table(master_df):
    g = _gates()
    return (
        (master_df['avg_dollar_vol'] >= g['min_avg_dollar_vol']) &
        (master_df['vol_sma50'] >= g['min_vol_sma50']) &
        (master_df['close'] > g['min_close']) &
        (master_df['adr_pct'] >= g['min_adr_pct']) &
        (master_df['vars'] > g['min_vars'])
    )
