# Zone Map Viz Tabs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the four Viz-tab networks (Theme, VARS, Momentum, Volume) with one shared zone map that plots each ticker by its distance from the daily EMA20 and the weekly EMA20, both in daily ATR14.

**Architecture:** A new `ema20_w` indicator column reaches the per-day master parquet. One Python helper, `build_zone_map`, turns a tab's ticker rows plus that session's unfilled master frame into a `zone_map` block. Each of the four snapshot builders writes the block into its current and history files. One browser renderer, `renderZoneMap`, draws plain SVG plus two HTML cards from that block. All colours come from CSS classes on existing tokens.

**Tech Stack:** Python 3.11, pandas, unittest (run with `uv run python -m unittest`); vanilla JS and SVG in `docs/app.js`; CSS in `docs/style.css`; no chart library.

**Spec:** `docs/superpowers/specs/2026-10-12-zone-map-viz-tabs-design.md`

## Deviations from the spec

The plan changes six details of the spec. Each change keeps the spec's intent. Tell the user about each one in the PR.

1. **`min_adr_pct: 0.04`, not `4.0`.** The `adr_pct` column is a fraction (0.0477 means 4.77%), and every other ADR floor in `config/workflow_config.yaml` is a fraction (`vars_screener.min_adr_pct: 0.02`).
2. **The distances come from the session's master parquet, never from a screener parquet.** `src/screening/run_screener.py:28-57` loads the master frame with `.fillna(0)` and saves the filtered rows. So a screener parquet already holds `ema20_w = 0` where the master holds NaN. Only the master parquet can tell "missing" from "zero".
3. **Colours are CSS classes, not `ZONE_COLORS` plus `getComputedStyle`.** The renderer emits classes such as `zm-band`, and `style.css` paints them with tokens. This is the dashboard's existing rule ("Colours live in `style.css`"), and `tests/test_dashboard_legends.py` already enforces it.
4. **Hover uses the native SVG `<title>` element.** It gives the ticker, both distances, the state and the row number with no tooltip positioning code. The network tooltip CSS is deleted with the networks.
5. **`VIZ_COLORS` is removed completely.** Dot labels use the grey `--text2` / `--text` tokens, and selection uses `--yellow` from CSS.
6. **The Theme Viz leaf list comes from the radar's own scored leaves.** It does not come from a second read of `ticker_themes.json`. The uncapped radar body already holds every scored leaf of every member.

## Global Constraints

- No new colour. Use only existing tokens: `--green`, `--amber`, `--red`, `--gdim`, `--adim2`, `--rdim`, `--tight`, `--yellow`, `--border2`, `--text`, `--text2`, `--text3`, `--bg`, `--bg3`, `--bg4`.
- No hex colour literal in `docs/app.js` outside the colour registry section.
- The zone distances never pass through `.fillna(0)`.
- A missing input publishes as `null`. A missing liquidity metric fails the floor (fail closed): the floor is a universe gate, not a view filter.
- `zone_map:` config values: `min_avg_dollar_vol: 100000000`, `min_adr_pct: 0.04`, `top_leaves: 20`, `weekly_ema_span: 20`, `weekly_min_weeks: 20`, `band_low: -0.5`, `band_high: 1.0`, `weekly_low: 0.0`, `weekly_high: 4.0`, `x_range: [-2.0, 5.0]`, `y_range: [-4.0, 8.0]`.
- Zone rules: daily `under` if `d < band_low`, `band` if `band_low <= d <= band_high`, `above` if `d > band_high`. Weekly `below` if `w < weekly_low`, `normal` if `weekly_low <= w <= weekly_high`, `extended` if `w > weekly_high`.
- Row number: `rs` (RS_STS%) on Momentum Viz; `vars` on Theme Viz, VARS Viz and Volume Viz.
- State notes say where a name sits. They never predict a direction or give advice.
- Code PRs do not commit `docs/data/*.json`. After any local export, run `git checkout -- docs/data/`.
- Comments and docs follow ASD-STE100 (short sentences, active voice), the same as the rest of the repo.
- Every task ends with `uv run python -m unittest discover -s tests` passing.

## Review Focus

These five inputs are the ones most likely to hurt a user that no test exercises by default. Each line names the task that adds its test.

1. **A value exactly on a limit** (`d_daily == -0.5`, `d_weekly == 4.0`) must land in the inclusive zone (`band`, `normal`). Task 2 pins every limit.
2. **A back-dated session whose parquet predates `ema20_w`** must publish `zone_map: null`, and the tab must say "Zone map data arrives with the next daily run" instead of drawing an empty chart. Task 2 pins the `null`; Task 5 pins the message.
3. **Bad bars** (`atr14 == 0`, `close <= 0`, NaN) must leave the name off and count it. A divide by zero must not pin the name to an edge. Task 2 pins it.
4. **A dual-tagged ticker** must draw one dot, list both leaves, and count once in each cluster row. Task 2 (helper) and Task 4 (radar) pin it.
5. **A NaN `tight_range`** must read as not tight, because `bool(float('nan'))` is True. Task 2 pins it.

---

## File Structure

| File | Responsibility | Tasks |
|---|---|---|
| `config/workflow_config.yaml` | New `zone_map:` block | 1 |
| `src/indicators/create_technical_indicators.py` | `compute_weekly_ema`, `weekly_ema_config`, the `ema20_w` column | 1 |
| `src/reporting/export_dashboard_data.py` | Zone helper section; the four builders write `zone_map`; `_build_network` deleted | 2, 3, 4 |
| `docs/app.js` | Registry (`ZONE_RULES`, `ZONE_STATES`, `ZONE_FAMILY_CLASS`); zone map section; wiring; network code deleted | 5, 6 |
| `docs/index.html` | Viz tab markup; cytoscape script deleted | 5, 6 |
| `docs/style.css` | Zone map CSS; network CSS deleted | 5, 6 |
| `docs/legends.js` | Zone map section and keys; network section deleted | 5, 6 |
| `tests/test_weekly_ema.py` | New | 1 |
| `tests/test_zone_map.py` | New: helper | 2 |
| `tests/test_zone_map_builders.py` | New: VARS, Momentum, Volume builders | 3 |
| `tests/test_zone_map_radar.py` | New: radar builder and history | 4 |
| `tests/test_dashboard_zone_map_markup.py` | New: browser mirror and markup | 5, 6 |
| `tests/test_vars_grouping.py` | Drop the network test | 3 |
| `tests/test_dashboard_legends.py` | Viz expectations follow the change | 5, 6 |
| `tests/test_dashboard_highlight_markup.py` | KD2 test points at the scatter | 5, 6 |
| `CLAUDE.md`, `CONCEPTS.md` | Docs | 7 |

---

### Task 1: The weekly EMA20 indicator

**Files:**
- Modify: `config/workflow_config.yaml` (add the block after the `vars_tab:` block)
- Modify: `src/indicators/create_technical_indicators.py:46` (new functions after `compute_sma50_full`), `:566` (config read), `:576` (new column)
- Test: `tests/test_weekly_ema.py`

**Interfaces:**
- Produces: `compute_weekly_ema(close: pd.Series, span: int = 20, min_weeks: int = 20) -> pd.Series` (same index as `close`, float, NaN before `min_weeks` completed weeks); `weekly_ema_config() -> dict` with keys `span`, `min_weeks`; master column `ema20_w`; config block `zone_map:`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_weekly_ema.py`:

```python
"""Weekly EMA20 for the zone map: a live weekly bar with no look-ahead.

Each daily bar reads the EMA of COMPLETED weeks, plus its own close as the
current week's bar. That is how a TradingView weekly chart reads mid-week.
"""

import unittest

import numpy as np
import pandas as pd

from src.indicators.create_technical_indicators import compute_weekly_ema

A = 2.0 / 21.0


def _closes(n=200, seed=7):
    # 2025-01-06 is a Monday, so bar i falls on weekday i % 5.
    idx = pd.bdate_range('2025-01-06', periods=n)
    rng = np.random.default_rng(seed)
    return pd.Series(100 * np.exp(np.cumsum(rng.normal(0, 0.02, n))), index=idx)


def _weekly_ema(close):
    weekly = close.groupby(close.index.to_period('W-FRI')).last()
    return weekly.ewm(span=20, adjust=False).mean()


class WeeklyEmaTests(unittest.TestCase):
    def test_each_bar_matches_the_series_cut_at_that_bar(self):
        close = _closes()
        full = compute_weekly_ema(close, span=20, min_weeks=20)
        for i in range(100, len(close), 3):
            cut = compute_weekly_ema(close.iloc[:i + 1], span=20, min_weeks=20)
            self.assertAlmostEqual(full.iloc[i], cut.iloc[-1], places=10, msg=f'bar {i}')

    def test_mid_week_value_blends_todays_close_with_last_weeks_ema(self):
        close = _closes()
        out = compute_weekly_ema(close, span=20, min_weeks=20)
        wema = _weekly_ema(close)
        i = 152  # a Wednesday
        week = close.index[i].to_period('W-FRI')
        prev = wema.iloc[wema.index.get_loc(week) - 1]
        self.assertAlmostEqual(out.iloc[i], A * close.iloc[i] + (1 - A) * prev, places=10)

    def test_the_last_bar_of_a_week_equals_that_weeks_ema(self):
        close = _closes()
        out = compute_weekly_ema(close, span=20, min_weeks=20)
        i = 154  # a Friday
        self.assertEqual(4, close.index[i].dayofweek)
        self.assertAlmostEqual(
            out.iloc[i], _weekly_ema(close).loc[close.index[i].to_period('W-FRI')], places=10)

    def test_the_value_starts_once_twenty_weeks_are_complete(self):
        close = _closes()
        out = compute_weekly_ema(close, span=20, min_weeks=20)
        self.assertTrue(np.isnan(out.iloc[99]), 'week 19 has only 19 completed weeks')
        self.assertTrue(np.isfinite(out.iloc[100]), 'week 20 has 20 completed weeks')

    def test_a_short_history_is_all_nan(self):
        out = compute_weekly_ema(_closes(n=99), span=20, min_weeks=20)
        self.assertTrue(out.isna().all())

    def test_a_holiday_friday_closes_the_week_on_thursday(self):
        close = _closes()
        friday, thursday, monday = close.index[154], close.index[153], close.index[155]
        holiday = close.drop(friday)
        out = compute_weekly_ema(holiday, span=20, min_weeks=20)
        wema = _weekly_ema(holiday)
        self.assertAlmostEqual(
            out.loc[monday],
            A * holiday.loc[monday] + (1 - A) * wema.loc[thursday.to_period('W-FRI')],
            places=10)

    def test_the_index_is_unchanged(self):
        close = _closes()
        out = compute_weekly_ema(close)
        self.assertTrue(out.index.equals(close.index))


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run python -m unittest tests.test_weekly_ema -v`
Expected: FAIL with `ImportError: cannot import name 'compute_weekly_ema'`.

- [ ] **Step 3: Add the config block**

In `config/workflow_config.yaml`, add this block directly after the `vars_tab:` block:

```yaml
# Zone map — the four Viz tabs (Theme, VARS, Momentum, Volume). Each ticker is
# plotted by (close - daily EMA20) / ATR14 across and (close - weekly EMA20) /
# ATR14 up. The server assigns every zone and state; docs/app.js ZONE_RULES
# mirrors the limits (pinned by tests/test_dashboard_zone_map_markup.py).
# The limits come from a reference chart. No forward return has been measured.
zone_map:
  min_avg_dollar_vol: 100000000   # 20-day avg dollar volume floor
  min_adr_pct: 0.04               # 20-day ADR floor, as a fraction (0.04 == 4%)
  top_leaves: 20                  # Theme Viz universe: members of the top N radar leaves
  weekly_ema_span: 20
  weekly_min_weeks: 20            # completed weeks before ema20_w has a value
  band_low: -0.5                  # daily band, in ATR14
  band_high: 1.0
  weekly_low: 0.0                 # weekly zones, in ATR14
  weekly_high: 4.0
  x_range: [-2.0, 5.0]            # fixed plot window, in ATR14
  y_range: [-4.0, 8.0]
```

- [ ] **Step 4: Write the implementation**

In `src/indicators/create_technical_indicators.py`, add after `compute_sma50_full` (line 56):

```python
def compute_weekly_ema(close, span=20, min_weeks=20):
    """Weekly EMA with a live weekly bar, as a daily series.

    Each bar groups into its week (`W-FRI`). The EMA runs over the last close
    of each COMPLETED week, then the bar's own close stands in for the
    current week: ``a * close + (1 - a) * ema[previous week]``. That is the
    value a TradingView weekly chart shows mid-week, and on the last bar of a
    week it equals that week's EMA.

    No look-ahead: a bar reads only completed weeks and its own close.

    Before `min_weeks` completed weeks exist the value is NaN. A listing
    younger than that has no weekly EMA, and the zone map leaves it off.
    """
    close = pd.to_numeric(close, errors='coerce')
    week = close.index.to_period('W-FRI')
    weekly_close = close.groupby(week).last()
    ema = weekly_close.ewm(span=span, adjust=False).mean()
    completed = pd.Series(np.arange(len(weekly_close)), index=weekly_close.index)
    prev = ema.shift(1).where(completed >= min_weeks)
    a = 2.0 / (span + 1)
    prev_daily = prev.reindex(week).to_numpy()
    return pd.Series(a * close.to_numpy() + (1 - a) * prev_daily, index=close.index)


def weekly_ema_config():
    """The weekly-EMA tunables from the `zone_map:` config block."""
    cfg = CONFIG.get('zone_map') or {}
    return {
        'span': int(cfg.get('weekly_ema_span', 20)),
        'min_weeks': int(cfg.get('weekly_min_weeks', 20)),
    }
```

In `calculate_technical_indicators`, after `_tight_cfg = tight_range_config()` (line 566), add:

```python
    # Weekly-EMA tunables for the zone map, read once.
    _weekly_cfg = weekly_ema_config()
```

After the line `daily['ema10'], daily['ema20'] = compute_ema_pair(daily['close'])` (line 576), add:

```python
            # Weekly EMA20 for the zone map. See compute_weekly_ema for the
            # live-bar rule that keeps it free of look-ahead.
            daily['ema20_w'] = compute_weekly_ema(daily['close'], **_weekly_cfg)
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run python -m unittest tests.test_weekly_ema -v`
Expected: 7 tests PASS.

Run: `uv run python -m unittest discover -s tests`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add config/workflow_config.yaml src/indicators/create_technical_indicators.py tests/test_weekly_ema.py
git commit -m "Add weekly EMA20 indicator for the zone map"
```

---

### Task 2: The zone-map helper

**Files:**
- Modify: `src/reporting/export_dashboard_data.py` (new section after `_highlight_from_row`, which ends at line 1350)
- Test: `tests/test_zone_map.py`

**Interfaces:**
- Consumes: master column `ema20_w` (Task 1); existing `_bars_by_ticker`, `_finite`, `_round_or_none`, `_int_or_none`, `CONFIG`, `SCREENING_OUTPUT_DIR`, `su`.
- Produces:
  - `ZONE_MAP_DEFAULTS: dict`, `ZONE_STATES: tuple[str, ...]` (7 ids, best first), `ZONE_MASTER_COLUMNS: tuple`, `ZONE_HIDDEN_LEAVES: set`
  - `zone_map_config() -> dict` (keys of `ZONE_MAP_DEFAULTS`)
  - `zone_distance(close, ma, atr) -> float | None` (2 decimals)
  - `classify_zone(d_daily, d_weekly, cfg) -> tuple[str, str, str]` = `(zone_daily, zone_weekly, state)`
  - `build_zone_map(rows, master_df, leaves_by_ticker, cfg=None) -> dict | None`. `rows` is an iterable of payload ticker dicts with keys `ticker`, `dollar_vol`, `adr_pct`, `vars`, `rs`, `highlight`, and optional `ticker_color`. Returns `None` when `master_df` is None or lacks `ema20_w`. Otherwise returns `{'params', 'n_candidates', 'n_floor_dropped', 'n_no_weekly', 'n_no_daily', 'tickers'}`. Each ticker carries `ticker, d_daily, d_weekly, zone_daily, zone_weekly, state, vars, rs, tight, tight_len, highlight, ticker_color, leaves`.
  - `_load_zone_master(date_str) -> pd.DataFrame | None` (reads `SCREENING_OUTPUT_DIR / 'master' / f'master_{date_str}.parquet'`, unfilled)

- [ ] **Step 1: Write the failing test**

Create `tests/test_zone_map.py`:

```python
"""The zone-map helper: floors, distances, zones and states.

The helper reads an UNFILLED master frame. A zero-filled weekly EMA would
put a name far up the extended zone with nothing on screen to show it, so
every missing input must leave the name off and be counted.
"""

import unittest

import pandas as pd

from src.reporting import export_dashboard_data as ex

CFG = dict(ex.ZONE_MAP_DEFAULTS)
NAN = float('nan')


def _master(rows):
    """rows: (ticker, close, ema20, ema20_w, atr14, tight_range)."""
    return pd.DataFrame([{
        'ticker': t, 'close': c, 'ema20': e, 'ema20_w': w, 'atr14': a,
        'tight_range': tr, 'tight_len': 5,
    } for t, c, e, w, a, tr in rows])


def _row(ticker, dollar_vol=150e6, adr=0.05, vars_=5.0, rs=80.0, highlight=None):
    return {'ticker': ticker, 'dollar_vol': dollar_vol, 'adr_pct': adr,
            'vars': vars_, 'rs': rs, 'highlight': highlight}


class ClassifyZoneTests(unittest.TestCase):
    CASES = [
        (0.0, 2.0, 'band', 'normal', 'buy_area'),
        (-0.5, 0.0, 'band', 'normal', 'buy_area'),      # both lower limits are inclusive
        (1.0, 4.0, 'band', 'normal', 'buy_area'),       # both upper limits are inclusive
        (0.0, 4.01, 'band', 'extended', 'buy_stretched'),
        (1.01, 2.0, 'above', 'normal', 'above'),
        (2.0, 5.0, 'above', 'extended', 'extended'),
        (-0.51, 2.0, 'under', 'normal', 'under_daily'),
        (-1.0, 6.0, 'under', 'extended', 'under_daily'),
        (0.0, -0.01, 'band', 'below', 'weekly_broken'),
        (3.0, -1.0, 'above', 'below', 'weekly_broken'),
        (-1.0, -1.0, 'under', 'below', 'under_both'),
    ]

    def test_each_cell_and_each_limit(self):
        for d, w, zd, zw, state in self.CASES:
            self.assertEqual((zd, zw, state), ex.classify_zone(d, w, CFG), f'({d}, {w})')

    def test_all_nine_cells_have_a_state_and_all_states_are_reachable(self):
        self.assertEqual(9, len(ex._ZONE_STATE_BY_CELL))
        self.assertEqual(set(ex.ZONE_STATES), set(ex._ZONE_STATE_BY_CELL.values()))
        self.assertEqual(7, len(ex.ZONE_STATES))


class ZoneDistanceTests(unittest.TestCase):
    def test_a_clean_bar(self):
        self.assertEqual(1.0, ex.zone_distance(12.0, 10.0, 2.0))

    def test_missing_or_non_positive_inputs_are_none(self):
        for args in [(None, 10, 1), (NAN, 10, 1), (10, NAN, 1), (10, 9, NAN),
                     (10, 0, 1), (10, 9, 0), (0, 9, 1), (-5, 9, 1)]:
            self.assertIsNone(ex.zone_distance(*args), args)


class BuildZoneMapTests(unittest.TestCase):
    def test_floors_drop_and_count_a_missing_metric_fails_closed(self):
        master = _master([(t, 100, 99, 95, 2, False) for t in 'ABCD'])
        rows = [_row('A'), _row('B', dollar_vol=99e6), _row('C', adr=0.039),
                _row('D', dollar_vol=None)]
        zm = ex.build_zone_map(rows, master, {}, CFG)
        self.assertEqual(['A'], [t['ticker'] for t in zm['tickers']])
        self.assertEqual(4, zm['n_candidates'])
        self.assertEqual(3, zm['n_floor_dropped'])

    def test_a_missing_weekly_ema_is_left_off_never_read_as_zero(self):
        master = _master([('A', 100, 99, 95, 2, False), ('B', 100, 99, NAN, 2, False)])
        zm = ex.build_zone_map([_row('A'), _row('B')], master, {}, CFG)
        self.assertEqual(['A'], [t['ticker'] for t in zm['tickers']])
        self.assertEqual(1, zm['n_no_weekly'])

    def test_a_bad_bar_counts_as_no_daily_reading(self):
        master = _master([('A', 100, 99, 95, 0.0, False)])
        zm = ex.build_zone_map([_row('A')], master, {}, CFG)
        self.assertEqual([], zm['tickers'])
        self.assertEqual(1, zm['n_no_daily'])

    def test_a_ticker_with_no_master_row_counts_as_no_weekly(self):
        master = _master([('A', 100, 99, 95, 2, False)])
        zm = ex.build_zone_map([_row('ZZZ')], master, {}, CFG)
        self.assertEqual(1, zm['n_no_weekly'])

    def test_a_master_without_the_column_publishes_none(self):
        master = _master([('A', 100, 99, 95, 2, False)]).drop(columns=['ema20_w'])
        self.assertIsNone(ex.build_zone_map([_row('A')], master, {}, CFG))
        self.assertIsNone(ex.build_zone_map([_row('A')], None, {}, CFG))

    def test_a_duplicate_row_makes_one_dot(self):
        master = _master([('A', 100, 99, 95, 2, False)])
        zm = ex.build_zone_map([_row('A'), _row('a')], master, {}, CFG)
        self.assertEqual(1, zm['n_candidates'])
        self.assertEqual(1, len(zm['tickers']))

    def test_leaves_keep_every_real_leaf_and_drop_catch_alls(self):
        master = _master([('A', 100, 99, 95, 2, False)])
        leaves = {'A': ['AI / Data Center / Optics', 'Cybersecurity / Network', 'Singleton']}
        zm = ex.build_zone_map([_row('A')], master, leaves, CFG)
        self.assertEqual(['AI / Data Center / Optics', 'Cybersecurity / Network'],
                         zm['tickers'][0]['leaves'])

    def test_a_nan_tight_flag_is_not_tight(self):
        master = _master([('A', 100, 99, 95, 2, NAN), ('B', 100, 99, 95, 2, True)])
        zm = ex.build_zone_map([_row('A'), _row('B')], master, {}, CFG)
        by = {t['ticker']: t for t in zm['tickers']}
        self.assertFalse(by['A']['tight'])
        self.assertIsNone(by['A']['tight_len'])
        self.assertTrue(by['B']['tight'])
        self.assertEqual(5, by['B']['tight_len'])

    def test_the_payload_shape(self):
        master = _master([('A', 100, 99, 95, 2, False)])
        row = dict(_row('A', highlight='ma_up'), ticker_color='green')
        zm = ex.build_zone_map([row], master, {}, CFG)
        self.assertEqual({'params', 'n_candidates', 'n_floor_dropped', 'n_no_weekly',
                          'n_no_daily', 'tickers'}, set(zm))
        t = zm['tickers'][0]
        self.assertEqual({'ticker', 'd_daily', 'd_weekly', 'zone_daily', 'zone_weekly',
                          'state', 'vars', 'rs', 'tight', 'tight_len', 'highlight',
                          'ticker_color', 'leaves'}, set(t))
        self.assertEqual((0.5, 2.5, 'buy_area'), (t['d_daily'], t['d_weekly'], t['state']))
        self.assertEqual(('ma_up', 'green'), (t['highlight'], t['ticker_color']))
        self.assertEqual(CFG['top_leaves'], zm['params']['top_leaves'])


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run python -m unittest tests.test_zone_map -v`
Expected: FAIL with `AttributeError: module ... has no attribute 'ZONE_MAP_DEFAULTS'`.

- [ ] **Step 3: Write the implementation**

In `src/reporting/export_dashboard_data.py`, insert after `_highlight_from_row` (it ends at line 1350, before `def _build_momentum_136_snapshot`):

```python
# ── Zone map (the four Viz tabs) ────────────────────────────────────────────
#
# Each ticker sits at (distance from the daily EMA20, distance from the weekly
# EMA20), both in daily ATR14. The server decides every zone and state; the
# browser only draws (`renderZoneMap` in docs/app.js, whose ZONE_RULES mirror
# is pinned to the `zone_map:` config block).
#
# ⛔ The distances read the session's MASTER parquet, unfilled. The screener
# parquets are saved from a frame that run_screener.py loads with
# `.fillna(0)`, so a screener row holds `ema20_w = 0` where the master holds
# NaN. A zero-filled weekly EMA gives `close / atr14`, a large positive
# number, and the name plots far up the extended zone with nothing on screen
# to show it.

ZONE_MAP_DEFAULTS = {
    'min_avg_dollar_vol': 100_000_000,
    'min_adr_pct': 0.04,
    'top_leaves': 20,
    'band_low': -0.5,
    'band_high': 1.0,
    'weekly_low': 0.0,
    'weekly_high': 4.0,
    'x_range': [-2.0, 5.0],
    'y_range': [-4.0, 8.0],
}

#: The seven states, best first. docs/app.js ZONE_STATES lists the same ids in
#: the same order (pinned by tests/test_dashboard_zone_map_markup.py).
ZONE_STATES = ('buy_area', 'buy_stretched', 'above', 'extended',
               'under_daily', 'weekly_broken', 'under_both')

#: Every (daily, weekly) cell maps to exactly one state.
_ZONE_STATE_BY_CELL = {
    ('band', 'normal'): 'buy_area',
    ('band', 'extended'): 'buy_stretched',
    ('above', 'normal'): 'above',
    ('above', 'extended'): 'extended',
    ('under', 'normal'): 'under_daily',
    ('under', 'extended'): 'under_daily',
    ('band', 'below'): 'weekly_broken',
    ('above', 'below'): 'weekly_broken',
    ('under', 'below'): 'under_both',
}

ZONE_MASTER_COLUMNS = ('close', 'ema20', 'ema20_w', 'atr14', 'tight_range', 'tight_len')
ZONE_HIDDEN_LEAVES = {'Uncategorized', 'Singleton'}


def zone_map_config():
    """The `zone_map:` config block, with the defaults filling any gap."""
    cfg = CONFIG.get('zone_map') or {}
    return {key: cfg.get(key, default) for key, default in ZONE_MAP_DEFAULTS.items()}


def zone_distance(close, ma, atr):
    """``(close - ma) / atr`` to 2 decimals, or None.

    A missing, non-finite or non-positive input returns None. A zero ATR
    must not become an infinite distance, and a zero average is a filled
    absence, not a price.
    """
    close, ma, atr = _finite(close), _finite(ma), _finite(atr)
    if close is None or ma is None or atr is None:
        return None
    if close <= 0 or ma <= 0 or atr <= 0:
        return None
    return round((close - ma) / atr, 2)


def classify_zone(d_daily, d_weekly, cfg):
    """Return ``(zone_daily, zone_weekly, state)``. All four limits are inclusive."""
    if d_daily < cfg['band_low']:
        daily = 'under'
    elif d_daily > cfg['band_high']:
        daily = 'above'
    else:
        daily = 'band'
    if d_weekly < cfg['weekly_low']:
        weekly = 'below'
    elif d_weekly > cfg['weekly_high']:
        weekly = 'extended'
    else:
        weekly = 'normal'
    return daily, weekly, _ZONE_STATE_BY_CELL[(daily, weekly)]


def _zone_flag(value):
    """True only for a real set flag. `bool(float('nan'))` is True."""
    number = _finite(value)
    return number is not None and number != 0


def _load_zone_master(date_str):
    """The session's master parquet, UNFILLED, or None when it is absent."""
    if not date_str:
        return None
    path = SCREENING_OUTPUT_DIR / 'master' / f'master_{date_str}.parquet'
    if not path.exists():
        return None
    return su.load_df_from_parquet(path)


def build_zone_map(rows, master_df, leaves_by_ticker, cfg=None):
    """Build one tab's `zone_map` block from its ticker rows.

    `rows` are the tab's payload ticker dicts. The floors read their
    `dollar_vol` and `adr_pct`; the display reads `vars`, `rs`,
    `highlight` and `ticker_color`. The distances and the tight flag read
    `master_df`, which must be the session's unfilled master frame.

    A missing liquidity metric FAILS the floor. The floor is a universe gate,
    like `si_gate`, not a view lens like the V/A cutoffs, so an unknown must
    not pass it.

    Returns None when the frame is absent or predates `ema20_w`: an empty map
    would read as a quiet market. Every name left off is counted.
    """
    cfg = cfg or zone_map_config()
    if master_df is None or 'ema20_w' not in master_df.columns:
        return None
    bars = _bars_by_ticker(master_df, ZONE_MASTER_COLUMNS)
    counts = {'n_candidates': 0, 'n_floor_dropped': 0, 'n_no_weekly': 0, 'n_no_daily': 0}
    seen = set()
    out = []
    for row in rows:
        ticker = str(row.get('ticker') or '').upper()
        if not ticker or ticker in seen:
            continue
        seen.add(ticker)
        counts['n_candidates'] += 1
        dollar_vol = _finite(row.get('dollar_vol'))
        adr = _finite(row.get('adr_pct'))
        if (dollar_vol is None or adr is None
                or dollar_vol < cfg['min_avg_dollar_vol'] or adr < cfg['min_adr_pct']):
            counts['n_floor_dropped'] += 1
            continue
        bar = bars.get(ticker) or {}
        if _finite(bar.get('ema20_w')) is None:
            counts['n_no_weekly'] += 1
            continue
        d_daily = zone_distance(bar.get('close'), bar.get('ema20'), bar.get('atr14'))
        d_weekly = zone_distance(bar.get('close'), bar.get('ema20_w'), bar.get('atr14'))
        if d_daily is None or d_weekly is None:
            counts['n_no_daily'] += 1
            continue
        zone_daily, zone_weekly, state = classify_zone(d_daily, d_weekly, cfg)
        tight = _zone_flag(bar.get('tight_range'))
        out.append({
            'ticker': ticker,
            'd_daily': d_daily,
            'd_weekly': d_weekly,
            'zone_daily': zone_daily,
            'zone_weekly': zone_weekly,
            'state': state,
            'vars': _round_or_none(row.get('vars'), 2),
            'rs': _round_or_none(row.get('rs'), 1),
            'tight': tight,
            'tight_len': _int_or_none(bar.get('tight_len')) if tight else None,
            'highlight': row.get('highlight'),
            'ticker_color': row.get('ticker_color'),
            'leaves': [p for p in (leaves_by_ticker.get(ticker) or [])
                       if p not in ZONE_HIDDEN_LEAVES],
        })
    out.sort(key=lambda t: t['ticker'])
    params = {key: cfg[key] for key in ZONE_MAP_DEFAULTS}
    return {'params': params, **counts, 'tickers': out}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run python -m unittest tests.test_zone_map -v`
Expected: all PASS.

Run: `uv run python -m unittest discover -s tests`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/reporting/export_dashboard_data.py tests/test_zone_map.py
git commit -m "Add the zone-map helper: floors, distances, zones and states"
```

---

### Task 3: VARS, Momentum and Volume builders write `zone_map`

**Files:**
- Modify: `src/reporting/export_dashboard_data.py` — `_build_network` (lines 55-95, delete), `_build_momentum_136_snapshot` (return at 1454-1458), `_build_vars_snapshot` (docstring 1529-1531, return at 1655-1662), `_build_volume_snapshot` (return at 2360-2364)
- Modify: `tests/test_vars_grouping.py:136-145` (delete the network test)
- Test: `tests/test_zone_map_builders.py`

**Interfaces:**
- Consumes: `build_zone_map`, `_load_zone_master` (Task 2).
- Produces: each of the three snapshots carries `zone_map` (dict or None) and no `network` key.

- [ ] **Step 1: Write the failing test**

Create `tests/test_zone_map_builders.py`:

```python
"""The VARS, Momentum and Volume builders publish one `zone_map` block each.

The fixture writes the screener parquet the way run_screener.py does — from
a zero-filled master frame — so the fillna trap is real here: the screener
row holds ema20_w = 0 while the master holds NaN.
"""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import src.stock_utils as su
from src.reporting import export_dashboard_data as ex

DATE = '2026-10-08'
TICKERS = ['AAA', 'BBB', 'CCC', 'DDD']
THEMES = {t: ['Cybersecurity / Network'] for t in TICKERS}


def _master():
    rows = []
    for i, t in enumerate(TICKERS):
        rows.append({
            'date': DATE, 'ticker': t, 'close': 100.0, 'high': 101.0, 'low': 98.0,
            'volume': 2_000_000, 'vol_sma50': 2_000_000,
            'ema10': 101.0, 'ema20': 99.0, 'ema20_w': 95.0, 'sma50_full': 90.0,
            'atr14': 2.0, 'avg_dollar_vol': 150e6, 'adr_pct': 0.05,
            'vars': 6.0 - i, 'vars_20ema': 5.0 - i, 'rs_sts_pct': 90.0 - i,
            'tight_range': False, 'tight_len': 0, 'tightness': 0.5,
            'days_since_highest_volume': 3, 'rela_perf_1mo_rank': 50,
        })
    df = pd.DataFrame(rows)
    df.loc[df['ticker'] == 'DDD', 'ema20_w'] = float('nan')   # a young listing
    return df


class ZoneMapBuilderTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = Path(self._tmp.name)
        master = _master()
        self.master_file = self.root / 'master' / f'master_{DATE}.parquet'
        files = {
            self.master_file: master,
            # run_screener.py saves rows from a zero-filled frame.
            self.root / 'vars' / f'vars_{DATE}.parquet': master.fillna(0),
            self.root / 'momentum_136' / f'momentum_136_{DATE}.parquet': master.fillna(0),
            self.root / 'volspike' / f'volspike_{DATE}.parquet': master.fillna(0),
        }
        for path, frame in files.items():
            path.parent.mkdir(parents=True, exist_ok=True)
            su.save_df_to_parquet(frame, path)
        for p in (
            patch.object(ex, 'SCREENING_OUTPUT_DIR', self.root),
            patch.object(ex, 'FUNDAMENTALS_DB', self.root / 'no-such.db'),
            patch('src.themes.theme_registry.load_ticker_themes', return_value=THEMES),
        ):
            p.start()
            self.addCleanup(p.stop)

    def _snaps(self):
        return {
            'vars': ex._build_vars_snapshot(self.root / 'vars' / f'vars_{DATE}.parquet', {}),
            'momentum': ex._build_momentum_136_snapshot(
                self.root / 'momentum_136' / f'momentum_136_{DATE}.parquet', {}),
            'volume': ex._build_volume_snapshot(DATE, {}),
        }

    def test_each_builder_publishes_the_map_and_no_network(self):
        for name, snap in self._snaps().items():
            self.assertNotIn('network', snap, name)
            zm = snap['zone_map']
            self.assertEqual({'AAA', 'BBB', 'CCC'}, {t['ticker'] for t in zm['tickers']}, name)

    def test_the_filled_screener_row_does_not_reach_the_map(self):
        for name, snap in self._snaps().items():
            self.assertEqual(1, snap['zone_map']['n_no_weekly'], name)

    def test_a_ticker_reads_its_state_numbers_and_leaf(self):
        for name, snap in self._snaps().items():
            aaa = next(t for t in snap['zone_map']['tickers'] if t['ticker'] == 'AAA')
            self.assertEqual(('band', 'normal', 'buy_area'),
                             (aaa['zone_daily'], aaa['zone_weekly'], aaa['state']), name)
            # The Momentum payload carries no `vars` field, so its map has none.
            expected_vars = None if name == 'momentum' else 6.0
            self.assertEqual((expected_vars, 90.0), (aaa['vars'], aaa['rs']), name)
            self.assertEqual(['Cybersecurity / Network'], aaa['leaves'], name)

    def test_a_missing_master_parquet_publishes_none(self):
        self.master_file.unlink()
        for name, snap in self._snaps().items():
            self.assertIsNone(snap['zone_map'], name)

    def test_all_three_blocks_share_one_shape(self):
        shapes = {name: set(snap['zone_map']) for name, snap in self._snaps().items()}
        self.assertEqual(1, len({frozenset(s) for s in shapes.values()}), shapes)


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run python -m unittest tests.test_zone_map_builders -v`
Expected: FAIL with `KeyError: 'zone_map'` and `AssertionError: 'network' unexpectedly found`.

- [ ] **Step 3: Write the implementation**

In `_build_momentum_136_snapshot`, replace the return (lines 1454-1458):

```python
    return {
        'report_date': csv_date,
        'themes': themes_list,
        'zone_map': build_zone_map(
            per_ticker.values(), _load_zone_master(csv_date), ticker_themes),
    }
```

In `_build_vars_snapshot`, replace the two lines of the docstring that read "The `network` payload stays leaf-level so the viz is unchanged." with:

```
    has >= 3 members. `zone_map` covers the tickers the tab renders.
```

Then replace lines 1655-1662 (the `shown_leaves` comment, the variable and the return):

```python
    # The zone map covers exactly the tickers this tab renders.
    rendered = [td for grp in l1_list for leaf in grp['leaves'] for td in leaf['tickers']]

    return {
        'report_date': csv_date,
        'themes': l1_list,
        'zone_map': build_zone_map(rendered, _load_zone_master(csv_date), ticker_themes),
    }
```

In `_build_volume_snapshot`, replace the return (lines 2360-2364):

```python
    return {
        'report_date': csv_date,
        'themes': themes_list,
        'zone_map': build_zone_map(
            per_ticker.values(), _load_zone_master(csv_date), ticker_themes),
    }
```

Delete `_build_network` (lines 55-95). Then confirm nothing calls it:

Run: `uv run python -c "import subprocess,sys; sys.exit(subprocess.call(['git','grep','-n','_build_network']))"`
Expected: no output, exit code 1.

In `tests/test_vars_grouping.py`, delete `test_network_stays_leaf_level_for_rendered_l1s_only` (starts at line 136; delete the whole method).

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run python -m unittest tests.test_zone_map_builders tests.test_vars_grouping -v`
Expected: all PASS.

Run: `uv run python -m unittest discover -s tests`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/reporting/export_dashboard_data.py tests/test_zone_map_builders.py tests/test_vars_grouping.py
git commit -m "Publish zone_map from the VARS, Momentum and Volume snapshots"
```

---

### Task 4: The radar builder writes the Theme Viz `zone_map`

**Files:**
- Modify: `src/reporting/export_dashboard_data.py` — `_build_radar_snapshot` (the leaf loop at lines 2089-2147)
- Test: `tests/test_zone_map_radar.py`

**Interfaces:**
- Consumes: `build_zone_map`, `zone_map_config` (Task 2); `compute_radar` body (`body['l1s'][i]['leaves'][j]` has `global_rank`, `theme`, `members`).
- Produces: `radar.json` and every `radar_history.json` entry carry `zone_map` built from the UNCAPPED members of leaves with `global_rank <= top_leaves`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_zone_map_radar.py`:

```python
"""Theme Viz: members of the top radar leaves, read UNCAPPED.

radar_history.json caps chips at `radar.tickers_per_leaf` (10). The map must
still see every member of a top leaf, or a back-dated session would show
fewer names than the newest one.
"""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import src.stock_utils as su
from src.reporting import export_dashboard_data as ex

DATE = '2026-10-08'
OLDER = '2026-10-07'
CYBER = [f'T{i:02d}' for i in range(1, 13)]           # 12 members, one leaf
OPTICS = ['O1', 'O2', 'O3']
THEMES = {t: ['Cybersecurity / Network'] for t in CYBER}
THEMES.update({t: ['AI / Data Center / Optics'] for t in OPTICS})
THEMES['O1'] = ['AI / Data Center / Optics', 'Cybersecurity / Network']   # dual-tagged


def _master(date_str):
    rows = []
    for i, t in enumerate(CYBER + OPTICS):
        rows.append({
            'date': date_str, 'ticker': t, 'close': 100.0, 'high': 101.0, 'low': 98.0,
            'volume': 2_000_000, 'vol_sma50': 2_000_000,
            'ema10': 101.0, 'ema20': 99.0, 'ema20_w': 95.0, 'sma50_full': 90.0,
            'atr14': 2.0, 'avg_dollar_vol': 150e6, 'adr_pct': 0.05,
            'vars': 8.0 - 0.1 * i, 'vars_20ema': 7.0, 'rs_sts_pct': 95.0 - i,
            'rela_perf_1mo_rank': 50, 'tight_range': False, 'tight_len': 0,
            'tightness': 0.5, 'tight_pctile': 50.0, 'tight_gate': None,
        })
    return pd.DataFrame(rows)


class RadarZoneMapTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = Path(self._tmp.name)
        (self.root / 'master').mkdir()
        for d in (DATE, OLDER):
            su.save_df_to_parquet(_master(d), self.root / 'master' / f'master_{d}.parquet')
        self.master_file = self.root / 'master' / f'master_{DATE}.parquet'
        for p in (
            patch('src.themes.l1_score.load_ticker_themes', return_value=THEMES),
            patch('src.themes.theme_registry.load_ticker_themes', return_value=THEMES),
        ):
            p.start()
            self.addCleanup(p.stop)

    def _snap(self, cap=10):
        return ex._build_radar_snapshot(self.master_file, set(), {}, tickers_per_leaf=cap)

    def test_a_capped_snapshot_still_maps_every_member(self):
        snap = self._snap(cap=10)
        cyber = next(lf for l1 in snap['l1s'] for lf in l1['leaves']
                     if lf['name'] == 'Cybersecurity / Network')
        self.assertEqual(10, len(cyber['tickers']))
        mapped = {t['ticker'] for t in snap['zone_map']['tickers']}
        self.assertTrue(set(CYBER) <= mapped, set(CYBER) - mapped)

    def test_only_members_of_the_top_leaves_are_mapped(self):
        cfg = dict(ex.CONFIG.get('zone_map') or {}, top_leaves=1)
        with patch.dict(ex.CONFIG, {'zone_map': cfg}):
            snap = self._snap(cap=None)
        top = next(lf for l1 in snap['l1s'] for lf in l1['leaves'] if lf['global_rank'] == 1)
        self.assertEqual({t['ticker'] for t in top['tickers']},
                         {t['ticker'] for t in snap['zone_map']['tickers']})

    def test_a_dual_tagged_member_is_one_dot_with_both_leaves(self):
        snap = self._snap(cap=None)
        o1 = [t for t in snap['zone_map']['tickers'] if t['ticker'] == 'O1']
        self.assertEqual(1, len(o1))
        self.assertEqual({'AI / Data Center / Optics', 'Cybersecurity / Network'},
                         set(o1[0]['leaves']))

    def test_every_history_entry_carries_the_map(self):
        out = self.root / 'out'
        out.mkdir()
        with patch.object(ex, '_load_fundamentals_for_tickers', return_value={}):
            ex.export_radar({}, root=self.root, out_dir=out)
        history = json.loads((out / 'radar_history.json').read_text(encoding='utf-8'))
        self.assertEqual(2, len(history))
        for entry in history:
            self.assertIsNotNone(entry['zone_map'], entry['report_date'])
            self.assertEqual(15, len(entry['zone_map']['tickers']), entry['report_date'])


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run python -m unittest tests.test_zone_map_radar -v`
Expected: FAIL with `KeyError: 'zone_map'`.

If `compute_radar` scores no leaf because a fixture column is missing, compare `_master` with `_master_frame` in `tests/test_highlight_publishing.py` and add the missing column. Do not change `compute_radar`.

- [ ] **Step 3: Write the implementation**

In `_build_radar_snapshot`, replace the block from `l1s = []` (line 2089) to the end of the function (line 2147) with:

```python
    # Theme Viz: the members of the top `top_leaves` leaves, UNCAPPED. The
    # history file caps chips at `tickers_per_leaf`, and a map built from the
    # capped chips would show fewer names on a back-dated session than on
    # the newest one.
    zone_cfg = zone_map_config()
    top_leaves = int(zone_cfg['top_leaves'])
    zone_rows = []
    leaves_by_ticker = {}

    l1s = []
    for l1_entry in body['l1s']:
        leaves = []
        for leaf in l1_entry['leaves']:
            members = [{
                'ticker': m['ticker'],
                'score': round(m['composite'], 1),
                'rs': round(m['rs'], 1),
                'vars': round(m['vars'], 2) if m['vars'] is not None else None,
                'price': round(m['price'], 2) if m['price'] is not None else None,
                'dollar_vol': _int_or_none(m.get('avg_dollar_vol')),
                'adr_pct': _round_or_none(m.get('adr_pct'), 4),
                # NaN is not valid JSON and would break the page rather than
                # degrade it, so an unmeasurable figure serializes as null.
                'tightness': _round_or_none(m.get('tightness'), 3),
                'tight': bool(m.get('tight', False)),
                'tight_len': m.get('tight_len'),
                'tight_pctile': _round_or_none(m.get('tight_pctile'), 1),
                'tight_gate': m.get('tight_gate'),
                'is_screened': bool(m.get('is_screened', False)),
                'highlight': _member_highlight(m['ticker']),
            } for m in leaf['members']]
            if day_flags:
                enrich_with_ticker_color(members, day_flags)
            for td in members:
                leaves_by_ticker.setdefault(str(td['ticker']).upper(), []).append(leaf['theme'])
            if leaf['global_rank'] <= top_leaves:
                zone_rows.extend(members)
            ticker_dicts = members if tickers_per_leaf is None else members[:tickers_per_leaf]
            leaves.append({
                'global_rank': leaf['global_rank'],
                'name': leaf['theme'],
                'l2': leaf['l2'],
                'l3': leaf['l3'],
                'raw': round(leaf['raw'], 3),
                'boosted': round(leaf['boosted'], 3),
                'n': leaf['breadth'],
                # Count over ALL scored members, not just the chips that
                # survived tickers_per_leaf — a capped history entry must not
                # report a smaller tight count than the same session's radar.json.
                'n_tight': leaf.get('n_tight') if has_tight else None,
                'tickers': ticker_dicts,
            })
        l1s.append({
            'rank': l1_entry['rank'],
            'name': l1_entry['name'],
            'raw': round(l1_entry['raw'], 4),
            'boosted': round(l1_entry['boosted'], 4),
            'delta': round(l1_entry['delta'], 4),
            'n_leaves': l1_entry['n_leaves'],
            'n_members': l1_entry['n_members'],
            'n_screened': l1_entry['n_screened'],
            'n_tight': l1_entry.get('n_tight') if has_tight else None,
            'leaves': leaves,
        })

    return {
        'report_date': csv_date,
        'params': body['params'],
        'universe_size': body['universe_size'],
        'n_leaves_scored': body['n_leaves_scored'],
        'l1s': l1s,
        'zone_map': build_zone_map(zone_rows, master_df, leaves_by_ticker, zone_cfg),
    }
```

`export_radar`'s history trim uses `{**snap, 'l1s': ...}`, so `zone_map` reaches every history entry with no change there.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run python -m unittest tests.test_zone_map_radar tests.test_export_radar tests.test_highlight_publishing -v`
Expected: all PASS.

Run: `uv run python -m unittest discover -s tests`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/reporting/export_dashboard_data.py tests/test_zone_map_radar.py
git commit -m "Publish the Theme Viz zone_map from the radar snapshot"
```

---

### Task 5: The shared renderer, on VARS Viz first

**Files:**
- Modify: `docs/app.js` — registry (after `CHART_MA_COLORS`, line 1384), `window.MM_COLORS` (line 1389), new zone section inserted directly before the `// ── NETWORK VIZ` header (line 1733), `loadVARSData` (lines 1496-1504), `onTimeTravelSelect` (line 1593), `initTickerClicks` (lines 252-267), init call (line 123)
- Modify: `docs/index.html:825-850` (the VARS Viz block)
- Modify: `docs/style.css:1506-1512` (`.viz-left`) and add the zone section after `.themeviz-legend .lg-sep` (line 1662)
- Modify: `docs/legends.js` — new `zone` section before the `viz` section (line 817), `renderVizLegends` selector (line 976), new `renderZoneKeys`, `DOMContentLoaded` handler
- Modify: `tests/test_dashboard_legends.py:279-287`, `tests/test_dashboard_highlight_markup.py` (`RENDER_SITES`)
- Test: `tests/test_dashboard_zone_map_markup.py`

**Interfaces:**
- Consumes: `snap.zone_map` (Tasks 3-4); existing `escHtml`, `escAttr`, `hlClass`, `hlTitle`, `pickBand`, `COLOR_BANDS`, `openChart`.
- Produces: `ZONE_RULES`, `ZONE_STATES`, `ZONE_FAMILY_CLASS` (registry, exposed on `window.MM_COLORS`); `renderZoneMap(snap, mode, date)` with `mode` in `themes | vars | momentum | volume`; `ZONE_MODES` keyed by mode with `containerId` `<tab>-zone`; `initZoneMaps()`; legends `renderZoneKeys()`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_dashboard_zone_map_markup.py`:

```python
"""Zone map: the browser mirror, the renderer and the markup.

The server decides every zone (export_dashboard_data.build_zone_map). The
browser mirrors the limits in ZONE_RULES only to draw rails, the box and the
Legends tab, so the mirror is pinned to the config here.
"""

import re
import unittest
from pathlib import Path

import yaml

from src.reporting import export_dashboard_data as ex

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / 'docs' / 'app.js').read_text(encoding='utf-8')
HTML = (ROOT / 'docs' / 'index.html').read_text(encoding='utf-8')
CONFIG = yaml.safe_load((ROOT / 'config' / 'workflow_config.yaml').read_text(encoding='utf-8'))

#: Tabs already moved onto the zone map. Task 6 adds the other three.
ZONE_TABS = ('varsviz',)


def _zone_section():
    start = APP.index('  // ── ZONE MAP')
    end = APP.index('  // ── END ZONE MAP', start)
    return APP[start:end]


def _fn(name):
    start = APP.index(f'\n  function {name}(')
    end = APP.index('\n  function ', start + 10)
    return APP[start:end]


class ZoneRulesMirrorTests(unittest.TestCase):
    def _rules(self):
        m = re.search(r'const ZONE_RULES = \{(.*?)\n  \};', APP, re.S)
        self.assertIsNotNone(m, 'ZONE_RULES is gone from docs/app.js')
        return m.group(1)

    def test_every_limit_matches_the_config(self):
        rules, cfg = self._rules(), CONFIG['zone_map']
        for js, py in (('bandLow', 'band_low'), ('bandHigh', 'band_high'),
                       ('weeklyLow', 'weekly_low'), ('weeklyHigh', 'weekly_high')):
            m = re.search(r'\b' + js + r':\s*(-?[\d.]+)', rules)
            self.assertIsNotNone(m, js)
            self.assertEqual(float(cfg[py]), float(m.group(1)), js)
        for js, py in (('xRange', 'x_range'), ('yRange', 'y_range')):
            m = re.search(r'\b' + js + r':\s*\[\s*(-?[\d.]+),\s*(-?[\d.]+)\s*\]', rules)
            self.assertIsNotNone(m, js)
            self.assertEqual([float(v) for v in cfg[py]], [float(m.group(1)), float(m.group(2))], js)

    def test_the_states_match_the_server_in_order(self):
        m = re.search(r'const ZONE_STATES = \[(.*?)\n  \];', APP, re.S)
        self.assertIsNotNone(m, 'ZONE_STATES is gone from docs/app.js')
        self.assertEqual(list(ex.ZONE_STATES), re.findall(r"\bid: '(\w+)'", m.group(1)))

    def test_the_config_matches_the_server_defaults(self):
        for key, default in ex.ZONE_MAP_DEFAULTS.items():
            self.assertEqual(default, CONFIG['zone_map'][key], key)


class ZoneRendererTests(unittest.TestCase):
    def test_the_section_paints_with_classes_never_literals(self):
        self.assertNotRegex(_zone_section(), r"#[0-9a-fA-F]{3,6}\b",
                            'a raw colour in the zone map; paint it with a CSS class')

    def test_an_absent_block_says_when_data_arrives(self):
        self.assertIn('Zone map data arrives with the next daily run.', _zone_section())

    def test_momentum_prints_rs_and_the_rest_print_vars(self):
        modes = re.search(r'const ZONE_MODES = \{(.*?)\n  \};', APP, re.S).group(1)
        self.assertRegex(modes, r"momentum:\s*\{[^}]*metric:\s*'rs'")
        for mode in ('themes', 'vars', 'volume'):
            self.assertRegex(modes, mode + r":\s*\{[^}]*metric:\s*'vars'")

    def test_the_row_number_reads_the_shared_bands(self):
        body = _fn('zoneNum')
        self.assertIn('pickBand(COLOR_BANDS.rs', body)
        self.assertIn('pickBand(COLOR_BANDS.vars', body)

    def test_list_tickers_are_real_links_with_the_tier_tint(self):
        body = _fn('zoneTickerLink')
        for token in ('tn-link', 'hlClass(', 'hlTitle(', 'day-pattern-green', 'data-sym='):
            self.assertIn(token, body)

    def test_the_scatter_emits_no_tier(self):
        """Dot colour already means the daily zone; a tier on it would be noise."""
        body = _fn('zoneScatterCard')
        for token in ('hlClass(', 'hlTitle(', 'hl-short', 'hl-ma-up', 'hl-ma-split'):
            self.assertNotIn(token, body)

    def test_an_off_scale_name_is_clamped_and_titled_with_its_real_value(self):
        body = _fn('zoneScatterCard')
        self.assertIn('zmClamp(', body)
        self.assertIn('zoneTriangle(', body)
        self.assertIn('t.d_daily', body)
        self.assertIn('<title>', body)


class ZoneMarkupTests(unittest.TestCase):
    def test_each_zone_tab_has_its_container_and_key(self):
        for tab in ZONE_TABS:
            block = HTML.split(f'id="content-{tab}"')[1].split('class="tab-content"')[0]
            self.assertIn(f'id="{tab}-zone"', block, tab)
            self.assertIn('data-viz-legend="zone"', block, tab)
            self.assertNotIn('theme-network', block, tab)

    def test_a_list_click_on_a_zone_tab_opens_the_chart(self):
        body = _fn('initTickerClicks')
        for tab in ZONE_TABS:
            self.assertIn(f"'{tab}'", body, tab)


if __name__ == '__main__':
    unittest.main()
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run python -m unittest tests.test_dashboard_zone_map_markup -v`
Expected: FAIL with `ValueError: substring not found` and `ZONE_RULES is gone`.

- [ ] **Step 3: Add the registry entries**

In `docs/app.js`, after `const CHART_MA_COLORS = ...;` (line 1384), add:

```js
  /* Zone map limits, in daily ATR14. A MIRROR of the `zone_map:` block in
     config/workflow_config.yaml. The server decides every zone and state
     (export_dashboard_data.build_zone_map); the browser reads these only to
     draw the rails, the buy-area box and the Legends tab.
     tests/test_dashboard_zone_map_markup.py pins each value to the config. */
  const ZONE_RULES = {
    bandLow: -0.5,
    bandHigh: 1.0,
    weeklyLow: 0.0,
    weeklyHigh: 4.0,
    xRange: [-2.0, 5.0],
    yRange: [-4.0, 8.0],
  };

  /* The colour class of each daily zone. Dots, zone backgrounds, cluster bar
     segments and state rows all paint from these three classes in style.css. */
  const ZONE_FAMILY_CLASS = { under: 'zm-under', band: 'zm-band', above: 'zm-above' };

  /* The seven states, best first. `id` matches export_dashboard_data.ZONE_STATES
     in the same order (pinned by the same test). `family` picks the row colour.
     A note says where a name sits. It never says what the name will do next. */
  const ZONE_STATES = [
    { id: 'buy_area', family: 'band', label: 'In the buy area',
      note: 'In the daily band, and the weekly is not stretched.' },
    { id: 'buy_stretched', family: 'band', label: 'Buy area, weekly stretched',
      note: 'In the daily band, but extended above the weekly EMA20.' },
    { id: 'above', family: 'above', label: 'Above the buy area',
      note: 'Above the daily band. The weekly is not stretched.' },
    { id: 'extended', family: 'above', label: 'Extended on both',
      note: 'Above the daily band and extended above the weekly EMA20.' },
    { id: 'under_daily', family: 'under', label: 'Under the daily band',
      note: 'Below the daily band. The close is at or above the weekly EMA20.' },
    { id: 'weekly_broken', family: 'under', label: 'Weekly broken',
      note: 'Below the weekly EMA20. The daily is in or above its band.' },
    { id: 'under_both', family: 'under', label: 'Under both',
      note: 'Below the daily band and below the weekly EMA20.' },
  ];
```

In `window.MM_COLORS` (line 1389), add `ZONE_RULES, ZONE_STATES, ZONE_FAMILY_CLASS,` after `CHART_MA_COLORS,`.

- [ ] **Step 4: Add the zone map section**

In `docs/app.js`, insert directly before the line `  // ── NETWORK VIZ — Theme, Momentum, Volume, and VARS Viz ─────────────` (line 1733):

```js
  // ── ZONE MAP — the four Viz tabs ───────────────────────────────────
  // Each ticker sits at (close vs the daily EMA20, close vs the weekly
  // EMA20), both in daily ATR14. The server decides every zone and state
  // (export_dashboard_data.build_zone_map); this code only draws. It paints
  // with CSS classes, never with colour literals. The SVG scales through its
  // viewBox, so it draws correctly on a hidden tab.

  const ZONE_MODES = {
    themes: { containerId: 'themeviz-zone', tabId: 'themeviz', metric: 'vars',
      source: p => `members of the top ${p.top_leaves} radar leaves` },
    vars: { containerId: 'varsviz-zone', tabId: 'varsviz', metric: 'vars',
      source: () => 'VARS tab survivors' },
    momentum: { containerId: 'momentumviz-zone', tabId: 'momentumviz', metric: 'rs',
      source: () => 'Momentum tab survivors' },
    volume: { containerId: 'volumeviz-zone', tabId: 'volumeviz', metric: 'vars',
      source: () => 'Volume tab survivors' },
  };

  const ZONE_METRIC = {
    vars: { key: 'vars', label: 'VARS', digits: 1 },
    rs: { key: 'rs', label: 'RS', digits: 0 },
  };

  // Leaf rows shown in the clusters card before the "N other themes" line.
  const ZONE_CLUSTER_ROWS = 8;

  // SVG geometry, in viewBox units.
  const ZM = { w: 760, h: 470, l: 46, r: 14, t: 40, b: 34 };

  function zmX(v) {
    const [a, b] = ZONE_RULES.xRange;
    return ZM.l + (v - a) / (b - a) * (ZM.w - ZM.l - ZM.r);
  }

  function zmY(v) {
    const [a, b] = ZONE_RULES.yRange;
    return ZM.h - ZM.b - (v - a) / (b - a) * (ZM.h - ZM.t - ZM.b);
  }

  function zmClamp(v, range) {
    return Math.min(range[1], Math.max(range[0], v));
  }

  function zmSigned(v) {
    return (v > 0 ? '+' : '') + Number(v).toFixed(2);
  }

  function zmF(v) {
    return Number(v).toFixed(1);
  }

  function zoneStateLabel(id) {
    const s = ZONE_STATES.find(x => x.id === id);
    return s ? s.label : id;
  }

  function zoneMetricText(t, metric) {
    const v = t[metric.key];
    return v == null ? '—' : Number(v).toFixed(metric.digits);
  }

  function zoneNum(t, metric) {
    const v = t[metric.key];
    if (v == null) return '';
    const cls = metric.key === 'rs' ? pickBand(COLOR_BANDS.rs, v) : pickBand(COLOR_BANDS.vars, v);
    return ` <span class="zm-num ${cls}">${Number(v).toFixed(metric.digits)}</span>`;
  }

  function zoneSortByMetric(list, metric) {
    const val = t => (t[metric.key] == null ? -Infinity : t[metric.key]);
    return list.slice().sort((a, b) => val(b) - val(a) || a.ticker.localeCompare(b.ticker));
  }

  /** A ticker in the states or clusters card: a real .tn-link, so it wears
   *  the day-pattern text, the tier tint and the selection like every tab. */
  function zoneTickerLink(t, metric, bold) {
    return `<span class="zm-tk${bold ? ' zm-buy' : ''}"><span class="tn-link${t.ticker_color === 'green' ? ' day-pattern-green' : ''}${hlClass(t.highlight)}"${hlTitle(t.highlight)} data-sym="${escAttr(t.ticker)}" data-nm="${escAttr(t.ticker + ' · Zone map')}">${escHtml(t.ticker)}</span>${metric ? zoneNum(t, metric) : ''}</span>`;
  }

  /** An off-scale name: a triangle on the nearest edge, pointing outward. */
  function zoneTriangle(p, t) {
    const R = ZONE_RULES;
    const s = 5;
    let pts;
    if (t.d_weekly > R.yRange[1]) pts = [[0, -s], [s, s], [-s, s]];
    else if (t.d_weekly < R.yRange[0]) pts = [[0, s], [s, -s], [-s, -s]];
    else if (t.d_daily > R.xRange[1]) pts = [[s, 0], [-s, s], [-s, -s]];
    else pts = [[-s, 0], [s, s], [s, -s]];
    const points = pts.map(([dx, dy]) => `${zmF(p.x + dx)},${zmF(p.y + dy)}`).join(' ');
    return `<polygon class="zm-dot ${ZONE_FAMILY_CLASS[t.zone_daily]}" points="${points}"/>`;
  }

  function zoneScatterCard(zm, cfg, metric) {
    const R = ZONE_RULES;
    const tickers = zm.tickers;
    const p = zm.params || {};
    const xs = [R.xRange[0], R.bandLow, R.bandHigh, R.xRange[1]].map(zmX);
    const yTop = zmY(R.yRange[1]);
    const yBot = zmY(R.yRange[0]);
    const families = ['under', 'band', 'above'];
    const heads = { under: 'Under the daily band', band: 'In the daily band', above: 'Above the daily band' };
    const counts = { under: 0, band: 0, above: 0 };
    tickers.forEach(t => { counts[t.zone_daily] += 1; });
    const nBuy = tickers.filter(t => t.state === 'buy_area').length;

    let svg = '';
    families.forEach((f, i) => {
      const mid = (xs[i] + xs[i + 1]) / 2;
      svg += `<rect class="zm-bg ${ZONE_FAMILY_CLASS[f]}" x="${zmF(xs[i])}" y="${zmF(yTop)}" width="${zmF(xs[i + 1] - xs[i])}" height="${zmF(yBot - yTop)}"/>`;
      svg += `<text class="zm-head" x="${zmF(mid)}" y="${zmF(yTop - 20)}">${heads[f]}</text>`;
      svg += `<text class="zm-axis" x="${zmF(mid)}" y="${zmF(yTop - 8)}">${counts[f]} of ${tickers.length}</text>`;
    });

    const rail = (x1, y1, x2, y2, extra) =>
      `<line class="zm-rail${extra ? ' ' + extra : ''}" x1="${zmF(x1)}" y1="${zmF(y1)}" x2="${zmF(x2)}" y2="${zmF(y2)}"/>`;
    const yLow = zmY(R.weeklyLow);
    const yHigh = zmY(R.weeklyHigh);
    svg += rail(xs[1], yTop, xs[1], yBot) + rail(xs[2], yTop, xs[2], yBot);
    svg += rail(xs[0], yLow, xs[3], yLow);
    svg += rail(xs[0], yHigh, xs[3], yHigh, 'zm-rail-dash');
    svg += `<text class="zm-axis zm-left" x="${zmF(xs[0] + 4)}" y="${zmF(yLow - 4)}">weekly EMA20</text>`;
    svg += `<text class="zm-axis zm-left" x="${zmF(xs[0] + 4)}" y="${zmF(yHigh - 4)}">weekly extended · &gt; +${R.weeklyHigh} ATR</text>`;

    svg += `<rect class="zm-box" x="${zmF(xs[1])}" y="${zmF(yHigh)}" width="${zmF(xs[2] - xs[1])}" height="${zmF(yLow - yHigh)}"/>`;
    svg += `<text class="zm-box-tag" x="${zmF((xs[1] + xs[2]) / 2)}" y="${zmF(yHigh + 13)}">Buy area · ${nBuy}</text>`;

    for (let v = Math.ceil(R.xRange[0]); v <= R.xRange[1]; v += 1) {
      svg += `<text class="zm-axis" x="${zmF(zmX(v))}" y="${zmF(yBot + 13)}">${v > 0 ? '+' + v : v}</text>`;
    }
    for (let v = Math.ceil(R.yRange[0] / 2) * 2; v <= R.yRange[1]; v += 2) {
      svg += `<text class="zm-axis zm-right" x="${zmF(xs[0] - 5)}" y="${zmF(zmY(v) + 3)}">${v > 0 ? '+' + v : v}</text>`;
    }
    svg += `<text class="zm-axis" x="${zmF((xs[0] + xs[3]) / 2)}" y="${ZM.h - 4}">daily: close vs EMA20, in ATR →</text>`;
    svg += `<text class="zm-axis" transform="translate(11 ${zmF((yTop + yBot) / 2)}) rotate(-90)">weekly: close vs EMA20, in ATR →</text>`;

    // Positions, then labels: buy-area names first, then tight names, then
    // the rest by the row number. A label that would overlap one already
    // placed is skipped; the <title> still names every dot on hover.
    const pts = tickers.map(t => {
      const cx = zmClamp(t.d_daily, R.xRange);
      const cy = zmClamp(t.d_weekly, R.yRange);
      return { t, x: zmX(cx), y: zmY(cy), off: cx !== t.d_daily || cy !== t.d_weekly, box: null };
    });
    const prio = q => (q.t.state === 'buy_area' ? 0 : q.t.tight ? 1 : 2);
    const val = q => (q.t[metric.key] == null ? -Infinity : q.t[metric.key]);
    const placed = [];
    pts.slice().sort((a, b) => prio(a) - prio(b) || val(b) - val(a)).forEach(q => {
      const w = q.t.ticker.length * 6.2 + 2;
      const h = 10;
      const box = { x: q.x + 6, y: q.y - h / 2, w, h };
      if (box.x + w > ZM.w - ZM.r) box.x = q.x - 6 - w;
      const hit = placed.some(o => box.x < o.x + o.w && o.x < box.x + box.w && box.y < o.y + o.h && o.y < box.y + box.h);
      if (hit) return;
      placed.push(box);
      q.box = box;
    });

    pts.forEach(q => {
      const t = q.t;
      const title = `${t.ticker} · ${zoneStateLabel(t.state)}\n`
        + `Daily ${zmSigned(t.d_daily)} ATR · Weekly ${zmSigned(t.d_weekly)} ATR\n`
        + `${metric.label} ${zoneMetricText(t, metric)}`
        + (t.tight ? `\nTight range${t.tight_len ? ' ' + t.tight_len + 'd' : ''}` : '');
      const shape = q.off
        ? zoneTriangle(q, t)
        : `<circle class="zm-dot ${ZONE_FAMILY_CLASS[t.zone_daily]}" cx="${zmF(q.x)}" cy="${zmF(q.y)}" r="4"/>`;
      const ring = t.tight ? `<circle class="zm-ring" cx="${zmF(q.x)}" cy="${zmF(q.y)}" r="7"/>` : '';
      const label = q.box
        ? `<text class="zm-label${t.state === 'buy_area' ? ' zm-label-buy' : ''}" x="${zmF(q.box.x)}" y="${zmF(q.y + 3.5)}">${escHtml(t.ticker)}</text>`
        : '';
      svg += `<g class="zm-pt" data-zm-sym="${escAttr(t.ticker)}"><title>${escHtml(title)}</title>${ring}${shape}${label}</g>`;
    });

    const left = [];
    if (zm.n_floor_dropped) left.push(`${zm.n_floor_dropped} under the floors`);
    if (zm.n_no_weekly) left.push(`${zm.n_no_weekly} without a weekly EMA20`);
    if (zm.n_no_daily) left.push(`${zm.n_no_daily} without a daily reading`);
    const adr = ((p.min_adr_pct || 0) * 100).toFixed(1).replace(/\.0$/, '');
    const caption = `${tickers.length} names: ${cfg.source(p)}, average dollar volume $${Math.round((p.min_avg_dollar_vol || 0) / 1e6)}M or more, ADR ${adr}% or more.`
      + (left.length ? ` Left off: ${left.join(', ')}.` : '');
    return `<div class="card zm-card"><div class="card-lbl">Where is each name?</div>`
      + `<p class="zm-caption">${escHtml(caption)}</p>`
      + `<svg class="zm-svg" viewBox="0 0 ${ZM.w} ${ZM.h}" role="img" aria-label="Zone map">${svg}</svg></div>`;
  }

  function zoneStatesCard(tickers, metric) {
    const rows = ZONE_STATES.map(s => {
      const members = zoneSortByMetric(tickers.filter(t => t.state === s.id), metric);
      const list = members.length ? members.map(t => zoneTickerLink(t, null, false)).join(' ') : '—';
      return `<div class="zm-state ${ZONE_FAMILY_CLASS[s.family]}">`
        + `<div class="zm-state-head"><span>${escHtml(s.label)}</span><span class="zm-state-n">${members.length}</span></div>`
        + `<div class="zm-state-note">${escHtml(s.note)}</div>`
        + `<div class="zm-state-list">${list}</div></div>`;
    }).join('');
    return `<div class="card zm-card"><div class="card-lbl">Seven states, best first</div>${rows}</div>`;
  }

  function zoneClustersCard(tickers, metric) {
    const byLeaf = new Map();
    tickers.forEach(t => (t.leaves || []).forEach(leaf => {
      if (!byLeaf.has(leaf)) byLeaf.set(leaf, []);
      byLeaf.get(leaf).push(t);
    }));
    const leaves = [...byLeaf.entries()].map(([name, members]) => ({
      name,
      members: zoneSortByMetric(members, metric),
      nBuy: members.filter(t => t.state === 'buy_area').length,
    })).sort((a, b) => b.nBuy - a.nBuy || b.members.length - a.members.length || a.name.localeCompare(b.name));
    const shown = leaves.slice(0, ZONE_CLUSTER_ROWS);
    const shownNames = new Set(shown.map(l => l.name));
    const remaining = tickers.filter(t => !(t.leaves || []).some(l => shownNames.has(l)));

    const rows = shown.map(l => {
      const n = l.members.length;
      const bar = ['band', 'above', 'under'].map(f => {
        const k = l.members.filter(t => t.zone_daily === f).length;
        return k ? `<span class="zm-seg ${ZONE_FAMILY_CLASS[f]}" style="width:${(100 * k / n).toFixed(1)}%"></span>` : '';
      }).join('');
      const list = l.members.map(t => zoneTickerLink(t, metric, t.state === 'buy_area')).join('<span class="zm-sep"> · </span>');
      return `<div class="zm-leaf"><div class="zm-leaf-head">`
        + `<span class="zm-leaf-name" title="${escAttr(l.name)}">${escHtml(l.name)}</span>`
        + `<span class="zm-bar">${bar}</span><span class="zm-leaf-n">${l.nBuy}/${n}</span></div>`
        + `<div class="zm-leaf-list">${list}</div></div>`;
    }).join('');
    const rest = leaves.length - shown.length;
    const foot = (remaining.length
      ? `${remaining.length} more ${remaining.length === 1 ? 'name sits' : 'names sit'} in ${rest} other ${rest === 1 ? 'theme' : 'themes'} or carry no theme tag. `
      : '')
      + `Buy area / total. Names in the buy area are bold. The number after each ticker is its ${metric.label}.`;
    return `<div class="card zm-card"><div class="card-lbl">Where they cluster</div>${rows || '<div class="no-data">No theme tags.</div>'}`
      + `<p class="zm-foot">${escHtml(foot)}</p></div>`;
  }

  function renderZoneMap(snap, mode, date) {
    const cfg = ZONE_MODES[mode];
    const el = cfg && document.getElementById(cfg.containerId);
    if (!el) return;
    if (!snap) {
      el.innerHTML = `<div class="no-data">No data for ${escHtml(date || 'this session')}.</div>`;
      return;
    }
    const zm = snap.zone_map;
    if (!zm) {
      el.innerHTML = '<div class="no-data">Zone map data arrives with the next daily run.</div>';
      return;
    }
    const tickers = zm.tickers || [];
    if (!tickers.length) {
      el.innerHTML = `<div class="no-data">No ticker passed the zone-map floors for ${escHtml(snap.report_date || date || 'this session')}: `
        + `${zm.n_candidates} candidates, ${zm.n_floor_dropped} under the floors, `
        + `${zm.n_no_weekly} without a weekly EMA20, ${zm.n_no_daily} without a daily reading.</div>`;
      return;
    }
    const metric = ZONE_METRIC[cfg.metric];
    el.innerHTML = zoneScatterCard(zm, cfg, metric)
      + zoneStatesCard(tickers, metric)
      + zoneClustersCard(tickers, metric);
  }

  /** Mark one ticker selected on the dot and on every list link of the tab. */
  function selectZoneTicker(mode, sym) {
    const el = document.getElementById(ZONE_MODES[mode].containerId);
    if (!el) return;
    const q = CSS.escape(sym);
    el.querySelectorAll('.zm-pt.zm-sel').forEach(g => g.classList.remove('zm-sel'));
    el.querySelectorAll(`.zm-pt[data-zm-sym="${q}"]`).forEach(g => g.classList.add('zm-sel'));
    el.querySelectorAll('.tn-link.active-ticker').forEach(l => l.classList.remove('active-ticker'));
    el.querySelectorAll(`.tn-link[data-sym="${q}"]`).forEach(l => l.classList.add('active-ticker'));
  }

  function initZoneMaps() {
    Object.entries(ZONE_MODES).forEach(([mode, cfg]) => {
      const el = document.getElementById(cfg.containerId);
      if (!el) return;
      el.addEventListener('click', e => {
        const pt = e.target.closest('.zm-pt');
        const link = e.target.closest('.tn-link');
        const sym = pt ? pt.dataset.zmSym : link ? link.dataset.sym : null;
        if (!sym) return;
        // A list link is a .tn-link, so initTickerClicks opens its chart.
        // A dot is not, so this handler opens the chart for it.
        if (pt) openChart(cfg.tabId, sym);
        // After initTickerClicks has cleared and set its single link.
        setTimeout(() => selectZoneTicker(mode, sym), 0);
      });
    });
  }
  // ── END ZONE MAP ───────────────────────────────────────────────────

```

- [ ] **Step 5: Wire VARS Viz**

In `loadVARSData` (lines 1496-1504), replace `renderVARSNetwork(current);` with `renderZoneMap(current, 'vars');`. In its `.catch`, replace the two lines that find `vars-network` with:

```js
        const vz = document.getElementById('varsviz-zone');
        if (vz) vz.innerHTML = '<div class="no-data">VARS data not available.</div>';
```

In `onTimeTravelSelect` (line 1593), replace `renderVARSNetwork(varsSnap, date);` with `renderZoneMap(varsSnap, 'vars', date);`.

In `initTickerClicks` (lines 252-267), replace the whole `let tabId; if ... else return;` chain with:

```js
      // Every tab whose ticker links open the chart beside them.
      const CHART_TABS = ['macro', 'themes', 'themeviz', 'momentum', 'momentumviz',
        'volume', 'volumeviz', 'vars', 'varsviz', 'industry', 'etf', 'ep', 'parabolic', 'si'];
      const tabId = tabContent.id.replace('content-', '');
      if (!CHART_TABS.includes(tabId)) return;
```

After `initTickerClicks();` (line 123), add `initZoneMaps();`.

- [ ] **Step 6: Replace the VARS Viz markup**

In `docs/index.html`, inside `<div class="left-panel viz-left" id="varsviz-left">` (line 826), replace the whole `<div class="card themeviz-card"> … </div>` block (from line 830 to the closing `</div>` of that card, after the `data-viz-legend="vars"` line) with:

```html
      <div class="zone-map" id="varsviz-zone"></div>
      <!-- Drawn by legends.js from ZONE_RULES, the same limits the map uses. -->
      <div class="themeviz-legend" data-viz-legend="zone"></div>
```

In the VARS Viz chart placeholder below it, change "Click any ticker node to open chart" to "Click any ticker to open chart".

- [ ] **Step 7: Add the CSS**

In `docs/style.css`, replace the `.viz-left` rule (lines 1506-1512) with:

```css
.viz-left {
  width: 60%;
  display: flex;
  flex-direction: column;
  overflow-y: auto;
  padding: 14px;
}
```

After the `.themeviz-legend .lg-sep` rule (ends at line 1662), add:

```css
/* ═══════════════════════════════════════════════════════
   ZONE MAP — the four Viz tabs
   Every colour is an existing token. The three daily-zone classes
   (zm-under / zm-band / zm-above) paint the dots, the zone backgrounds,
   the cluster bar segments and the state rows. A rail is grey, never a
   marker colour (the NASI rule). Labels and rails are structure, not signal.
   ═══════════════════════════════════════════════════════ */

.zone-map { display: flex; flex-direction: column; gap: 12px; margin-top: 12px; }
.zm-card { margin: 0; }
.zm-caption { margin: 0 0 8px; font-size: 11px; color: var(--text2); }
.zm-svg { display: block; width: 100%; height: auto; font-family: var(--mono); }

.zm-bg { stroke: none; }
.zm-bg.zm-under { fill: var(--rdim); }
.zm-bg.zm-band { fill: var(--gdim); }
.zm-bg.zm-above { fill: var(--adim2); }

.zm-dot { stroke: var(--bg); stroke-width: 1; }
.zm-dot.zm-under { fill: var(--red); }
.zm-dot.zm-band { fill: var(--green); }
.zm-dot.zm-above { fill: var(--amber); }
.zm-ring { fill: none; stroke: var(--tight); stroke-width: 1.5; }
.zm-pt { cursor: pointer; }
.zm-pt.zm-sel .zm-dot { stroke: var(--yellow); stroke-width: 2; }
.zm-pt.zm-sel .zm-label { fill: var(--yellow); }

.zm-rail { stroke: var(--border2); stroke-width: 1; }
.zm-rail.zm-rail-dash { stroke-dasharray: 5 4; }
.zm-box { fill: none; stroke: var(--green); stroke-width: 1.2; }
.zm-box-tag { fill: var(--green); font-size: 10px; text-anchor: middle; }
.zm-head { fill: var(--text2); font-family: var(--display); font-size: 10px; text-anchor: middle; }
.zm-axis { fill: var(--text3); font-size: 9px; text-anchor: middle; }
.zm-axis.zm-left { text-anchor: start; }
.zm-axis.zm-right { text-anchor: end; }
.zm-label { fill: var(--text2); font-size: 10px; }
.zm-label.zm-label-buy { fill: var(--text); }

.zm-state { margin-top: 8px; padding: 8px 10px; border-left: 3px solid var(--border2); background: var(--bg3); }
.zm-state.zm-under { border-left-color: var(--red); background: var(--rdim); }
.zm-state.zm-band { border-left-color: var(--green); background: var(--gdim); }
.zm-state.zm-above { border-left-color: var(--amber); background: var(--adim2); }
.zm-state-head { display: flex; justify-content: space-between; font-family: var(--display); font-size: 12px; color: var(--text); }
.zm-state-n { font-family: var(--mono); color: var(--text2); }
.zm-state-note { margin: 2px 0 4px; font-size: 11px; color: var(--text2); }
.zm-state-list,
.zm-leaf-list { font-family: var(--mono); font-size: 11px; line-height: 1.7; }
.zm-tk { white-space: nowrap; }
.zm-buy .tn-link { font-weight: 600; }

.zm-leaf { margin-top: 8px; }
.zm-leaf-head { display: grid; grid-template-columns: minmax(0, 1fr) minmax(80px, 2fr) auto; gap: 10px; align-items: center; font-size: 12px; }
.zm-leaf-name { overflow: hidden; white-space: nowrap; text-overflow: ellipsis; color: var(--text); }
.zm-bar { display: flex; height: 6px; background: var(--bg4); }
.zm-seg.zm-under { background: var(--red); }
.zm-seg.zm-band { background: var(--green); }
.zm-seg.zm-above { background: var(--amber); }
.zm-leaf-n { font-family: var(--mono); color: var(--text2); }
.zm-num { font-size: 10px; color: var(--text3); }
.zm-sep { color: var(--text3); }
.zm-foot { margin: 8px 0 0; font-size: 11px; color: var(--text3); }

.lg-zm { width: 14px; height: 14px; vertical-align: middle; margin-right: 6px; }
```

- [ ] **Step 8: Add the Legends section and the zone key**

In `docs/legends.js`, add directly after the line `const VIZ_TABS = ['themeviz', 'momentumviz', 'volumeviz', 'varsviz'];` (line 522):

```js
    const ZONE_TABS = ['themeviz', 'varsviz', 'momentumviz', 'volumeviz'];
    const Z = C.ZONE_RULES;
    const zs = v => (v > 0 ? '+' : '') + v;
```

Insert this section before the `{ id: 'viz', title: 'Network viz tabs', ...` section (line 817):

```js
      {
        id: 'zone', title: 'Zone map (Viz tabs)',
        intro: 'The four Viz tabs place each ticker by two distances, both in daily ATR14. Across: the close against the daily EMA20. Up: the close against the weekly EMA20.',
        cards: [
          {
            id: 'zone-dots', title: 'Dot colour — the daily zone',
            sample: zoneDot('zm-band'),
            where: ZONE_TABS,
            body: `${keyList([
                [zoneDot('zm-under'), `Red: under the daily band, below ${zs(Z.bandLow)} ATR.`],
                [zoneDot('zm-band'), `Green: in the daily band, from ${zs(Z.bandLow)} to ${zs(Z.bandHigh)} ATR.`],
                [zoneDot('zm-above'), `Amber: above the daily band, above ${zs(Z.bandHigh)} ATR.`],
              ])}
              <p>Height is the weekly zone. Below ${zs(Z.weeklyLow)} ATR the close is under the weekly EMA20. Above ${zs(Z.weeklyHigh)} ATR it is extended. The zone backgrounds, the cluster bars and the state rows use the same three colours:</p>
              ${zoneBgFigure()}
              <div class="zm-state zm-band"><div class="zm-state-head"><span>In the buy area</span><span class="zm-state-n">3</span></div></div>
              <p><span class="zm-bar" style="display:inline-flex;width:80px"><span class="zm-seg zm-band" style="width:50%"></span><span class="zm-seg zm-above" style="width:30%"></span><span class="zm-seg zm-under" style="width:20%"></span></span> A cluster bar: the leaf's names by daily zone.</p>`,
          },
          {
            id: 'zone-box', title: 'Green outline — the buy area',
            sample: '<svg class="lg-zm" viewBox="0 0 24 14"><rect class="zm-box" x="1" y="1" width="22" height="12"/></svg>',
            where: ZONE_TABS,
            body: `<p>The buy area is the daily band with the weekly between ${zs(Z.weeklyLow)} and ${zs(Z.weeklyHigh)} ATR. Its tag (<svg class="lg-zm" style="width:70px" viewBox="0 0 70 14"><text class="zm-box-tag" x="35" y="11">Buy area · 3</text></svg>) counts the names inside. In the cluster rows, names in the buy area are bold.</p>
              <p>No forward return has been measured for the buy area. The limits are a display choice.</p>`,
          },
          {
            id: 'zone-marks', title: 'Rings and markers',
            sample: '<svg class="lg-zm" viewBox="0 0 16 16"><circle class="zm-ring" cx="8" cy="8" r="7"/><circle class="zm-dot zm-band" cx="8" cy="8" r="4"/></svg>',
            where: ZONE_TABS,
            body: keyList([
              ['<svg class="lg-zm" viewBox="0 0 16 16"><circle class="zm-ring" cx="8" cy="8" r="7"/><circle class="zm-dot zm-band" cx="8" cy="8" r="4"/></svg>', 'Violet ring: the ticker is in a tight range (see the violet tint card).'],
              ['<svg class="lg-zm" style="width:44px" viewBox="0 0 44 16"><g class="zm-pt zm-sel"><circle class="zm-dot zm-band" cx="8" cy="8" r="4"/><text class="zm-label" x="15" y="11" font-size="10">MU</text></g></svg>', 'Yellow outline and label: the selected ticker.'],
              ['<svg class="lg-zm" viewBox="0 0 12 12"><polygon class="zm-dot zm-above" points="6,1 11,11 1,11"/></svg>', 'Triangle on an edge: the ticker is off the scale. Hover it for the real distances.'],
            ]),
          },
        ],
      },
```

Add these helpers after `function edgesSvg()` (it starts at line 439; add after its closing brace):

```js
  function zoneDot(cls) {
    return `<svg class="lg-zm" viewBox="0 0 12 12"><circle class="zm-dot ${cls}" cx="6" cy="6" r="4"/></svg>`;
  }

  function zoneBgFigure() {
    return '<svg class="lg-zm" style="width:120px;height:24px" viewBox="0 0 120 24">'
      + '<rect class="zm-bg zm-under" x="0" y="0" width="30" height="24"/>'
      + '<rect class="zm-bg zm-band" x="30" y="0" width="40" height="24"/>'
      + '<rect class="zm-bg zm-above" x="70" y="0" width="50" height="24"/>'
      + '<circle class="zm-dot zm-under" cx="15" cy="12" r="4"/>'
      + '<circle class="zm-dot zm-band" cx="50" cy="12" r="4"/>'
      + '<circle class="zm-dot zm-above" cx="95" cy="12" r="4"/></svg>';
  }

  /* The short key under each zone map, drawn from the same classes. */
  function renderZoneKeys() {
    const items = [
      [zoneDot('zm-under'), 'Under the daily band'],
      [zoneDot('zm-band'), 'In the band'],
      [zoneDot('zm-above'), 'Above the band'],
      ['<svg class="lg-zm" viewBox="0 0 24 14"><rect class="zm-box" x="1" y="1" width="22" height="12"/></svg>', 'Buy area'],
      ['<svg class="lg-zm" viewBox="0 0 16 16"><circle class="zm-ring" cx="8" cy="8" r="7"/></svg>', 'Tight range'],
    ].map(([s, t]) => `<span class="lg-item">${s}${esc(t)}</span>`).join('');
    document.querySelectorAll('[data-viz-legend="zone"]').forEach(el => {
      el.innerHTML = items + '<a class="lg-item lg-more" href="#" data-open-legends>All colours →</a>';
    });
  }
```

In `renderVizLegends` (line 976), change `document.querySelectorAll('[data-viz-legend]')` to `document.querySelectorAll('[data-viz-legend="rs"], [data-viz-legend="vars"]')`, so the network key does not overwrite the zone key.

In the `DOMContentLoaded` handler, add `renderZoneKeys();` after `renderVizLegends();`.

In the `green`, `selected`, `hover`, `ladder`, `tight`, `short`, `ma-up` and `ma-split` cards, the list links on the zone tabs now wear those classes. Change each card's `where:` to add `ZONE_TABS`. For example, `where: LIST_TABS,` becomes `where: LIST_TABS.concat(ZONE_TABS),` and `where: LIST_TABS.concat(['ep']),` becomes `where: LIST_TABS.concat(['ep'], ZONE_TABS),`.

- [ ] **Step 9: Update the existing tests**

In `tests/test_dashboard_legends.py`, replace `test_the_viz_keys_are_generated_not_hand_written` (lines 279-287) with:

```python
    def test_the_viz_keys_are_generated_not_hand_written(self):
        """The hand-written keys had drifted: a blue swatch for a cyan node,
        "score >= 100" for a band that starts at 65, and RS swatches on the
        Volume Viz tab, whose nodes are coloured by VARS."""
        self.assertEqual(3, len(re.findall(r'data-viz-legend="(?:rs|vars)"', HTML)))
        self.assertEqual(1, len(re.findall(r'data-viz-legend="zone"', HTML)))
        for stale in ('lg-rs90', 'lg-vars6', 'lg-blazing'):
            self.assertNotIn(stale, HTML)
            self.assertNotIn(stale, CSS)
        self.assertIn('data-viz-legend="vars"></div>', HTML.split('volume-network')[1][:1500])
        self.assertIn('[data-viz-legend="zone"]', LEGENDS)
```

In `tests/test_dashboard_highlight_markup.py`, add `'zoneTickerLink'` to the `RENDER_SITES` list.

- [ ] **Step 10: Run the tests to verify they pass**

Run: `uv run python -m unittest tests.test_dashboard_zone_map_markup tests.test_dashboard_legends tests.test_dashboard_highlight_markup -v`
Expected: all PASS. If `test_every_signal_class_on_the_page_has_a_legend_entry` lists a `zm-` class, add that literal class name to a sample in the `zone` legend section. Do not add it to `NOT_A_SIGNAL`.

Run: `uv run python -m unittest discover -s tests`
Expected: all PASS.

- [ ] **Step 11: Commit**

```bash
git add docs/app.js docs/index.html docs/style.css docs/legends.js tests/test_dashboard_zone_map_markup.py tests/test_dashboard_legends.py tests/test_dashboard_highlight_markup.py
git commit -m "Draw VARS Viz as a zone map"
```

---

### Task 6: Theme, Momentum and Volume Viz on the zone map; remove the networks

**Files:**
- Modify: `docs/index.html` — the themeviz (lines 425-454), momentumviz (574-602) and volumeviz (604-632) blocks; the cytoscape script (line 947)
- Modify: `docs/app.js` — `loadThemeData`, `loadMomentumData`, `loadVolumeData`, `onTimeTravelSelect`; delete the network section; delete `RADAR_VIZ_MEMBERS`, `radarVizSnap`, `varsVizSnap`; delete `vizRs`, `vizVars`, `vizThemeStrength`, `VIZ_COLORS`, `VIZ_HOT`; edit `window.MM_COLORS`
- Modify: `docs/style.css` — delete the network rules
- Modify: `docs/legends.js` — delete the `viz` section and its helpers
- Modify: `tests/test_dashboard_legends.py`, `tests/test_dashboard_highlight_markup.py`, `tests/test_dashboard_zone_map_markup.py`

**Interfaces:**
- Consumes: `renderZoneMap`, `ZONE_MODES` (Task 5).
- Produces: all four Viz tabs on the zone map; no cytoscape anywhere.

- [ ] **Step 1: Extend the failing test**

In `tests/test_dashboard_zone_map_markup.py`, change `ZONE_TABS` to:

```python
ZONE_TABS = ('themeviz', 'varsviz', 'momentumviz', 'volumeviz')
```

Add to `ZoneMarkupTests`:

```python
    def test_no_network_code_remains(self):
        LEGENDS = (ROOT / 'docs' / 'legends.js').read_text(encoding='utf-8')
        CSS = (ROOT / 'docs' / 'style.css').read_text(encoding='utf-8')
        for token in ('cytoscape', 'renderNetwork', 'radarVizSnap', 'varsVizSnap',
                      'VIZ_COLORS', 'VIZ_HOT', 'vizRs', 'vizVars', 'vizThemeStrength',
                      'theme-network', 'tight-pulse-ring'):
            for name, text in (('app.js', APP), ('index.html', HTML),
                               ('legends.js', LEGENDS), ('style.css', CSS)):
                self.assertNotIn(token, text, f'{token} still in {name}')
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run python -m unittest tests.test_dashboard_zone_map_markup -v`
Expected: FAIL on `themeviz-zone` and on `cytoscape still in app.js`.

- [ ] **Step 3: Replace the three tab blocks and the script tag**

In `docs/index.html`, for each of `themeviz-left`, `momentumviz-left` and `volumeviz-left`, replace the `<div class="card themeviz-card"> … </div>` block with the same two lines Task 5 used, with the tab's own id:

```html
        <div class="zone-map" id="themeviz-zone"></div>
        <!-- Drawn by legends.js from ZONE_RULES, the same limits the map uses. -->
        <div class="themeviz-legend" data-viz-legend="zone"></div>
```

(`momentumviz-zone` and `volumeviz-zone` for the other two.) Change each placeholder "Click any ticker node to open chart" to "Click any ticker to open chart".

Delete the line `<script src="https://unpkg.com/cytoscape@3.30.4/dist/cytoscape.min.js"></script>` (line 947).

- [ ] **Step 4: Wire the three tabs**

In `docs/app.js`:

- `loadThemeData`: replace `renderThemeNetwork(radarVizSnap(current));` with `renderZoneMap(current, 'themes');`. In its `.catch`, replace the two `theme-network` lines with:
  ```js
        const tz = document.getElementById('themeviz-zone');
        if (tz) tz.innerHTML = '<div class="no-data">Theme data not available.</div>';
  ```
- `loadMomentumData`: replace `renderMomentumNetwork(current);` with `renderZoneMap(current, 'momentum');`. In its `.catch`, replace the `momentum-network` lines with the same pattern for `momentumviz-zone` and "Momentum data not available."
- `loadVolumeData`: replace `renderVolumeNetwork(current);` with `renderZoneMap(current, 'volume');`. In its `.catch`, replace the `volume-network` lines with the same pattern for `volumeviz-zone` and "Volume data not available."
- `onTimeTravelSelect`: replace `renderThemeNetwork(radarVizSnap(themeSnap), date);` with `renderZoneMap(themeSnap, 'themes', date);`, `renderMomentumNetwork(momSnap, date);` with `renderZoneMap(momSnap, 'momentum', date);`, and `renderVolumeNetwork(volumeSnap, date);` with `renderZoneMap(volumeSnap, 'volume', date);`.

- [ ] **Step 5: Delete the network code**

In `docs/app.js`:

- Delete from the line `  // ── NETWORK VIZ — Theme, Momentum, Volume, and VARS Viz ─────────────` down to, but not including, the line `  // ── THEMES TAB (L1 Radar) ─────────────────────────`. This removes `VIZ_MODES`, `vizState`, every helper and `actuallyRenderNetwork`.
- Delete `RADAR_VIZ_MEMBERS` with its comment, `radarVizSnap` with its comment, and `varsVizSnap` with its comment. Keep `radarL1s`; `renderThemes` uses it.
- In `COLOR_BANDS`, delete the `vizRs`, `vizVars` and `vizThemeStrength` tables and the two comments above them.
- Delete `VIZ_COLORS` with its comment, and `VIZ_HOT` with its comment.
- In `window.MM_COLORS`, delete `VIZ_COLORS, VIZ_HOT,` and the line `rsFill, varsFill, themeFill,`.

Then run:

Run: `git grep -n -E "rsFill|varsFill|themeFill|VIZ_MODES|vizState|syncTightPulses|filterAndRankThemes|computeAvgRs|actionabilityScore" -- docs`
Expected: no output. Remove any remaining caller you find.

- [ ] **Step 6: Delete the network CSS**

In `docs/style.css`, delete these rules: `.themeviz-card`, `.theme-network-wrap`, `.theme-network`, `.themeviz-overlay`, `@keyframes tight-pulse`, `.tight-pulse-ring`, every `.themeviz-tooltip …` rule (including `.themeviz-tooltip.lg-static-tip` near line 2502), and the comment header "THEME VIZ + MOMENTUM VIZ — Force-directed network tabs" (change it to "VIZ TABS — panel"). Keep `.viz-left`, `.viz-left .time-travel-bar` and every `.themeviz-legend …` rule.

- [ ] **Step 7: Delete the network legend**

In `docs/legends.js`:

- Delete the `{ id: 'viz', title: 'Network viz tabs', … }` section.
- Delete `const VIZ_TABS = …;` (keep the `ZONE_TABS`, `Z` and `zs` lines Task 5 added below it), `function ringsSvg()`, `function edgesSvg()`, `function themeStrengthTable(C)` and `function renderVizLegends()`, and the `renderVizLegends();` call.
- In the `dates` card, change `where: LIST_TABS.concat(['ep', 'themeviz', 'momentumviz', 'volumeviz', 'varsviz']),` to `where: LIST_TABS.concat(['ep'], ZONE_TABS),`.

Then find legend CSS classes that no card uses any more:

Run: `uv run python -c "import re,pathlib; css=pathlib.Path('docs/style.css').read_text(encoding='utf-8'); js=pathlib.Path('docs/legends.js').read_text(encoding='utf-8'); print(sorted(c for c in set(re.findall(r'\.(lg-[\w-]+)', css)) if not re.search(r'(?<![\w-])'+re.escape(c)+r'(?![\w-])', js)))"`
Expected: a list such as `lg-bridge`, `lg-edge-demo`, `lg-hex`, `lg-leader-dot`, `lg-pulse-demo`, `lg-static-tip`, `lg-tight`. Delete the CSS rules for each listed class. Keep a class that `docs/app.js` or `docs/index.html` still uses (check with `git grep`).

- [ ] **Step 8: Update the existing tests**

In `tests/test_dashboard_legends.py`:

- In `test_app_js_exports_the_registry`, change the key tuple to:
  ```python
        for key in ('COLOR_BANDS', 'pickBand', 'PATTERN_RULES', 'HL_TIER_CLASS',
                    'HL_TIER_TIP', 'CHART_MA_COLORS', 'MARKET_SESSIONS',
                    'ZONE_RULES', 'ZONE_STATES', 'ZONE_FAMILY_CLASS'):
  ```
- Delete `test_no_fill_function_returns_a_literal_colour` and `test_the_cytoscape_style_reads_every_colour_from_the_registry`.
- In `test_every_banded_column_calls_pickband`, delete the `'vizRs': 1, 'vizVars': 1,` and `'vizThemeStrength': 1,` entries.
- Replace `test_the_viz_keys_are_generated_not_hand_written` with:
  ```python
    def test_the_viz_keys_are_generated_not_hand_written(self):
        """Each zone map's key is drawn by legends.js from the same classes the
        map paints with. The hand-written network keys had drifted."""
        self.assertEqual(4, len(re.findall(r'data-viz-legend="zone"', HTML)))
        self.assertEqual(0, len(re.findall(r'data-viz-legend="(?:rs|vars)"', HTML)))
        self.assertIn('[data-viz-legend="zone"]', LEGENDS)
  ```

In `tests/test_dashboard_highlight_markup.py`, replace `test_no_viz_render_path_emits_a_tier` with:

```python
    def test_the_zone_scatter_emits_no_tier(self):
        """KD2. On the zone map a dot's colour already means its daily zone, so
        a tier on the dot would be a second meaning on the same channel. The
        list links under the map are ordinary .tn-link spans and DO carry the
        tier, like every other tab (zoneTickerLink is in RENDER_SITES)."""
        body = _strip_js_comments(_fn_body('zoneScatterCard'))
        for token in ('hlClass(', 'hlChipClass(', 'hlTitle(', 'HL_TIER_',
                      'hl-short', 'hl-ma-up', 'hl-ma-split'):
            self.assertNotIn(token, body, f'the zone scatter emits {token}')
```

If `_fn_body` cannot find `zoneScatterCard`, compare its lookup pattern with the function's declaration line and use the same form as the other `RENDER_SITES` entries.

- [ ] **Step 9: Run the tests to verify they pass**

Run: `uv run python -m unittest tests.test_dashboard_zone_map_markup tests.test_dashboard_legends tests.test_dashboard_highlight_markup -v`
Expected: all PASS.

Run: `uv run python -m unittest discover -s tests`
Expected: all PASS.

Run: `git grep -n -E "cytoscape|renderNetwork|radarVizSnap|varsVizSnap|VIZ_COLORS|VIZ_HOT|theme-network|-network\"" -- docs src tests`
Expected: no output.

- [ ] **Step 10: Commit**

```bash
git add docs/app.js docs/index.html docs/style.css docs/legends.js tests/test_dashboard_zone_map_markup.py tests/test_dashboard_legends.py tests/test_dashboard_highlight_markup.py
git commit -m "Move Theme, Momentum and Volume Viz to the zone map; remove the networks"
```

---

### Task 7: Docs, live check and payload size

**Files:**
- Modify: `CLAUDE.md`, `CONCEPTS.md`
- Create (scratch, not committed): `<scratchpad>/zone_payload_probe.py`

**Interfaces:**
- Consumes: everything above.
- Produces: docs that match the code; a browser check of all four tabs; payload sizes for the PR.

- [ ] **Step 1: Update CLAUDE.md**

Add a new section after "### Tight Range Marker — tight ranges on the Themes tab":

```markdown
### Zone Map — the four Viz tabs

`compute_weekly_ema` in `create_technical_indicators.py` (the `ema20_w` column) + `build_zone_map` and its helpers in `export_dashboard_data.py` (the four snapshot builders each write a `zone_map` block) + the ZONE MAP section and the `ZONE_RULES` / `ZONE_STATES` registry in `docs/app.js` (display). Tunables live in the `zone_map:` config block.

Theme Viz, VARS Viz, Momentum Viz and Volume Viz each plot their tab's names by two distances, both in daily ATR14: across, `(close − ema20) / atr14`; up, `(close − ema20_w) / atr14`. The server assigns every zone and one of seven states; the browser only draws. Below the map, a states card lists the names by state, and a clusters card ranks the leaves by how many names sit in the buy area.

| Tab | Universe (all with `avg_dollar_vol` ≥ $100M and `adr_pct` ≥ 0.04) | Row number |
|---|---|---|
| Theme Viz | members of the top `top_leaves` (20) radar leaves, uncapped | VARS |
| VARS Viz | the VARS tab's rendered tickers | VARS |
| Momentum Viz | the Momentum tab's tickers | RS_STS% |
| Volume Viz | the Volume tab's tickers | VARS |

- **⛔ The distances read the session's MASTER parquet, unfilled — never a screener parquet.** `run_screener.py` saves screener rows from a frame loaded with `.fillna(0)`, so a screener row holds `ema20_w = 0` where the master holds NaN, and `close / atr14` would plot the name far up the extended zone. `tests/test_zone_map_builders.py` writes its screener fixture the same zero-filled way to keep the trap real.
- **`ema20_w` uses a live weekly bar.** Each day reads the EMA of completed weeks, plus its own close as the current week: the value a TradingView weekly chart shows mid-week. It is NaN before 20 completed weeks, so a young listing is left off and counted in `n_no_weekly`. `tests/test_weekly_ema.py` pins the no-look-ahead property.
- **The floors fail CLOSED on a missing metric**, like `si_gate` and unlike the V/A cutoffs: they choose the universe, not a view.
- **Theme Viz reads uncapped leaves.** `radar_history.json` caps chips at `tickers_per_leaf` (10); a map built from the chips would show fewer names on a back-dated session.
- **RS_STS% is not a gate here.** It is the percentile of today's ticker/SPY ratio inside its own last 27 sessions, so a leader that pulls back into the daily band loses RS_STS% — the gate would remove the names the map exists to show. Measured 2026-10-08: RS_STS% ≥ 70 left 0 names under the daily band where VARS > 2 left 11.
- **A missing block says so.** A parquet predating `ema20_w` publishes `zone_map: null`, and the tab reads "Zone map data arrives with the next daily run" rather than drawing an empty chart.
- **Colours are existing tokens, painted by CSS class.** `zm-under` / `zm-band` / `zm-above` (red / green / amber) paint the dots, backgrounds, cluster bars and state rows; the tight ring is `--tight`, selection `--yellow`, rails grey. The list links are real `.tn-link` spans and carry the highlight tiers; the dots carry none (`test_the_zone_scatter_emits_no_tier`).
- **⛔ Nothing here measures whether the buy area predicts forward returns.** The limits (−0.5 / +1 daily, 0 / +4 weekly) come from a reference chart, and `top_leaves` is a choice. Measured 2026-10-08 on yfinance bars, over the 74 Themes-tab names with $100M, ADR 4% and VARS > 2: 25 in the buy area, 23 above +4 weekly. Re-measure on the published maps after the first daily run.
```

Then edit these existing passages:

- **L1 Radar**: replace the sentence that begins "The **Theme Viz** network is fed from the same radar snapshots via the client-side `radarVizSnap` adapter" (to the end of that sentence about `RADAR_VIZ_MEMBERS`) with: "The **Theme Viz** tab draws the radar snapshot's `zone_map` block (see Zone Map)."
- **VARS**: replace "The snapshot's `network` payload stays **leaf-level**; `varsVizSnap` in `docs/app.js` flattens L1-clustered snapshots for the shared network viz, and `renderVARS` falls back gracefully on legacy flat history entries." with "`renderVARS` falls back gracefully on legacy flat history entries. VARS Viz draws the snapshot's `zone_map` block (see Zone Map)."
- **Theme Taxonomy**: delete the sentence "The Theme Viz / VARS Viz networks render each L1 as a yellow hexagonal hub; leaf themes connect to their L1 via dashed `is_a` edges." In "Bare-L1 paths", replace the symptom sentence about the network viz with "Symptom: the leaf is a bare L1 name in every leaf list and cluster row." and delete the paragraph that begins "`_build_network` (`src/reporting/export_dashboard_data.py`) and the matching loop".
- **Highlight Tiers**: replace "The four Viz tabs carry no tint — node colour there already means theme strength and selection." with "On the four Viz tabs the dots carry no tint, because dot colour means the daily zone; the list links under each map carry the tier like every other tab."
- **Legends Tab**: replace the sentence that begins "It also writes the short key under each Viz network (`data-viz-legend`)." through "whose nodes are coloured by VARS." with "It also writes the short key under each zone map (`data-viz-legend=\"zone\"`)."
- **Dashboard Panel Layout**: after "Legends is one scrolling page with no chart (see Legends Tab below).", add "The Viz tabs' picture pane (`.viz-left`) defaults to 60% and scrolls."

- [ ] **Step 2: Update CONCEPTS.md**

Add two entries in the metrics part of `CONCEPTS.md`, in the file's existing entry style:

```markdown
- **Zone map** — the picture on the four Viz tabs. Each ticker sits at (close − daily EMA20) / ATR14 across and (close − weekly EMA20) / ATR14 up. See CLAUDE.md "Zone Map".
- **Buy area** — on the zone map, the daily band (−0.5 to +1 ATR from the daily EMA20) with the weekly between 0 and +4 ATR. A display zone, not a measured signal.
```

- [ ] **Step 3: Build a probe payload for the browser check**

`screening_output/` is absent in a worktree, so inject `zone_map` blocks into local copies of the four current snapshots. The script calls the shipped `compute_weekly_ema` and `build_zone_map`.

Create `<scratchpad>/zone_payload_probe.py`:

```python
"""Throwaway: inject zone_map blocks into docs/data from yfinance bars.

Calls the shipped compute_ema_pair, compute_weekly_ema and build_zone_map.
The bars run to the last close, which may be newer than the payload date;
that is acceptable for a render check. Reset with `git checkout -- docs/data/`.
"""
import json
from pathlib import Path

import pandas as pd
import yfinance as yf

from src.indicators.create_technical_indicators import compute_ema_pair, compute_weekly_ema
from src.reporting import export_dashboard_data as ex
from src.themes.theme_registry import load_ticker_themes

DATA = Path('docs/data')
FILES = ('radar.json', 'vars.json', 'momentum_136.json', 'volume.json')


def walk(o, out):
    if isinstance(o, dict):
        if isinstance(o.get('ticker'), str):
            out.setdefault(o['ticker'], o)
        for v in o.values():
            walk(v, out)
    elif isinstance(o, list):
        for v in o:
            walk(v, out)


snaps = {f: json.loads((DATA / f).read_text(encoding='utf-8')) for f in FILES}
radar_rows = [m for l1 in snaps['radar.json']['l1s'] for lf in l1['leaves']
              if lf['global_rank'] <= 20 for m in lf['tickers']]
rows_by_file = {'radar.json': radar_rows}
for f in FILES[1:]:
    found = {}
    walk({k: v for k, v in snaps[f].items() if k != 'network'}, found)
    rows_by_file[f] = list(found.values())

tight = {m['ticker']: m for l1 in snaps['radar.json']['l1s'] for lf in l1['leaves'] for m in lf['tickers']}
tickers = sorted({r['ticker'] for rows in rows_by_file.values() for r in rows})
raw = yf.download(tickers, period='2y', group_by='ticker', auto_adjust=False,
                  progress=False, threads=True)
bars = []
for t in tickers:
    try:
        df = raw[t].dropna(subset=['Close'])
    except KeyError:
        continue
    if len(df) < 30:
        continue
    c, h, l = df['Close'], df['High'], df['Low']
    tr = pd.concat([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)
    _, ema20 = compute_ema_pair(c)
    bars.append({'ticker': t, 'close': c.iloc[-1], 'ema20': ema20.iloc[-1],
                 'ema20_w': compute_weekly_ema(c).iloc[-1],
                 'atr14': tr.rolling(14, min_periods=1).mean().iloc[-1],
                 'tight_range': (tight.get(t) or {}).get('tight'),
                 'tight_len': (tight.get(t) or {}).get('tight_len')})
master = pd.DataFrame(bars)
leaves = load_ticker_themes()
for f in FILES:
    snap = snaps[f]
    before = len(json.dumps(snap.get('network'), separators=(',', ':'))) if 'network' in snap else 0
    snap.pop('network', None)
    snap['zone_map'] = ex.build_zone_map(rows_by_file[f], master, leaves)
    zm = snap['zone_map']
    size = len(json.dumps(zm, separators=(',', ':')))
    states = pd.Series([t['state'] for t in zm['tickers']]).value_counts().to_dict()
    print(f, 'names', len(zm['tickers']), 'zone_map bytes', size, 'network bytes', before, states)
    (DATA / f).write_text(json.dumps(snap, indent=2), encoding='utf-8')
```

Run: `uv run python <scratchpad>/zone_payload_probe.py`
Expected: four lines. Each prints the name count (about 82 / 70 / 58 / 26), the `zone_map` size, the removed `network` size and the state counts. Record the four lines for the PR.

- [ ] **Step 4: Check all four tabs in the browser**

1. Start the preview with `preview_start {name: "dashboard"}` (port 8777, from `.claude/launch.json`). Do not edit `.claude/launch.json`.
2. Open each of Theme Viz, VARS Viz, Momentum Viz and Volume Viz. Run `read_console_messages` with `onlyErrors: true`. Expected: no errors.
3. On each tab, confirm with `read_page`: the scatter card, the states card with seven rows, and the clusters card. Momentum Viz must print RS numbers; the other three print VARS.
4. Click a dot. Expected: the TradingView chart opens in the chart pane, and the dot and its list links turn yellow. Click a list link. Expected: the same.
5. Drag the resize handle. Expected: the SVG rescales and no label moves off its dot.
6. Take a screenshot of each tab for the PR.
7. Open the Legends tab. Expected: the "Zone map (Viz tabs)" section, and no "Network viz tabs" section.

Also check one back-dated session: pick an older date in the time-travel bar. Expected: "Zone map data arrives with the next daily run.", because the history files have no `zone_map` yet.

- [ ] **Step 5: Reset the probe data**

Run: `git checkout -- docs/data/`
Run: `git status --short`
Expected: only `CLAUDE.md` and `CONCEPTS.md` are modified.

- [ ] **Step 6: Run the full suite and commit**

Run: `uv run python -m unittest discover -s tests`
Expected: all PASS.

```bash
git add CLAUDE.md CONCEPTS.md
git commit -m "Document the zone map on the four Viz tabs"
```

---

## Self-review notes

- **Spec coverage.** Indicator (Task 1), distances, zones, states and config (Tasks 1-2), payload in all four builders (Tasks 3-4), uncapped Theme Viz (Task 4), renderer, layout at 60%, scatter, states and clusters (Task 5), colours and registry (Task 5), Legends (Tasks 5-6), removals (Tasks 3, 6), tests (all tasks), live check (Task 7), deployment-lag message (Task 5 test, Task 7 check), docs (Task 7). The PR itself is outside this plan; open it after Task 7 with the payload sizes and screenshots.
- **Names used across tasks.** `build_zone_map`, `zone_map_config`, `_load_zone_master`, `ZONE_STATES`, `ZONE_MAP_DEFAULTS` (Python); `renderZoneMap`, `ZONE_MODES`, `ZONE_RULES`, `ZONE_STATES`, `ZONE_FAMILY_CLASS`, `zoneTickerLink`, `zoneScatterCard`, `zoneNum`, `zoneTriangle`, `zmClamp`, `initZoneMaps`, `renderZoneKeys` (browser). Container ids are `<tab>-zone`.
