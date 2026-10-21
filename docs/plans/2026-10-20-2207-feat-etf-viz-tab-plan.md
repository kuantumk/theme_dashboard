# ETF Viz Tab Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an **ETF Viz** tab, directly right of **Lev ETF**, that puts each long leveraged ETF on the zone map and clusters the ETFs by theme.

**Architecture:** `fetch_etf_metrics` already downloads ETF bars from yfinance. It now also returns the four zone inputs (close, EMA20, weekly EMA20, ATR14) plus dollar volume and ADR. A new committed file, `config/etf_themes.yaml`, gives each sheet ETF a side (long / short / exclude) and a theme: the underlying stock's live tags, or an explicit taxonomy path. A new builder feeds the long ETFs to the existing `build_zone_map` and writes `docs/data/etf_viz.json` plus a forward-accumulating `etf_viz_history.json`. The browser adds a fifth `ZONE_MODES` entry and reuses `renderZoneMap` unchanged.

**Tech Stack:** Python 3.11, pandas, yfinance, PyYAML, unittest; vanilla JS/HTML/CSS in `docs/`.

**Spec:** The user's request in the session of 2026-10-20, with the four defaults the user accepted:
1. Inverse and volatility ETFs stay off the map. Only long ETFs plot.
2. Keep the 4% ADR floor. Use a lower dollar-volume floor for this tab only.
3. Rows that are not leveraged ETFs (CORD, HOOZ) stay off. ETFs with no theme map entry are logged and listed, not plotted.
4. The theme map lives in `config/etf_themes.yaml`. A test fails when a sheet ETF has no entry.

## Global Constraints

- Tab label: `ETF Viz`. Tab id: `etfviz`. Its button sits immediately after `<button … data-tab="etf" …>Lev ETF</button>` in `docs/index.html`.
- Layout: the same as the other four Viz tabs (`.viz-left` list pane holding `#etfviz-zone` and a `data-viz-legend="zone"` key; chart pane `#etfviz-chart-area`).
- Zone limits, states and colours come from the existing `zone_map:` block. Do not add a second copy.
- ETF dollar-volume floor: `etf_viz.min_avg_dollar_vol: 20000000` ($20M). ADR floor stays `zone_map.min_adr_pct` (0.04).
- Row number on the map: VARS (`metric: 'vars'`).
- Dollar volume = 20-session mean of `close × volume` with `min_periods=10`, the pipeline's `avg_dollar_vol` definition (`create_technical_indicators.py:652`).
- The yfinance fetch period in `fetch_etf_metrics` becomes `'2y'` (was `'8mo'`). See Task 2 for the reason.
- PR convention: do not commit regenerated `docs/data/*.json`. Delete any locally generated `docs/data/etf_viz*.json` before the commit.
- Prose (comments, docs, commit messages) follows ASD-STE100 per the user's CLAUDE.md.

## Measured facts this plan relies on (2026-10-20, yfinance, 2y bars)

- The sheet holds 74 rows: 47 long, 25 short or volatility, 2 not ETFs.
- At a $20M floor plus ADR ≥ 4%, **28 of 47** long ETFs pass. At $100M only 15 pass, which is why this tab gets its own floor.
- TQQQ (ADR 3.6%), AGQ (3.6%), TNA (3.9%), FAS (3.4%), AAPU (3.6%) and USO (2.9%) fail the ADR floor. That is expected, not a defect.
- BABA has no tag in `data/ticker_themes.json`, so BABX needs an explicit path.
- The taxonomy has no node for banks or homebuilders. DPST, FAS and NAIL plot with no theme.

## Review Focus

1. **The sheet adds a ticker the map does not know.** Expected: the ETF does not plot (its side is unknown, so it could be inverse), the payload lists it under `unmapped`, the export logs a warning, and the tab prints the list under the map. Test in Task 3 (`test_an_unknown_etf_is_listed_not_plotted`) and Task 4 (`test_the_unmapped_note_renders`).
2. **yfinance returns no bars at all (outage).** Expected: no `etf_viz.json` is written, so the tab keeps the last snapshot under its own date. An empty map must not read as a quiet market. Test in Task 3 (`test_no_bars_builds_nothing`) and the export wiring step.
3. **A frame with no `Volume` column** (the existing `_etf_frame` test fixture has none). Expected: `dollar_vol` is None, the ETF fails the floor and counts in `n_floor_dropped`. Nothing raises. Test in Task 2 (`test_a_frame_without_volume_has_no_dollar_vol`).
4. **A young ETF with fewer than 20 completed weeks** (STXL has 125 bars today). Expected: `ema20_w` is None and the ETF counts in `n_no_weekly`. Test in Task 2 (`test_fewer_than_20_weeks_has_no_weekly_ema`).
5. **A single-stock ETF whose underlying is retagged.** Expected: the ETF's leaves follow the underlying's new tags on the next run with no edit to `etf_themes.yaml`. Test in Task 1 (`test_an_underlying_retag_flows_through`).

---

### Task 1: The ETF theme map

**Files:**
- Create: `config/etf_themes.yaml`
- Modify: `src/reporting/export_dashboard_data.py` (new section after `build_zone_map`, ~line 1488)
- Test: `tests/test_etf_themes.py`

**Interfaces:**
- Produces:
  - `ETF_THEMES_FILE: Path` = `PROJECT_ROOT / 'config' / 'etf_themes.yaml'`
  - `ETF_SIDES = ('long', 'short', 'exclude')`
  - `load_etf_theme_map(path: Path = ETF_THEMES_FILE) -> dict[str, dict]` — upper-case ticker → entry dict. A missing file returns `{}`.
  - `etf_leaves(entry: dict, ticker_themes: dict[str, list[str]]) -> list[str] | None` — the entry's `themes` list when the key is present (it may be `[]`); else the underlying's tags from `ticker_themes`; else None (unmapped).

- [ ] **Step 1: Write `config/etf_themes.yaml`**

Header comment (STE): what the file is, the three sides, that `underlying` reads live tags and `themes` overrides them, that `themes: []` plots with no cluster, and that a bare L1 is allowed here for a broad-sector fund only (LABU), unlike `ticker_themes.json`. Content:

```yaml
# Long, single stock: the theme is the underlying's live tag.
AMDL: {side: long, underlying: AMD}
TSLL: {side: long, underlying: TSLA}
NVDL: {side: long, underlying: NVDA}
MSTU: {side: long, underlying: MSTR}
PLTU: {side: long, underlying: PLTR}
APPX: {side: long, underlying: APP}
NFLU: {side: long, underlying: NFLX}
MSFU: {side: long, underlying: MSFT}
UBRL: {side: long, underlying: UBER}
SMCX: {side: long, underlying: SMCI}
RGTX: {side: long, underlying: RGTI}
CRWG: {side: long, underlying: CRWV}
AVGX: {side: long, underlying: AVGO}
ROBN: {side: long, underlying: HOOD}
BABX: {side: long, underlying: BABA, themes: ["Geographic / China"]}   # BABA is untagged
CONL: {side: long, underlying: COIN}
HIMZ: {side: long, underlying: HIMS}
IONX: {side: long, underlying: IONQ}
RDTL: {side: long, underlying: RDDT}
FBL:  {side: long, underlying: META}
CRWL: {side: long, underlying: CRWD}
NBIL: {side: long, underlying: NBIS}
AMZU: {side: long, underlying: AMZN}
TEMT: {side: long, underlying: TEM}
GGLL: {side: long, underlying: GOOGL}
AAPU: {side: long, underlying: AAPL}
SNXX: {side: long, underlying: SNDK}
MULL: {side: long, underlying: MU}
STXL: {side: long, underlying: STX}
INTW: {side: long, underlying: INTC}
WDCX: {side: long, underlying: WDC}
QCML: {side: long, underlying: QCOM}
# Long, sector / commodity / crypto: an explicit path.
ETHU: {side: long, themes: ["Fintech & Crypto / Crypto / Generalist"]}
BITX: {side: long, themes: ["Fintech & Crypto / Crypto / Bitcoin Proxy"]}
NUGT: {side: long, themes: ["Metals & Mining / Gold"]}
AGQ:  {side: long, themes: ["Metals & Mining / Silver"]}
BOIL: {side: long, themes: ["Oil & Gas / LNG & Natural Gas"]}
GUSH: {side: long, themes: ["Oil & Gas / E&P"]}
USO:  {side: long, themes: ["Oil & Gas / E&P"]}
LABU: {side: long, themes: ["Biotech"]}
UTSL: {side: long, themes: ["Power & Utilities / Diversified Utilities"]}
SOXL: {side: long, themes: ["AI / Data Center / Chips & Processors"]}
# Long, no taxonomy node: plots with no cluster.
TQQQ: {side: long, themes: []}
TNA:  {side: long, themes: []}
FAS:  {side: long, themes: []}
DPST: {side: long, themes: []}
NAIL: {side: long, themes: []}
# Short or volatility: off the map.
GGLS: {side: short}
AAPD: {side: short}
AMZD: {side: short}
NVD:  {side: short}
METD: {side: short}
SMCZ: {side: short}
RGTZ: {side: short}
IONZ: {side: short}
AMDD: {side: short}
DAMD: {side: short}
TSLQ: {side: short}
BITI: {side: short}
ETHD: {side: short}
MSTZ: {side: short}
PLTZ: {side: short}
MSFD: {side: short}
DUST: {side: short}
ZSL:  {side: short}
SBIT: {side: short}
SQQQ: {side: short}
LABD: {side: short}
TZA:  {side: short}
SOXS: {side: short}
DRIP: {side: short}
UVIX: {side: short}   # long VIX: it rises when the market falls
# Not a leveraged ETF.
CORD: {side: exclude}   # Cordiant Digital Infrastructure, a company
HOOZ: {side: exclude}   # the sheet reads #N/A
```

- [ ] **Step 2: Write the failing tests in `tests/test_etf_themes.py`**

```python
class EtfThemeMapTests(unittest.TestCase):
    def test_every_sheet_etf_has_an_entry(self):
        # docs/data/etf_data.json is the committed copy of the sheet.
        sheet = {r['ticker'].upper() for r in json.load(open(ROOT / 'docs/data/etf_data.json'))}
        self.assertEqual(set(), sheet - set(ex.load_etf_theme_map()))
    def test_every_side_is_known(self):
        for t, e in ex.load_etf_theme_map().items():
            self.assertIn(e.get('side'), ex.ETF_SIDES, t)
    def test_every_explicit_path_is_in_the_taxonomy(self):
        for t, e in ex.load_etf_theme_map().items():
            for p in e.get('themes') or []:
                self.assertTrue(validate_path(p), f'{t}: {p}')
    def test_every_long_entry_resolves_to_a_theme_list(self):
        tt = load_ticker_themes()
        for t, e in ex.load_etf_theme_map().items():
            if e['side'] == 'long':
                self.assertIsNotNone(ex.etf_leaves(e, tt), t)
    def test_explicit_themes_win_over_the_underlying(self):
        e = {'side': 'long', 'underlying': 'BABA', 'themes': ['Geographic / China']}
        self.assertEqual(['Geographic / China'], ex.etf_leaves(e, {'BABA': ['AI / Software & Analytics']}))
    def test_an_empty_theme_list_is_kept_not_unmapped(self):
        self.assertEqual([], ex.etf_leaves({'side': 'long', 'themes': []}, {}))
    def test_an_untagged_underlying_with_no_themes_is_unmapped(self):
        self.assertIsNone(ex.etf_leaves({'side': 'long', 'underlying': 'ZZZZ'}, {}))
    def test_an_underlying_retag_flows_through(self):
        e = {'side': 'long', 'underlying': 'NVDA'}
        self.assertEqual(['Robotics / Humanoid'], ex.etf_leaves(e, {'NVDA': ['Robotics / Humanoid']}))
    def test_a_missing_file_is_an_empty_map(self):
        self.assertEqual({}, ex.load_etf_theme_map(Path('no/such/file.yaml')))
```

`validate_path` from `src.themes.theme_taxonomy`; `load_ticker_themes` from `src.themes.theme_registry`.

- [ ] **Step 3: Run the tests to verify they fail**

Run: `uv run python -m unittest tests.test_etf_themes -v`
Expected: FAIL with `AttributeError: module … has no attribute 'load_etf_theme_map'`.

- [ ] **Step 4: Implement `ETF_THEMES_FILE`, `ETF_SIDES`, `load_etf_theme_map`, `etf_leaves`**

`yaml.safe_load`; upper-case the keys. `etf_leaves` reads `entry['themes']` when `'themes' in entry`, else `ticker_themes.get(entry.get('underlying', '').upper())`, returning None for a missing or empty tag list.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run python -m unittest tests.test_etf_themes -v`
Expected: 9 tests, OK.

- [ ] **Step 6: Commit**

```bash
git add config/etf_themes.yaml src/reporting/export_dashboard_data.py tests/test_etf_themes.py
git commit -m "Map each leveraged ETF to a side and a theme"
```

---

### Task 2: Zone inputs from the ETF fetch

**Files:**
- Modify: `src/reporting/export_dashboard_data.py` — imports (~line 24) and `fetch_etf_metrics` (~lines 992–1110)
- Test: `tests/test_etf_viz.py` (new; reuse the `_etf_frame` / `_etf_metrics` shape from `tests/test_highlight_publishing.py:407-423`, adding an optional `volume` argument)

**Interfaces:**
- Consumes: `compute_weekly_ema(close, span, min_weeks)` and `weekly_ema_config() -> {'span', 'min_weeks'}` from `src.indicators.create_technical_indicators`.
- Produces: each `fetch_etf_metrics` value gains a `'zone'` key:
  `{'close': float, 'ema20': float, 'ema20_w': float|None, 'atr14': float, 'dollar_vol': float|None, 'adr_pct': float}`.
  The existing `vars` / `color` / `highlight` keys keep their meaning.

- [ ] **Step 1: Write the failing tests**

```python
class EtfZoneInputTests(unittest.TestCase):
    def test_a_long_series_carries_all_zone_inputs(self):
        z = _etf_metrics(_etf_frame([100.0 + i * 0.1 for i in range(400)], volume=1_000_000))['TQQQ']['zone']
        for key in ('close', 'ema20', 'ema20_w', 'atr14', 'dollar_vol', 'adr_pct'):
            self.assertIsNotNone(z[key], key)
    def test_dollar_vol_is_the_20_session_mean_of_close_times_volume(self):
        closes = [100.0] * 380 + [200.0] * 20
        z = _etf_metrics(_etf_frame(closes, volume=1_000))['TQQQ']['zone']
        self.assertAlmostEqual(200_000.0, z['dollar_vol'])
    def test_a_frame_without_volume_has_no_dollar_vol(self):
        z = _etf_metrics(_etf_frame([100.0 + i for i in range(400)]))['TQQQ']['zone']
        self.assertIsNone(z['dollar_vol'])
    def test_fewer_than_20_weeks_has_no_weekly_ema(self):
        z = _etf_metrics(_etf_frame([100.0 + i for i in range(120)]))['TQQQ']['zone']   # 120 calendar days ≈ 17 weeks
        self.assertIsNone(z['ema20_w'])
    def test_the_weekly_ema_matches_the_pipeline_helper(self):
        frame = _etf_frame([100.0 + (i % 30) for i in range(400)], volume=1_000)
        want = compute_weekly_ema(frame['Close'], 20, 20).iloc[-1]
        self.assertAlmostEqual(want, _etf_metrics(frame)['TQQQ']['zone']['ema20_w'])
    def test_the_fetch_asks_for_two_years(self):
        # patch yfinance.download with a Mock, call fetch_etf_metrics, then:
        self.assertEqual('2y', mock.call_args.kwargs['period'])
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run python -m unittest tests.test_etf_viz -v`
Expected: FAIL with `KeyError: 'zone'`.

- [ ] **Step 3: Extend `fetch_etf_metrics`**

- Change `period='8mo'` to `period='2y'`, and rewrite the comment above it. **Why 2y:** the weekly EMA20 seeds on the first week. Over 8 months (~34 weeks) the seed keeps ~3% weight (`(19/21)^34`), and on a 2x ETF that moves the weekly distance visibly. Over 2 years (~100 weeks) the weight is below 0.01%, which matches the pipeline's 500-day history. VARS, ATR14, EMA10/20, SMA50 and ADR read only recent windows, so they do not change.
- `ema20_w` = `compute_weekly_ema(close, **weekly_ema_config()).iloc[-1]`, None when not finite.
- `dollar_vol` = `(close * volume).rolling(20, min_periods=10).mean().iloc[-1]` when `'Volume' in df`, else None.
- `zone` = the dict above, from the last bar. Update the docstring's return shape.
- The skip test (`if vars_value is None and color is None and highlight is None`) also keeps a ticker whose `zone` has a finite `close`.

- [ ] **Step 4: Run the new and the existing ETF tests**

Run: `uv run python -m unittest tests.test_etf_viz tests.test_highlight_publishing -v`
Expected: all OK. The `EtfHighlightTests` cases still pass unchanged.

- [ ] **Step 5: Commit**

```bash
git add src/reporting/export_dashboard_data.py tests/test_etf_viz.py
git commit -m "Return zone-map inputs from the ETF fetch"
```

---

### Task 3: The ETF Viz snapshot and its export

**Files:**
- Modify: `config/workflow_config.yaml` — new `etf_viz:` block after `zone_map:` (~line 116)
- Modify: `src/reporting/export_dashboard_data.py` — new builder after `etf_leaves`; `export_all` step 4c (~line 2912)
- Test: `tests/test_etf_viz.py`

**Interfaces:**
- Consumes: `build_zone_map(rows, master_df, leaves_by_ticker, cfg)`, `zone_map_config()`, `_update_history_file(path, report_date, entry)`, Task 1's `load_etf_theme_map` / `etf_leaves`, Task 2's `'zone'` dict.
- Produces:
  - `etf_viz_config() -> dict` — `zone_map_config()` with `min_avg_dollar_vol` replaced by `CONFIG['etf_viz']['min_avg_dollar_vol']` (default `20_000_000`).
  - `build_etf_viz_snapshot(etf_rows: list[dict], metrics: dict, theme_map: dict, ticker_themes: dict, report_date: str, cfg: dict | None = None) -> dict | None`
  - Payload: `{'report_date': str, 'zone_map': {...build_zone_map block..., 'n_short': int, 'n_excluded': int, 'unmapped': [str]}}`. `params.min_avg_dollar_vol` carries the ETF floor.
  - Files: `docs/data/etf_viz.json` and `docs/data/etf_viz_history.json` (list of payloads, newest first).

- [ ] **Step 1: Add the config block**

```yaml
# ETF Viz tab — the long leveraged ETFs on the zone map. Zone limits and the
# ADR floor come from zone_map:. Only the dollar floor differs: at $100M,
# 15 of 47 long ETFs pass; at $20M, 28 pass (measured 2026-10-20).
etf_viz:
  min_avg_dollar_vol: 20000000
```

- [ ] **Step 2: Write the failing tests**

Fixture: `metrics` built by hand as `{ticker: {'vars': v, 'color': None, 'highlight': None, 'zone': {...}}}` with zone values that land in the buy area (close 100, ema20 99, ema20_w 95, atr14 2, dollar_vol 30e6, adr_pct 0.05). `etf_rows` = `[{'ticker': t, 'rs_sts': 80.0}]`. Theme map holds `NVDL` (long, underlying NVDA), `TQQQ` (long, `themes: []`), `SQQQ` (short), `CORD` (exclude).

```python
class EtfVizSnapshotTests(unittest.TestCase):
    def test_a_long_etf_plots_with_its_underlying_theme(self):
        t = _tickers(snap)['NVDL']
        self.assertEqual('buy_area', t['state'])
        self.assertEqual(['AI / Data Center / Chips & Processors'], t['leaves'])
    def test_a_long_etf_with_no_theme_still_plots(self):
        self.assertEqual([], _tickers(snap)['TQQQ']['leaves'])
    def test_short_and_excluded_etfs_stay_off_and_are_counted(self):
        self.assertNotIn('SQQQ', _tickers(snap)); self.assertNotIn('CORD', _tickers(snap))
        self.assertEqual(1, snap['zone_map']['n_short']); self.assertEqual(1, snap['zone_map']['n_excluded'])
    def test_an_unknown_etf_is_listed_not_plotted(self):
        # add 'NEWX' to etf_rows and metrics, not to the theme map
        self.assertNotIn('NEWX', _tickers(snap)); self.assertEqual(['NEWX'], snap['zone_map']['unmapped'])
    def test_the_etf_floor_is_20m_and_is_published(self):
        # NVDL dollar_vol 15e6 → dropped; 25e6 → plots
        self.assertEqual(20_000_000, snap['zone_map']['params']['min_avg_dollar_vol'])
    def test_the_adr_floor_is_the_shared_one(self):
        # adr_pct 0.035 → dropped, counted in n_floor_dropped
    def test_the_row_number_is_vars_and_rs_reads_rs_sts(self):
        t = _tickers(snap)['NVDL']; self.assertEqual(round(vars_in, 2), t['vars']); self.assertEqual(80.0, t['rs'])
    def test_no_bars_builds_nothing(self):
        self.assertIsNone(ex.build_etf_viz_snapshot(rows, {}, theme_map, tt, '2026-10-20'))
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `uv run python -m unittest tests.test_etf_viz -v`
Expected: the `EtfVizSnapshotTests` cases FAIL with `AttributeError: … 'build_etf_viz_snapshot'`.

- [ ] **Step 4: Implement `etf_viz_config` and `build_etf_viz_snapshot`**

- Build a frame with columns `ticker, close, ema20, ema20_w, atr14` from every `metrics[t]['zone']`. NaN for a None `ema20_w`, so `build_zone_map` counts it in `n_no_weekly`. No frame rows → return None.
- Walk `etf_rows` in order. Entry None → append to `unmapped`. `side == 'short'` → `n_short += 1`. `side == 'exclude'` → `n_excluded += 1`. Long with `etf_leaves(...) is None` → `unmapped`. Long otherwise → a row `{ticker, vars, rs: row['rs_sts'], highlight, ticker_color, dollar_vol, adr_pct}` from the row and `metrics`.
- Call `build_zone_map(rows, frame, leaves_by_ticker, cfg or etf_viz_config())`. Return None when it returns None. Add the three keys, sort `unmapped`.
- When `unmapped` is not empty, print one line: `   Warning: no config/etf_themes.yaml entry for: A, B`.

- [ ] **Step 5: Wire it into `export_all` step 4c**

After `update_etf_history(...)`, when `etf_data` and `etf_metrics` and `report_date` are all present:

```python
from src.themes.theme_registry import load_ticker_themes
snap = build_etf_viz_snapshot(etf_data, etf_metrics, load_etf_theme_map(),
                              load_ticker_themes(), report_date)
```

When `snap` is None, print a warning and write nothing. Otherwise write `OUTPUT_DIR / 'etf_viz.json'` (`indent=2`) and call `_update_history_file(OUTPUT_DIR / 'etf_viz_history.json', report_date, snap)`. Hoist `etf_metrics = {}` above the `if all_etf_tickers:` block so the name always exists.

The history grows forward, one entry per run, like `etf_data_history.json`. Do not rebuild it: the sheet's roster and the theme map are today's, and pinning them on old bars would publish a back-dated universe that never existed.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `uv run python -m unittest tests.test_etf_viz tests.test_zone_map tests.test_zone_map_builders -v`
Expected: all OK.

- [ ] **Step 7: Commit**

```bash
git add config/workflow_config.yaml src/reporting/export_dashboard_data.py tests/test_etf_viz.py
git commit -m "Publish the ETF Viz zone map from the daily export"
```

---

### Task 4: The ETF Viz tab in the dashboard

**Files:**
- Modify: `docs/index.html` — tab button after line 52; new `content-etfviz` block after the Lev ETF content block (copy the Volume Viz block at lines 586–606, renaming `volumeviz` → `etfviz`)
- Modify: `docs/app.js` — URL constants (~line 25), `activeCharts` (79), `navIndices` (338), a new `loadETFVizData`, its call beside `loadETFData`, `onTimeTravelSelect` (~1550), `renderAllTimeTravelBars` (~1569), `ZONE_MODES` (1665), `renderZoneMap` (2105)
- Modify: `docs/legends.js` — `ZONE_TABS` (523), the zone section intro (809)
- Test: `tests/test_dashboard_zone_map_markup.py`, `tests/test_dashboard_legends.py`

**Interfaces:**
- Consumes: Task 3's `docs/data/etf_viz.json` / `etf_viz_history.json` payload.
- Produces: `ZONE_MODES.etf = { containerId: 'etfviz-zone', tabId: 'etfviz', metric: 'vars' }`; `ETF_VIZ_DATA_URL = 'data/etf_viz.json'`; `ETF_VIZ_HISTORY_URL = 'data/etf_viz_history.json'`; `let etfVizHistory = []`.

- [ ] **Step 1: Update the failing markup tests**

- `tests/test_dashboard_zone_map_markup.py`: `ZONE_TABS = ('themeviz', 'varsviz', 'momentumviz', 'volumeviz', 'etfviz')`. Add:

```python
    def test_etf_viz_sits_right_of_lev_etf(self):
        buttons = re.findall(r'data-tab="(\w+)"[^>]*>([^<]+)</button>', HTML)
        ids = [b[0] for b in buttons]
        self.assertEqual(ids.index('etf') + 1, ids.index('etfviz'))
        self.assertEqual('ETF Viz', dict(buttons)['etfviz'])
    def test_etf_mode_prints_vars(self):
        self.assertRegex(APP, r"etf:\s*\{\s*containerId: 'etfviz-zone', tabId: 'etfviz', metric: 'vars' \}")
    def test_the_unmapped_note_renders(self):
        body = _fn('renderZoneMap')
        self.assertIn('zm.unmapped', body); self.assertIn('etf_themes.yaml', body)
```

- `tests/test_dashboard_legends.py:269`: change the expected count from `4` to `5`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run python -m unittest tests.test_dashboard_zone_map_markup tests.test_dashboard_legends -v`
Expected: FAIL on the missing `content-etfviz` block, the button order, and the count of 5.

- [ ] **Step 3: Add the tab markup**

Button: `<button class="tab-btn" data-tab="etfviz" id="tab-etfviz">ETF Viz</button>` right after the Lev ETF button. Content block: the Volume Viz block with every `volumeviz` replaced by `etfviz` and the comment banner `TAB: ETF VIZ`.

- [ ] **Step 4: Wire the tab in `docs/app.js`**

- Add the two URL constants. Add `etfviz: null` to `activeCharts` and `etfviz: -1` to `navIndices`.
- `loadETFVizData()`: the `loadVolumeData` pattern — fetch current and history, merge by `report_date`, sort newest first into `etfVizHistory`, then `renderAllTimeTravelBars()` and `renderZoneMap(current, 'etf')`. On failure, write `<div class="no-data">ETF Viz data not available.</div>` into `#etfviz-zone`. Call it where `loadETFData()` is called.
- `onTimeTravelSelect`: `renderZoneMap(etfVizHistory.find(h => h.report_date === date), 'etf', date)`.
- `renderAllTimeTravelBars`: `renderTimeTravelBar('etfviz-tt-dates', tabSessionDates(etfVizHistory), onTimeTravelSelect)`.
- `ZONE_MODES`: add the `etf` entry. Update the section banner from "the four Viz tabs" to "the five Viz tabs".
- `renderZoneMap`: after the clusters card, when `zm.unmapped && zm.unmapped.length`, append `<p class="zm-foot">Not on the map, no entry in config/etf_themes.yaml: A, B.</p>` (escaped). The other four payloads carry no `unmapped`, so they render as before.

- [ ] **Step 5: Update `docs/legends.js`**

Add `'etfviz'` to `ZONE_TABS`. Change the zone intro to "The five Viz tabs …". Add one sentence to the `zone-dots` card body: "ETF Viz plots only long ETFs, and its dollar-volume floor is lower than the other tabs'."

- [ ] **Step 6: Run the dashboard tests**

Run: `uv run python -m unittest tests.test_dashboard_zone_map_markup tests.test_dashboard_legends tests.test_dashboard_panel_layout tests.test_dashboard_highlight_markup -v`
Expected: all OK.

- [ ] **Step 7: Commit**

```bash
git add docs/index.html docs/app.js docs/legends.js tests/test_dashboard_zone_map_markup.py tests/test_dashboard_legends.py
git commit -m "Add the ETF Viz tab beside Lev ETF"
```

---

### Task 5: Live check, docs and the full suite

**Files:**
- Modify: `CLAUDE.md` — the "Zone Map — the four Viz tabs" section
- Scratch (not committed): a script in the session scratchpad

- [ ] **Step 1: Generate a real snapshot locally**

Write a scratch script that: fetches the sheet with `ex.fetch_etf_data()`; downloads SPY 2y from yfinance, lower-cases its columns and passes it to `compute_spy_cum_norm_100`; calls `ex.fetch_etf_metrics`, `ex.enrich_etf_with_metrics`, then `ex.build_etf_viz_snapshot` with today's date; writes `docs/data/etf_viz.json` and calls `_update_history_file` for `etf_viz_history.json`. Run it with `PYTHONPATH=.`.
Expected: about 28 plotted tickers (± a few as volume moves), 25 in `n_short`, 2 in `n_excluded`, `unmapped` empty. Record the real counts for Step 3.

- [ ] **Step 2: Check the tab in the browser**

`preview_start {name: "dashboard"}`. Open ETF Viz. Confirm:
- The button sits right of Lev ETF.
- The map draws dots, the states card lists ETFs, the clusters card shows leaves such as `AI / Data Center / Memory` (MULL, SNXX, WDCX, STXL).
- A dot click and a list click open the TradingView chart on the right.
- The time-travel bar shows today's date.
- No console errors. The other four Viz tabs still draw.
Take one screenshot for the user.

- [ ] **Step 3: Update `CLAUDE.md`**

Rename the section to "Zone Map — the five Viz tabs". Add an ETF Viz row to the universe table: *long ETFs in `config/etf_themes.yaml`, `avg dollar vol` ≥ $20M, ADR ≥ 4%; row number VARS*. Add a bullet that states: the bars come from the yfinance ETF fetch (2y, not the master parquet); themes come from the underlying's live tags or an explicit path; inverse ETFs stay off; an unknown sheet ETF is listed under the map, not plotted; history grows forward only. Quote the Step 1 counts with the date.

- [ ] **Step 4: Run the full suite**

Run: `uv run python -m unittest discover -s tests`
Expected: OK, with no new failures against `main`.

- [ ] **Step 5: Remove the local data and commit**

```bash
rm docs/data/etf_viz.json docs/data/etf_viz_history.json
git checkout -- docs/data/
git status --short docs/data/    # expect no output
git add CLAUDE.md
git commit -m "Document the ETF Viz tab"
```
