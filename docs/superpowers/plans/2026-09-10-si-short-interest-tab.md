# SI Tab Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an **SI** dashboard tab that lists highly shorted stocks which have already sold off, grouped under theme L1 sections and ranked by short interest.

**Architecture:** A new daily-workflow step fetches one Finviz Ownership screener page set into `data/short_interest.json`. Two new technical-indicator columns supply the selloff legs. An export function joins the two, gates, groups by taxonomy L1, scores each L1 as the mean of its top-3 member short-interest values, and writes `docs/data/si.json` plus a forward-accumulating `si_history.json`. The dashboard renders it with the VARS tab's L1-section shape.

**Tech Stack:** Python 3.11, pandas, finvizfinance, uv. Vanilla JS + CSS for `docs/`. `unittest` for tests.

**Spec:** `docs/superpowers/specs/2026-09-10-si-short-interest-tab-design.md`

## Global Constraints

- Run every command with `uv run` from the worktree root. Bare `python` is the Windows Store stub.
- Prose in code comments and docs follows ASD-STE100 + Zinsser (see `~/.claude/CLAUDE.md`): active voice, one idea per sentence, 20–25 words maximum.
- Never commit `docs/data/*.json`. After any local export run, `git checkout -- docs/data/` before committing.
- `screening_output/` is never committed.
- Config values, copied verbatim from the spec: `min_short_interest: 12.0`, `max_drawdown_60d: -0.25`, `max_drop_15d: -0.25`, `min_tickers_per_l1: 3`, `top_k_si: 3`, `hot_radar_rank: 10`.
- The SI tab carries **no** V/A cutoff dropdowns. `tests/test_dashboard_filter_markup.py::EXPECTED_BAR_COUNT` stays at 5.
- Tests run with `uv run python -m unittest discover -s tests`.

---

### Task 1: Ticker-repair view factory

Finviz doubles the first character of every ticker. `repair_ticker_column` already fixes this, but `_TickerRepairOverview` is welded to the `Overview` view. The SI module needs the same repair on the `Ownership` view.

**Files:**
- Modify: `src/reporting/ep_scan_common.py:113-127` (replace `_TickerRepairOverview` with a factory)
- Test: `tests/test_ep_scan_ticker_parsing.py`

**Interfaces:**
- Produces: `make_ticker_repair_view(base_view_cls)` → a subclass of `base_view_cls` that repairs tickers after each page and counts repairs on `self.tickers_repaired`.
- Produces: `_TickerRepairOverview` stays as `make_ticker_repair_view(Overview)` so existing EP call sites are untouched.

- [ ] **Step 1: Write the failing test**

```python
def test_factory_repairs_tickers_on_any_view_class(self):
    """The repair must not be welded to Overview — SI uses Ownership."""
    from finvizfinance.screener.ownership import Ownership
    from src.reporting.ep_scan_common import make_ticker_repair_view

    cls = make_ticker_repair_view(Ownership)
    self.assertTrue(issubclass(cls, Ownership))
    self.assertEqual(cls().tickers_repaired, 0)
```

- [ ] **Step 2: Run it and watch it fail**

`uv run python -m unittest tests.test_ep_scan_ticker_parsing -v`
Expected: `ImportError: cannot import name 'make_ticker_repair_view'`

- [ ] **Step 3: Implement the factory**

Replace the `class _TickerRepairOverview(Overview)` block with:

```python
def make_ticker_repair_view(base_view_cls):
    """Build a screener view that repairs Finviz's doubled ticker characters.

    The repair is needed on every screener view, not just Overview: the SI
    collector reads the Ownership view and sees the same corruption.
    """

    class _TickerRepairView(base_view_cls):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.tickers_repaired = 0

        def _get_table(self, rows, df, num_col_index, table_header, limit=-1):
            table = super()._get_table(rows, df, num_col_index, table_header, limit)
            # Mirror the parent's row slicing so offsets line up.
            data_rows = rows[1:]
            if limit != -1:
                data_rows = data_rows[0:limit]
            self.tickers_repaired += repair_ticker_column(table, data_rows)
            return table

    _TickerRepairView.__name__ = f"_TickerRepair{base_view_cls.__name__}"
    return _TickerRepairView


if FINVIZ_AVAILABLE:
    _TickerRepairOverview = make_ticker_repair_view(Overview)
```

Move `make_ticker_repair_view` above the `if FINVIZ_AVAILABLE:` guard so it imports even when finvizfinance is absent.

- [ ] **Step 4: Run the full EP test module**

`uv run python -m unittest tests.test_ep_scan_ticker_parsing -v`
Expected: PASS, including the pre-existing repair tests.

- [ ] **Step 5: Commit**

```bash
git add src/reporting/ep_scan_common.py tests/test_ep_scan_ticker_parsing.py
git commit -m "Make Finviz ticker repair reusable across screener views"
```

---

### Task 2: `drop_15d` and `down_streak` indicators

**Files:**
- Modify: `src/indicators/create_technical_indicators.py` (near the `close_vs_252h` block, ~line 215)
- Create: `tests/test_drop_15d.py`

**Interfaces:**
- Produces: `compute_drop_15d(close, window=15, lookback=45) -> pd.Series`
- Produces: `compute_down_streak(close) -> pd.Series` (integer count of consecutive down closes ending at each bar)
- Produces: two new columns on every ticker frame in `price_daily_ta.pkl`, and therefore on every master parquet: `drop_15d`, `down_streak`.

- [ ] **Step 1: Write the failing tests**

```python
class Drop15dTests(unittest.TestCase):
    def test_recovers_a_past_crash_that_todays_bar_cannot_see(self):
        """A 50% fall followed by 20 flat bars is still a broken chart."""
        close = pd.Series([100] * 10 + [50] + [50] * 25)
        out = compute_drop_15d(close)
        self.assertAlmostEqual(out.iloc[-1], -0.5, places=6)

    def test_never_reads_shallower_than_the_per_bar_form(self):
        """Property: the rolling minimum is a lower bound on today's bar."""
        rng = np.random.default_rng(0)
        close = pd.Series(100 * np.exp(np.cumsum(rng.normal(0, 0.03, 200))))
        per_bar = close / close.rolling(16, min_periods=2).max() - 1
        out = compute_drop_15d(close)
        self.assertTrue((out.dropna() <= per_bar.reindex(out.dropna().index) + 1e-12).all())

    def test_drop_ages_out_of_the_lookback(self):
        close = pd.Series([100] * 5 + [50] + [50] * 60)
        self.assertAlmostEqual(compute_drop_15d(close).iloc[-1], 0.0, places=6)


class DownStreakTests(unittest.TestCase):
    def test_counts_consecutive_down_closes(self):
        close = pd.Series([10, 9, 8, 7, 8, 7, 6])
        self.assertEqual(list(compute_down_streak(close)), [0, 1, 2, 3, 0, 1, 2])

    def test_flat_close_breaks_the_streak(self):
        close = pd.Series([10, 9, 9, 8])
        self.assertEqual(list(compute_down_streak(close)), [0, 1, 0, 1])
```

- [ ] **Step 2: Run them and watch them fail**

`uv run python -m unittest tests.test_drop_15d -v`
Expected: `ImportError`.

- [ ] **Step 3: Implement both helpers**

Module-level in `create_technical_indicators.py`, beside `compute_inside_day`:

```python
def compute_drop_15d(close, window=15, lookback=45):
    """Worst drawdown over any window of `window` sessions or fewer, inside
    the trailing `lookback` sessions.

    The per-bar form -- today's close against its own trailing high -- is not
    a substitute. Measured over 183 shorted names it correlates 0.37 with a
    true window search and reads AEHR at -22.5% where the real figure is
    -47.6%. A name that fell 50% three weeks ago and then went flat is still
    a broken chart, and only the rolling minimum can see that.
    """
    per_bar = close / close.rolling(window + 1, min_periods=2).max() - 1
    return per_bar.rolling(lookback, min_periods=1).min()


def compute_down_streak(close):
    """Consecutive down closes ending at each bar.

    A flat close breaks the streak: an unchanged close is not a down day.
    Display only -- never gate on it. A name that fell 40% in three gap-downs
    has a streak of 1 and belongs on the SI tab.
    """
    down = close.diff() < 0
    groups = (~down).cumsum()
    return down.groupby(groups).cumsum().astype(int)
```

- [ ] **Step 4: Wire the columns in**

Inside the per-ticker loop, after the `close_vs_252h` line:

```python
            # Selloff legs for the SI tab. See compute_drop_15d for why the
            # rolling minimum is not interchangeable with today's bar.
            daily['drop_15d'] = compute_drop_15d(daily['close'])
            daily['down_streak'] = compute_down_streak(daily['close'])
```

- [ ] **Step 5: Run the tests**

`uv run python -m unittest tests.test_drop_15d -v`
Expected: PASS (5 tests).

- [ ] **Step 6: Commit**

```bash
git add src/indicators/create_technical_indicators.py tests/test_drop_15d.py
git commit -m "Add drop_15d and down_streak selloff indicators"
```

---

### Task 3: Short-interest collection module

**Files:**
- Create: `src/data_collection/fetch_short_interest.py`
- Modify: `config/workflow_config.yaml` (add the `si_tab:` block)
- Modify: `config/settings.py` (add `SHORT_INTEREST_FILE`)
- Create: `tests/test_fetch_short_interest.py`

**Interfaces:**
- Produces: `SI_FILTERS: dict` — the five Finviz filter legs.
- Produces: `parse_short_float(value: str) -> float | None` — `"70.54%"` → `70.54`, `"-"` → `None`.
- Produces: `build_rows(df: pd.DataFrame) -> list[dict]` — rows of `{ticker, si, float_shares, market_cap, inst_trans, price}`; drops rows with no parseable `si`.
- Produces: `fetch_short_interest() -> dict` — `{date, filters, rows}`.
- Produces: `main() -> int` — writes `data/short_interest.json`, returns a process exit code.

- [ ] **Step 1: Write the failing tests**

```python
class ParseShortFloatTests(unittest.TestCase):
    def test_parses_percent(self):
        self.assertAlmostEqual(parse_short_float("70.54%"), 70.54)

    def test_returns_none_for_placeholder(self):
        for raw in ("-", "", None):
            self.assertIsNone(parse_short_float(raw))


class BuildRowsTests(unittest.TestCase):
    def test_drops_rows_with_no_short_interest(self):
        df = pd.DataFrame({
            "Ticker": ["AAA", "BBB"],
            "Short Float": ["15.0%", "-"],
            "Float": [1e6, 2e6], "Market Cap": [1e9, 2e9],
            "Inst Trans": [0.01, -0.02], "Price": [10.0, 20.0],
        })
        rows = build_rows(df)
        self.assertEqual([r["ticker"] for r in rows], ["AAA"])
        self.assertAlmostEqual(rows[0]["si"], 15.0)

    def test_sorts_by_short_interest_descending(self):
        df = pd.DataFrame({
            "Ticker": ["LOW", "HIGH"], "Short Float": ["12.0%", "40.0%"],
            "Float": [1e6, 1e6], "Market Cap": [1e9, 1e9],
            "Inst Trans": [0.0, 0.0], "Price": [10.0, 10.0],
        })
        self.assertEqual([r["ticker"] for r in build_rows(df)], ["HIGH", "LOW"])


class FiltersTests(unittest.TestCase):
    def test_filters_match_the_source_url(self):
        self.assertEqual(SI_FILTERS, {
            "Average Volume": "Over 1M",
            "Current Volume": "Over 750K",
            "Price": "Over $10",
            "Float Short": "Over 10%",
            "Volatility": "Month - Over 4%",
        })
```

- [ ] **Step 2: Run and watch fail**

`uv run python -m unittest tests.test_fetch_short_interest -v`

- [ ] **Step 3: Add the config block**

Append to `config/workflow_config.yaml` after the `vars_tab:` block:

```yaml
# SI dashboard tab — highly shorted stocks that have already sold off,
# clustered under taxonomy L1 sections. Source: one Finviz Ownership
# screener call (see src/data_collection/fetch_short_interest.py).
si_tab:
  min_short_interest: 12.0      # percent of float; the Finviz leg gates at 10
  max_drawdown_60d: -0.25       # close vs the 60-day high
  max_drop_15d: -0.25           # worst <=15-session drop inside the last 45
  min_tickers_per_l1: 3         # L1 sections with fewer members are dropped
  top_k_si: 3                   # member SI values averaged into an L1 score
  hot_radar_rank: 10            # radar L1 rank at or better than this earns HOT
```

Add to `config/settings.py` beside the other `DATA_DIR` paths:

```python
SHORT_INTEREST_FILE = DATA_DIR / "short_interest.json"
```

- [ ] **Step 4: Write the module**

Key points the implementation must honour:
- Build the view with `make_ticker_repair_view(Ownership)`. Log `tickers_repaired`; 0 repairs on a non-empty table means Finviz changed the markup again.
- Order by `'Short Interest Share'`, `ascend=False`. `'Float Short'` is **not** a valid order key — the library raises `ValueError` listing the valid names.
- A 200 response that parses to zero rows logs a **warning distinct from a fetch failure**, mirroring the AAII guard. A quiet market and a broken parser must not look the same.
- `main()` writes `{date, filters, rows}` with `date` from the newest master parquet stem when one exists, else today's ISO date.

- [ ] **Step 5: Run the tests**

`uv run python -m unittest tests.test_fetch_short_interest -v`
Expected: PASS.

- [ ] **Step 6: Run it live once**

`uv run python -m src.data_collection.fetch_short_interest`
Expected: ~230 rows, a repair count equal to the row count, `data/short_interest.json` written.

- [ ] **Step 7: Commit**

```bash
git add src/data_collection/fetch_short_interest.py config/ tests/test_fetch_short_interest.py
git commit -m "Fetch Finviz short interest for the SI tab"
```

---

### Task 4: SI snapshot builder and export

**Files:**
- Modify: `src/reporting/export_dashboard_data.py` (add beside `export_vars`, ~line 1530)
- Create: `tests/test_si_grouping.py`

**Interfaces:**
- Consumes: `data/short_interest.json` from Task 3; `drop_15d` / `down_streak` from Task 2.
- Produces: `si_gate(row, cfg) -> bool`
- Produces: `_build_si_snapshot(si_rows, master_df, day_flags, radar_ranks, cfg) -> dict | None` — `{report_date, n_tickers, themes: [...]}` where each theme is an L1 section `{name, score, n, n_leaves, hot, radar_rank, leaves: [{name, score, tickers: [...]}]}`.
- Produces: `export_si(day_flags) -> dict | None`

- [ ] **Step 1: Write the failing tests**

```python
class SiGateTests(unittest.TestCase):
    def test_passes_on_drawdown_leg_alone(self):
        self.assertTrue(si_gate({"si": 20.0, "drawdown_60": -0.30, "drop_15d": -0.05}, CFG))

    def test_passes_on_drop_leg_alone(self):
        self.assertTrue(si_gate({"si": 20.0, "drawdown_60": -0.05, "drop_15d": -0.30}, CFG))

    def test_rejects_when_short_interest_is_too_low(self):
        self.assertFalse(si_gate({"si": 11.9, "drawdown_60": -0.90, "drop_15d": -0.90}, CFG))

    def test_rejects_when_neither_selloff_leg_fires(self):
        self.assertFalse(si_gate({"si": 40.0, "drawdown_60": -0.10, "drop_15d": -0.10}, CFG))

    def test_nan_selloff_legs_reject(self):
        """Unlike the V/A cutoffs, this gate fails CLOSED: a missing price
        history cannot prove a selloff, and this tab is about selloffs."""
        self.assertFalse(si_gate({"si": 40.0, "drawdown_60": float("nan"),
                                  "drop_15d": float("nan")}, CFG))


class SiGroupingTests(unittest.TestCase):
    def test_l1_scores_as_mean_of_top_three(self):
        # Members 40, 30, 20, 10 -> (40+30+20)/3 = 30.0
        ...

    def test_breadth_breaks_a_score_tie(self):
        ...

    def test_drops_l1_below_min_members(self):
        ...

    def test_tickers_sort_by_short_interest_descending(self):
        ...

    def test_hot_badge_comes_from_radar_rank(self):
        ...

    def test_missing_radar_drops_badges_without_reordering(self):
        ...
```

- [ ] **Step 2: Run and watch fail**

`uv run python -m unittest tests.test_si_grouping -v`

- [ ] **Step 3: Implement `si_gate`, `_build_si_snapshot`, `export_si`**

Rules the implementation must honour:
- `drawdown_60 = close / max60 - 1`, computed from the master row.
- The gate fails **closed** on NaN selloff legs. This is the opposite of `filter_metrics`, which fails open — that one hides rows from a view, this one asserts a selloff happened.
- A ticker tagged into several leaves counts once per L1, not once per leaf, when computing L1 member count.
- Drop `Uncategorized` and `Singleton` leaves before grouping.
- `radar_ranks` comes from reading `docs/data/radar.json`; a missing or malformed file yields `{}` and every `hot` is `False`.
- History uses `_update_history_file(OUTPUT_DIR / "si_history.json", report_date, snapshot)` — forward-accumulating, since Finviz publishes no per-session short-interest series.

- [ ] **Step 4: Run the tests**

`uv run python -m unittest tests.test_si_grouping -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/reporting/export_dashboard_data.py tests/test_si_grouping.py
git commit -m "Build and export the SI tab snapshot"
```

---

### Task 5: Workflow wiring

**Files:**
- Modify: `run_daily_workflow.py` (new step 7b after the fundamentals block, ~line 228)
- Modify: `src/reporting/export_dashboard_data.py` (`export_all`, ~line 2175)

- [ ] **Step 1: Add step 7b to the workflow**

Directly after the fundamentals `try/except`, mirroring its non-critical shape:

```python
        # Step 7b: Fetch short interest for the SI tab.
        # Non-critical: Finviz is a third party and the rest of the pipeline
        # does not depend on this file.
        logger.info(f"{'='*80}")
        logger.info(f"STEP: Fetch short interest (SI tab)")
        logger.info(f"{'='*80}")
        try:
            si_payload = fetch_short_interest()
            write_short_interest(si_payload, date_str)
            logger.info(f"OK Short interest: {len(si_payload['rows'])} tickers\n")
        except Exception as e:
            logger.warning(f"Short interest fetch failed: {e}")
            logger.warning("Continuing workflow without SI data...")
```

Note: `date_str` is defined in step 6, above this block. Import both names at the top of `run_daily_workflow.py`.

- [ ] **Step 2: Add the export call**

In `export_all`, after the VARS block (`1d`):

```python
    # 1d2. Export SI tab (Finviz short interest + local selloff legs)
    print("\n1d2. Exporting SI data")
    export_si(day_flags)
```

- [ ] **Step 3: Verify the workflow file still parses**

`uv run python -c "import ast,pathlib; ast.parse(pathlib.Path('run_daily_workflow.py').read_text())"`
Expected: no output.

- [ ] **Step 4: Commit**

```bash
git add run_daily_workflow.py src/reporting/export_dashboard_data.py
git commit -m "Wire the SI fetch and export into the daily workflow"
```

---

### Task 6: Dashboard tab

**Files:**
- Modify: `docs/index.html` (tab button after Parabolic, ~line 54; tab content after the Parabolic block)
- Modify: `docs/app.js` (URL constants, `siHistory`, `loadSIData`, `renderSI`, tab-switch branch, time-travel bar, init call)
- Modify: `docs/style.css` (only if a new class is needed)
- Modify: `tests/test_dashboard_panel_layout.py`
- Modify: `tests/test_dashboard_filter_markup.py` (docstring note only)

**Interfaces:**
- Consumes: `docs/data/si.json`, `docs/data/si_history.json` from Task 4.
- Produces: DOM ids `content-si`, `si-left`, `si-right`, `si-chart-area`, `si-container`, `si-tt-dates`.

- [ ] **Step 1: Add the width assertion first**

In `tests/test_dashboard_panel_layout.py`, add `"#si-left": 455,` to `PER_TAB_WIDTHS`.

455 = the 8-column table's natural width plus 28px panel padding, ~6px scrollbar and 2px border. It clears the 385px time-travel floor, and the bar has no cutoff pair to fit.

- [ ] **Step 2: Run it and watch it fail**

`uv run python -m unittest tests.test_dashboard_panel_layout -v`
Expected: FAIL — `#si-left` declares no width.

- [ ] **Step 3: Add the markup**

Tab button, after the Parabolic button:

```html
      <button class="tab-btn" data-tab="si" id="tab-si">SI</button>
```

Tab content, after the Parabolic content block. The time-travel bar carries **no** `.tt-filters` div:

```html
  <!-- ══ TAB: SI ═══════════════════════════════════════ -->
  <div class="tab-content" id="content-si">
    <div class="left-panel" id="si-left">
      <div class="time-travel-bar">
        <div class="time-travel-dates" id="si-tt-dates"></div>
      </div>
      <div id="si-container">
        <div class="loading">Loading SI data...</div>
      </div>
    </div>
    <div class="resize-handle" data-panel="si"></div>
    <div class="right-panel" id="si-right">
      <div class="chart-area" id="si-chart-area">
        <div class="chart-placeholder">
          <div class="placeholder-icon">📉</div>
          <div>Click any ticker to open chart</div>
          <div class="placeholder-sub">MA: 20 EMA · 50 SMA</div>
        </div>
      </div>
    </div>
  </div>
```

- [ ] **Step 4: Add the CSS width**

Beside the other per-tab overrides in `docs/style.css`:

```css
#si-left { width: 455px; }
```

- [ ] **Step 5: Wire `docs/app.js`**

Five edits:
1. Constants beside `VARS_HISTORY_URL`: `SI_DATA_URL = 'data/si.json'`, `SI_HISTORY_URL = 'data/si_history.json'`.
2. `let siHistory = [];` beside `varsHistory`.
3. `loadSIData()` — copy `loadVARSData`'s shape, minus the network render.
4. `renderSI(data, date)` — copy `renderVARS`'s L1-section shape. Columns: Ticker, SI%, DD60%, Drop15%, Streak, Price, Float(M), Inst%. **No `filterAttrs(t)` on the `<tr>`** and no `applyTickerFilters()` call — this tab has no cutoffs.
5. Tab-switch branch: `else if (tabContent.id === 'content-si') tabId = 'si';`
6. `renderTimeTravelBar('si-tt-dates', tabSessionDates(siHistory), onTimeTravelSelect);` in `renderAllTimeTravelBars`, and an SI block in `onTimeTravelSelect`.
7. `loadSIData();` beside `loadParabolicData();` at ~line 138.

- [ ] **Step 6: Run the layout tests**

`uv run python -m unittest tests.test_dashboard_panel_layout tests.test_dashboard_filter_markup -v`
Expected: PASS. `EXPECTED_BAR_COUNT` stays 5, proving SI added no cutoffs.

- [ ] **Step 7: Verify in the browser**

Generate a snapshot, serve `docs/`, open the SI tab, confirm sections render and a ticker click opens the chart. Then `git checkout -- docs/data/`.

- [ ] **Step 8: Commit**

```bash
git add docs/index.html docs/app.js docs/style.css tests/
git commit -m "Add the SI dashboard tab"
```

---

### Task 7: Documentation

**Files:**
- Modify: `CLAUDE.md` (new section after the VARS tab description; new rows in the data-store table)

- [ ] **Step 1: Document the tab**

Record the four findings a future reader cannot re-derive:
1. The Ownership view carries `Short Float` directly — one call, not 231.
2. The naive per-bar 15-session drop correlates 0.37 and fails AEHR's own gate; the rolling minimum is required.
3. History grows forward only, because Finviz publishes no per-session series. Backfilling would fabricate readings.
4. The tab deliberately carries no V/A cutoffs, because Finviz gates liquidity and volatility upstream.

- [ ] **Step 2: Add the data-store rows**

`data/short_interest.json` and `docs/data/si.json` / `si_history.json`.

- [ ] **Step 3: Run the whole suite**

`uv run python -m unittest discover -s tests`
Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add CLAUDE.md
git commit -m "Document the SI tab"
```

---

## Self-Review

**Spec coverage.** Data source → Task 3. Ticker repair → Task 1. Indicator columns → Task 2. Gate and scoring → Task 4. Hot badge → Task 4. Outputs and forward-only history → Tasks 3, 4. Dashboard and the no-cutoffs rule → Task 6. Config → Task 3. Tests → spread across 1, 2, 3, 4, 6. Documentation → Task 7. No gaps.

**Placeholders.** Task 4 Step 1 leaves six grouping test bodies as `...`. They are named precisely and their rules are stated in Step 3; the implementer writes them against the `_build_si_snapshot` signature in the Interfaces block.

**Type consistency.** `make_ticker_repair_view` (Tasks 1, 3), `compute_drop_15d` / `compute_down_streak` (Tasks 2, 4), `si_gate` / `_build_si_snapshot` / `export_si` (Tasks 4, 5), `fetch_short_interest` / `write_short_interest` (Tasks 3, 5) all match across tasks.
