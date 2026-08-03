---
title: Chart Earnings Markers - Plan
type: feat
date: 2026-07-31
artifact_contract: ce-unified-plan/v1
artifact_readiness: implementation-ready
product_contract_source: ce-plan-bootstrap
execution: code
---

# Chart Earnings Markers - Plan

> **⛔ SUPERSEDED — shipped as #72, then reverted.** The free `tv.js` embed applies only the **first 5** entries of `studies` and silently discards the rest, so adding `Earnings@tv-basicstudies` as a 6th entry pushed `STD;Volume` off the cap and killed the volume pane and its average-volume overlay. KTD3's "retain `STD;Volume`" was satisfied in the source but not at runtime, and the AE3 browser check did not catch it. `mainSeriesProperties.esdShowEarnings: true` alone is inert — the markers genuinely cost a study slot. Re-attempting this requires **giving up one moving average**, not appending. See the hard rule in `CLAUDE.md` → *Dashboard Chart (TradingView Free Embed Widget)*.

## Goal Capsule

- **Objective:** Show TradingView's native square `E` markers at earnings dates on every supported equity chart opened from the dashboard.
- **Authority:** The user request governs visible behavior. Repository TradingView constraints govern integration details. This plan governs implementation and verification.
- **Execution profile:** Lightweight frontend integration with one behavior-bearing unit and browser acceptance proof.
- **Stop condition:** Do not introduce a custom earnings feed, runtime Charting Library APIs, or unrelated corporate-event markers. Surface a blocker if the free widget no longer accepts its native earnings study through constructor configuration.
- **Tail ownership:** `ce-work` implements, verifies, reviews, and commits the change.

## Product Contract

### Summary

Enable TradingView's native earnings-date marker in the shared dashboard chart while preserving its current studies, legend values, navigation, and resizing behavior.

### Problem Frame

The dashboard's embedded daily chart does not show the earnings-date `E` marker that is available on TradingView charts. Traders must leave the dashboard or separately look up earnings timing, which removes useful event context from the price chart.

### Requirements

- R1. A supported equity chart shows TradingView's native square `E` marker on historical and scheduled earnings dates available from TradingView.
- R2. Interacting with an `E` marker exposes TradingView's native earnings details without dashboard-owned earnings data.
- R3. The behavior applies to every dashboard chart created through the shared `openChart()` path.
- R4. Symbols without TradingView earnings events continue to render normally without placeholder markers or errors.
- R5. Existing moving averages, volume study, OHLC/change/volume legend values, ticker switching, keyboard navigation, and chart resizing remain unchanged.
- R6. The change does not enable the economic-calendar panel, dividends, splits, or a custom earnings-data pipeline.

### Acceptance Examples

- AE1. **Covers R1, R2, R3.** Given a liquid U.S. equity such as `NASDAQ:AAPL` on the daily interval with a six-to-twelve-month visible range, when the chart loads, then at least one known report date has an `E` marker and interacting with it shows TradingView's earnings details.
- AE2. **Covers R4.** Given an unsupported symbol such as `FRED:DGS10`, when the chart loads, then it renders without an earnings marker and without a widget error.
- AE3. **Covers R5.** Given a dashboard stock chart after earnings markers are enabled, when the user switches tickers, tabs, and panel sizes, then the configured moving averages, `STD;Volume`, legend values, and chart controls remain available.
- AE4. **Covers R6.** Given the final chart configuration and repository diff, then the economic calendar remains disabled, no dividend or split studies are configured, and no custom earnings-data request or artifact is introduced.

### Scope Boundaries

- Keep `calendar: false`; that setting controls a separate calendar panel.
- Do not add dividend, split, news, or economic-event markers.
- Do not migrate from the current `tv.js` embed to TradingView's newer generated embed format.
- Do not add an earnings API, exporter, JSON artifact, or dashboard-owned tooltip.

## Planning Contract

### Key Technical Decisions

- KTD1. **Use the native earnings study in constructor configuration.** Add `Earnings@tv-basicstudies` to the existing all-object `studies` array in `openChart()`. TradingView's current public widget bundle exposes this study and the repository already relies on constructor-provided native studies.
- KTD2. **Treat explicit earnings visibility as an execution-time compatibility fallback.** First verify the native earnings study in the live free widget. Add `mainSeriesProperties.esdShowEarnings: true` to the existing `overrides` object only if the study does not display its marker by default. This keeps the primary change on the documented constructor channel while accommodating the free widget's undocumented internal visibility state.
- KTD3. **Preserve the widget's established invariants.** Keep every `studies` entry in object form, retain `STD;Volume`, leave `hide_legend: false`, and leave the per-element legend overrides unchanged.
- KTD4. **Use layered proof.** A focused source-contract test protects the constructor configuration, JavaScript syntax validation protects the static bundle, and browser QA proves TradingView's remote runtime behavior.

### Risks and Dependencies

- TradingView does not expose an earnings switch in the free widget configurator. The native study identifier and visibility property come from current TradingView-owned public assets and may change when TradingView updates the remote iframe.
- Marker availability depends on TradingView's symbol data and visible range. A missing marker on an index, ETF, forex, crypto, FRED symbol, or compressed chart is not evidence of a dashboard regression.
- Browser acceptance must use an equity with a known earnings event and enough visible history to avoid label suppression.

### Deferred to Implementation

- Determine from the live widget characterization whether `Earnings@tv-basicstudies` is sufficient or whether the compatibility visibility override from KTD2 is also required. Keep the smallest configuration that passes AE1 through AE3.

## Implementation Units

### U1. Enable and protect native earnings markers

- **Goal:** Configure every shared TradingView chart to show native earnings-date markers and guard the constructor contract against regression.
- **Requirements:** R1, R2, R3, R4, R5, R6; AE1, AE2, AE3.
- **Dependencies:** None.
- **Files:**
  - `docs/app.js`
  - `tests/test_dashboard_chart_config.py`
- **Approach:**
  1. Extend the existing object-form `studies` array with TradingView's native earnings study.
  2. Keep the current calendar, volume, study-style, legend, script-loading, resize, and active-chart behavior intact.
  3. Add the KTD2 visibility override only if browser characterization shows that the study alone is insufficient.
  4. Add a focused `unittest` source-contract check for the final earnings configuration and the existing all-object `STD;Volume` invariant.
- **Execution note:** Capture the missing-marker baseline on a known reporting equity before changing production code, then use the same symbol and visible range for the post-change proof.
- **Patterns to follow:** `docs/app.js` keeps all free-widget customization in the `new TradingView.widget()` constructor. `tests/test_vars_artifact.py` demonstrates repository-standard `unittest` structure and `pathlib` file access. Preserve the chart regressions documented by commits `fd5e7d9` and `1d85217`.
- **Test scenarios:**
  - The focused source-contract test finds the final native earnings constructor configuration inside `openChart()` and verifies that `STD;Volume` remains an object-form study.
  - Covers AE1. A known reporting equity on the daily interval shows at least one historical `E` marker, and the marker exposes TradingView's earnings details.
  - A scheduled future earnings date shows the provider's marker when TradingView has published the event; absence of future schedule data does not fail the historical-marker acceptance case.
  - Covers AE2. An unsupported non-equity symbol renders normally without an earnings marker or console error.
  - Covers AE3. Switching tickers and dashboard tabs reconstructs the widget with the earnings configuration while retaining the four moving averages, `STD;Volume`, OHLC/change/volume legend values, and resize behavior.
  - Covers AE4. The final constructor keeps the economic calendar disabled, includes no dividend or split studies, and the diff contains no custom earnings-data path or artifact.
- **Verification:** The focused test and JavaScript syntax check pass. The browser proof satisfies AE1 through AE3 at desktop and narrow viewport widths. The full unit suite remains green.

## Verification Contract

| Gate | Applies to | Command or proof | Done signal |
|---|---|---|---|
| Focused regression | U1 | `uv run python -m unittest tests.test_dashboard_chart_config` | Earnings configuration and the volume-study invariant pass. |
| JavaScript syntax | U1 | `node --check docs/app.js` | The static dashboard bundle parses without errors. |
| Browser acceptance | U1 | Serve `docs/`, open a known reporting equity and an unsupported symbol, and exercise ticker/tab/resize flows | AE1 through AE3 pass with no widget or console errors. |
| Repository regression | U1 | `uv run python -m unittest discover -s tests` | All unit tests pass. |

## Definition of Done

- Supported equity charts display TradingView's native earnings `E` markers and native event details.
- Unsupported symbols degrade by simply omitting the marker.
- Existing studies, legend values, navigation, chart loading, and resizing still work.
- The focused constructor-contract test, JavaScript syntax check, browser acceptance proof, and full unit suite pass.
- No generated `docs/data/*.json`, custom earnings data, unrelated corporate-event markers, or abandoned experimental code remains in the diff.

## Sources and Research

- Repository integration point: `docs/app.js` `openChart()` and its `new TradingView.widget()` constructor.
- TradingView Advanced Chart widget options: https://www.tradingview.com/widget-docs/widgets/charts/advanced-chart/
- TradingView earnings-marker behavior: https://www.tradingview.com/support/solutions/43000629790-earnings/
- TradingView label-suppression limitation: https://www.tradingview.com/support/solutions/43000557641-earnings-splits-or-dividends-labels-are-not-displayed/
- TradingView current public widget bundle containing the native earnings study: https://www.tradingview-widget.com/static/bundles/embed/93866.ff3e13d525fab70d79c2.js
- TradingView `tv.js` dynamic-construction guidance: https://www.tradingview.com/widget-docs/faq/general/
