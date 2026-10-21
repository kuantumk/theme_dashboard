// ═══════════════════════════════════════════════════════
// MARKET MONITOR — App Logic V2.1
// ═══════════════════════════════════════════════════════

(function () {
  'use strict';

  // ── CONFIG ────────────────────────────────────────────
  // The Themes tab renders the L1 Radar (screener-independent
  // theme-basket scoring with L1 roll-up + confirmation boost). The legacy
  // screened themes.json/themes_history.json exports are retired.
  const THEME_DATA_URL = 'data/radar.json';
  const THEME_HISTORY_URL = 'data/radar_history.json';
  const MOMENTUM_DATA_URL = 'data/momentum_136.json';
  const MOMENTUM_HISTORY_URL = 'data/momentum_136_history.json';
  const VOLUME_DATA_URL = 'data/volume.json';
  const VOLUME_HISTORY_URL = 'data/volume_history.json';
  const VARS_DATA_URL = 'data/vars.json';
  const VARS_HISTORY_URL = 'data/vars_history.json';
  const SI_DATA_URL = 'data/si.json';
  const SI_HISTORY_URL = 'data/si_history.json';
  const PARABOLIC_DATA_URL = 'data/parabolic.json';
  const PARABOLIC_HISTORY_URL = 'data/parabolic_history.json';
  const INDUSTRY_ETF_HISTORY_URL = 'data/industry_etf_history.json';
  const ETF_DATA_HISTORY_URL = 'data/etf_data_history.json';
  const ETF_VIZ_DATA_URL = 'data/etf_viz.json';
  const ETF_VIZ_HISTORY_URL = 'data/etf_viz_history.json';
  const BREADTH_DATA_URL = 'data/market_breadth.json';
  const NASI_DATA_URL = 'data/nasi.json';
  // Oversold line for OUR series. Not portable from a vendor's chart: RSI at the
  // floor is a ratio between a decayed memory of the last rally and the current
  // decline, so it shifts with issue coverage. Calibrated 2026-08 against
  // StockCharts $NASI, where ours reads ~1.0–1.1 higher at a trough (ours 9.97
  // vs their 8.85 on 2026-07-30), so 11 here is their 10.
  //
  // 11 rather than 10 for a second, independent reason: compute_nasi refreshes
  // a trailing 90 calendar days of A/D data on every run, so past sessions get
  // revised. 2026-07-30 read 9.84, 9.86, 9.87, 9.88, 9.94, 9.96, 10.02 and
  // 10.15 across 14 consecutive daily commits — its marker appeared and
  // vanished between deploys with nothing on screen to explain it. At 11 it
  // does not move.
  //
  // 11 also inherits the job of the retired dashed 12 rail, which marked the
  // major lows a lone 10-line hid: 2025-03-14 (11.01), 2025-04-11 (11.31),
  // 2025-11-21 (5.43), 2026-03-30 (10.95, the SPY year low) and 2026-07-31
  // (9.38). In the plotted 252 sessions 11 and 12 mark the same three episodes,
  // and at 0.4 CSS px apart the two rails drew as one line. The classic RSI 30
  // rail is useless here: this series crosses it constantly.
  const NASI_OVERSOLD = 11;
  const NASI_WATCH = 20;
  // A convention, not a calibrated level. Unlike NASI_OVERSOLD above — checked
  // against StockCharts $NASI in 2026-08 — 80 has never been compared against
  // an external reference. It arrived as a header tint and is now a plotted
  // rail, which lends it authority it has not earned. Re-verify it the same way
  // if the issue universe changes.
  const NASI_OVERBOUGHT = 80;
  // Sessions plotted, ~1 trading year. The window lives here and not in
  // compute_nasi.py's EXPORT_SESSIONS (378) on purpose: code PRs reset
  // docs/data/, so trimming the exporter would leave the deployed chart at 18
  // months until the next daily workflow run. The export is retention headroom.
  const NASI_CHART_SESSIONS = 252;
  const MACRO_DATA_URL = 'data/macro_data.json';
  const INDUSTRY_ETF_URL = 'data/industry_etf.json';
  const META_URL = 'data/report_meta.json';
  const EVENTS_URL = 'data/events.json';
  const ETF_SHEET_URL = 'https://docs.google.com/spreadsheets/d/1zwmK5YnbBHyin0n0DHIEEydPapCkln1WCvlKv4IhwSg/export?format=csv&gid=1565194920';
  const ETF_FALLBACK_URL = 'data/etf_data.json';
  const INDUSTRY_SHEET_URL = 'https://docs.google.com/spreadsheets/d/1zwmK5YnbBHyin0n0DHIEEydPapCkln1WCvlKv4IhwSg/export?format=csv&gid=549753148';
  const PAGE_LOAD_CACHE_KEY = Date.now();

  // Symbols that need a different symbol for TradingView widget vs data fetch.
  // NOTE: TVC/CAPITALCOM/CBOE treasury yield symbols are all restricted in the
  // embedded widget. FRED is the only embeddable source (line chart, no candlestick).
  const TV_CHART_SYM_MAP = {
    'CAPITALCOM:US2YR':  'FRED:DGS2',
    'CAPITALCOM:US10YR': 'FRED:DGS10',
    'CAPITALCOM:US30YR': 'FRED:DGS30',
  };

  // Active chart per tab
  let activeCharts = { macro: null, themes: null, momentum: null, momentumviz: null, volume: null, volumeviz: null, vars: null, varsviz: null, industry: null, etf: null, etfviz: null, parabolic: null };

  // Sort state per table
  let sortState = {
    etf: { column: 'vars', dir: 'desc' },
    industry: { column: 'vars', dir: 'desc' },
    parabolic: { column: 'atr_multi_50sma', dir: 'desc' },
  };
  let etfData = [];
  let industryData = [];
  let parabolicData = [];
  let volumeHistory = [];
  let parabolicEmptyMessage = 'No parabolic results for this date.';
  let industryEmptyMessage = 'No industry ETF data available.';
  let etfEmptyMessage = 'No ETF data available.';

  function withCacheBust(url) {
    const separator = url.includes('?') ? '&' : '?';
    return `${url}${separator}_=${PAGE_LOAD_CACHE_KEY}`;
  }

  // ── INIT ──────────────────────────────────────────────
  document.addEventListener('DOMContentLoaded', () => {
    initClock();
    initMarketStatus();
    initTabs();
    initTickerClicks();
    initZoneMaps();
    initTickerFilters();
    initResizablePanels();
    initRadarClamps();
    initArrowKeyNav();
    loadMeta();
    loadMacroData();
    loadBreadthData();
    initNasiResize();
    initNasiCrosshair();
    loadNasiData();
    loadThemeData();
    loadMomentumData();
    loadVolumeData();
    loadVARSData();
    loadIndustryETFData();
    loadETFData();
    loadETFVizData();
    loadParabolicData();
    loadSIData();
    loadMacroEvents();
    initTableSort();
  });

  // ── CLOCK ─────────────────────────────────────────────
  function initClock() {
    function update() {
      const now = new Date();
      const opts = { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' };
      document.getElementById('hdr-date').textContent = now.toLocaleDateString('en-US', opts).toUpperCase();
      document.getElementById('hdr-time').textContent = now.toLocaleTimeString('en-US', { hour12: false });
    }
    update();
    setInterval(update, 1000);
  }

  // ── MARKET STATUS ─────────────────────────────────────
  function initMarketStatus() {
    function update() {
      const now = new Date();
      const et = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));
      const h = et.getHours();
      const m = et.getMinutes();
      const day = et.getDay();
      const mins = h * 60 + m;
      const el = document.getElementById('market-status');
      const txt = document.getElementById('market-status-text');

      const weekend = day === 0 || day === 6;
      const state = (!weekend && MARKET_SESSIONS.find(w => mins >= w.from && mins < w.to))
        || MARKET_CLOSED;
      el.className = 'market-status ' + state.cls;
      txt.textContent = state.text;
    }
    update();
    setInterval(update, 30000);
  }

  // ── TAB SWITCHING ─────────────────────────────────────
  function initTabs() {
    document.querySelectorAll('.tab-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
        document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
        btn.classList.add('active');
        document.getElementById('content-' + btn.dataset.tab).classList.add('active');
        // The newly-shown tab may have rendered under a different filter state.
        applyTickerFilters();
        // Radar chips can only be measured once the tab has a box.
        if (btn.dataset.tab === 'themes') syncRadarClampsNow();
        // A zone map rendered on a hidden tab draws once the tab has a width.
        redrawZoneMapsNow();
      });
    });
  }

  // ── RESIZABLE PANELS ──────────────────────────────────
  function initResizablePanels() {
    document.querySelectorAll('.resize-handle').forEach(handle => {
      let startX, startWidth, leftPanel;

      handle.addEventListener('mousedown', (e) => {
        e.preventDefault();
        const tabContent = handle.closest('.tab-content') || handle.parentElement;
        leftPanel = tabContent.querySelector('.left-panel');
        startX = e.clientX;
        startWidth = leftPanel.offsetWidth;
        handle.classList.add('dragging');
        document.body.style.cursor = 'col-resize';
        document.body.style.userSelect = 'none';

        function onMove(e) {
          const dx = e.clientX - startX;
          // Minus, not plus: the list pane sits to the RIGHT of the handle
          // (see PANEL ORDER in style.css), so dragging right must shrink it
          // and hand the space to the chart. With a plus the divider runs
          // away from the pointer — wrong in a way that looks like a broken
          // handle rather than a sign error, and invisible in a screenshot.
          const newWidth = Math.max(250, Math.min(startWidth - dx, window.innerWidth - 300));
          leftPanel.style.width = newWidth + 'px';
          syncRadarClampsNow();
          redrawNasiNow();
          redrawZoneMapsSoon();
        }
        function onUp() {
          redrawZoneMapsNow();
          handle.classList.remove('dragging');
          document.body.style.cursor = '';
          document.body.style.userSelect = '';
          document.removeEventListener('mousemove', onMove);
          document.removeEventListener('mouseup', onUp);
        }
        document.addEventListener('mousemove', onMove);
        document.addEventListener('mouseup', onUp);
      });
    });
  }

  // ── TICKER CLICK → TRADINGVIEW CHART ──────────────────
  function initTickerClicks() {
    document.addEventListener('click', (e) => {
      const link = e.target.closest('.tn-link');
      if (!link) return;

      const sym = link.dataset.sym;
      const chartSym = link.dataset.chartSym || sym;
      if (!sym) return;

      const tabContent = link.closest('.tab-content');
      if (!tabContent) return;

      // A tab opens the chart only when it has a chart pane to open it in.
      const tabId = tabContent.id.replace('content-', '');
      if (!document.getElementById(tabId + '-chart-area')) return;

      tabContent.querySelectorAll('.tn-link').forEach(l => l.classList.remove('active-ticker'));
      link.classList.add('active-ticker');

      openChart(tabId, chartSym);
    });
  }

  function openChart(tabId, sym) {
    const areaEl = document.getElementById(tabId + '-chart-area');

    if (!areaEl) return;

    const containerId = 'tv_container_' + tabId;
    // Pin the widget container with absolute positioning so it always fills
    // chart-area regardless of flex layout timing. chart-area is given
    // position:relative below.
    areaEl.style.position = 'relative';
    areaEl.innerHTML = `<div id="${containerId}" style="position:absolute;inset:0"></div>`;

    function renderWidget() {
      new TradingView.widget({
        "autosize": true,
        "symbol": sym,
        "interval": "D",
        "timezone": "Etc/UTC",
        "theme": "dark",
        "style": "1",
        "locale": "en",
        "enable_publishing": false,
        "backgroundColor": "#0c0f15",
        "gridColor": "#1f2937",
        "hide_top_toolbar": false,
        "hide_legend": false,
        "save_image": false,
        "disabled_features": [
          "use_localstorage_for_settings"
        ],
        "container_id": containerId,
        "hotlist": false,
        "details": false,
        "calendar": false,
        "hide_volume": true,
        // HARD RULE: STD;Volume stays first and is never removed. The free
        // tv.js embed applies only the FIRST 5 studies and silently discards
        // the rest -- no error, no warning. Index 0 is the one slot the cap
        // can never reach, so the volume pane (bars + average-volume overlay)
        // is safe there. To add a study, REPLACE one; never append a 6th.
        "studies": [
          { "id": "STD;Volume" },
          { "id": "MAExp@tv-basicstudies", "inputs": { "length": 10 } },
          { "id": "MAExp@tv-basicstudies", "inputs": { "length": 20 } },
          { "id": "MASimple@tv-basicstudies", "inputs": { "length": 50 } },
          { "id": "MASimple@tv-basicstudies", "inputs": { "length": 200 } }
        ],
        "studies_overrides": {
          "moving average exponential.ma.color": CHART_MA_COLORS.ema,
          "moving average exponential.ma.linewidth": 1,
          "moving average exponential.ma.transparency": 20,
          "moving average.ma.color": CHART_MA_COLORS.sma,
          "moving average.ma.linewidth": 1,
          "moving average.ma.transparency": 20
        },
        "overrides": {
          "scalesProperties.scaleSeriesOnly": true,
          "paneProperties.legendProperties.showStudyTitles": false,
          "paneProperties.legendProperties.showStudyValues": false,
          "paneProperties.legendProperties.showStudyArguments": false
        }
      });
    }

    if (window.TradingView && window.TradingView.widget) {
      renderWidget();
    } else {
      if (!window.tvScriptLoading) {
        window.tvScriptLoading = true;
        const script = document.createElement('script');
        script.src = 'https://s3.tradingview.com/tv.js';
        script.onload = renderWidget;
        document.head.appendChild(script);
      } else {
        setTimeout(() => openChart(tabId, sym), 300);
      }
    }

    // Nudge any TradingView autosize observers a few times after creation so a
    // tab that was hidden / a flex layout still settling redraws to full size.
    [200, 600, 1500].forEach(ms => setTimeout(
      () => window.dispatchEvent(new Event('resize')), ms
    ));

    activeCharts[tabId] = sym;
  }

  // Expose globally for inline onclick handlers to work (e.g. from themes tab)
  window.openChart = openChart;

  // ── ARROW KEY NAVIGATION ────────────────────────────────
  let navIndices = { macro: -1, themes: -1, momentum: -1, momentumviz: -1, volume: -1, volumeviz: -1, vars: -1, varsviz: -1, industry: -1, etf: -1, etfviz: -1, parabolic: -1 };

  function getActiveTabId() {
    const activeBtn = document.querySelector('.tab-btn.active');
    return activeBtn ? activeBtn.dataset.tab : 'macro';
  }

  function getTickerLinksForTab(tabId) {
    const container = document.getElementById('content-' + tabId);
    if (!container) return [];
    const leftPanel = container.querySelector('.left-panel');
    if (!leftPanel) return [];
    // Arrow-key travel is out of scope on the zone map, so its list links
    // are skipped (each ticker also repeats across its states and clusters).
    return Array.from(leftPanel.querySelectorAll('.tn-link')).filter(l => !l.closest('.zone-map'));
  }

  /**
   * A ticker the V/A cutoffs have dimmed. `closest` matches self-or-ancestor,
   * which covers both shapes in one expression: table tabs put .filtered-out on
   * the <tr>, the Themes tab puts it on the chip, which is the link itself.
   */
  function isDimmed(link) {
    return link.closest('.filtered-out') !== null;
  }

  /**
   * Next undimmed ticker from `idx`, or -1 when there is none that way.
   *
   * Scans the full link array rather than a pre-filtered one so the stored index
   * keeps meaning the same thing regardless of the active cutoffs — the click
   * handler writes into the same index space with links.indexOf(link), and a
   * cutoff change must not silently repoint the selection.
   */
  function nextVisibleIndex(links, idx, step) {
    for (let i = idx + step; i >= 0 && i < links.length; i += step) {
      if (!isDimmed(links[i])) return i;
    }
    return -1;
  }

  function initArrowKeyNav() {
    document.addEventListener('keydown', (e) => {
      if (e.key !== 'ArrowDown' && e.key !== 'ArrowUp') return;

      // Don't intercept if user is typing in an input
      if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;

      e.preventDefault();

      const tabId = getActiveTabId();
      const links = getTickerLinksForTab(tabId);
      if (links.length === 0) return;

      // Travel between tickers that pass the cutoffs, stepping over any number
      // of consecutive dimmed ones. When nothing undimmed lies that way — end of
      // the list, or every remaining ticker dimmed — stay put rather than
      // falling back to the boundary, which would select a dimmed ticker.
      const step = e.key === 'ArrowDown' ? 1 : -1;
      let start = navIndices[tabId];
      // A re-render — time travel to a sparser session, a data reload — can
      // leave the stored index past the end of the new, shorter list. Re-enter
      // from the near edge instead of scanning from out of bounds, which
      // matches nothing and deadens BOTH arrows until the user clicks.
      if (start >= links.length) start = step > 0 ? -1 : links.length;
      if (start < 0 && step < 0) return;   // ArrowUp before any selection
      const idx = nextVisibleIndex(links, start, step);
      if (idx < 0) return;

      navIndices[tabId] = idx;
      const link = links[idx];
      const sym = link.dataset.sym;

      // Clear all active states in this tab
      const container = document.getElementById('content-' + tabId);
      container.querySelectorAll('.tn-link').forEach(l => l.classList.remove('active-ticker'));
      container.querySelectorAll('tr.nav-active').forEach(r => r.classList.remove('nav-active'));

      // Set active state
      link.classList.add('active-ticker');
      const row = link.closest('tr');
      if (row) {
        row.classList.add('nav-active');
        row.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
      }

      // Open chart
      openChart(tabId, link.dataset.chartSym || sym);
    });

    // Sync nav index when user clicks a ticker
    document.addEventListener('click', (e) => {
      const link = e.target.closest('.tn-link');
      if (!link) return;
      const tabContent = link.closest('.tab-content');
      if (!tabContent) return;
      const tabId = tabContent.id.replace('content-', '');
      const links = getTickerLinksForTab(tabId);
      const idx = links.indexOf(link);
      if (idx >= 0) navIndices[tabId] = idx;

      // Apply row highlight
      tabContent.querySelectorAll('tr.nav-active').forEach(r => r.classList.remove('nav-active'));
      const row = link.closest('tr');
      if (row) row.classList.add('nav-active');
    });
  }

  // ── MACRO EVENTS ───────────────────────────────────────
  function loadMacroEvents() {
    fetch(withCacheBust(EVENTS_URL))
      .then(r => {
        if (!r.ok) throw new Error('Not found');
        return r.json();
      })
      .then(events => {
        const container = document.getElementById('events-content');
        if (!events || events.length === 0) {
          container.innerHTML = '<div class="events-empty">No upcoming macro events</div>';
          return;
        }

        let html = '<div class="events-header">▸ Upcoming U.S. Macro Events</div>';
        let lastLocalDate = '';

        events.forEach(ev => {
          // Parse date (DD/MM/YYYY) and time (HH:MM) — stored as US Eastern
          const parts = ev.date.split('/');
          const day = parts[0], month = parts[1], year = parts[2];

          // Display original Eastern time directly (no timezone conversion)
          const dateObj = new Date(`${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}T12:00:00Z`);
          const dateStr = dateObj.toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric', timeZone: 'UTC' });
          const timeStr = ev.time || '';
          const displayDate = (dateStr !== lastLocalDate) ? dateStr : '';
          lastLocalDate = dateStr;
          const dtDisplay = displayDate ? (displayDate + (timeStr ? ' ' + timeStr : '')) : timeStr;

          html += `
            <div class="event-item">
              <span class="event-datetime">${escHtml(dtDisplay)}</span>
              <span class="event-name">${escHtml(ev.event)}</span>
            </div>
          `;
        });

        container.innerHTML = html;

        // Add click-toggle for the events button
        const eventsBtn = document.querySelector('.events-button');
        const eventsWrap = document.querySelector('.economic-events');
        if (eventsBtn && eventsWrap) {
          eventsBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            eventsWrap.classList.toggle('events-open');
          });
          // Close when clicking outside
          document.addEventListener('click', (e) => {
            if (!eventsWrap.contains(e.target)) {
              eventsWrap.classList.remove('events-open');
            }
          });
        }
      })
      .catch(() => {
        const container = document.getElementById('events-content');
        container.innerHTML = '<div class="events-empty">Events data not available</div>';
      });
  }

  // ── LOAD META (last refresh) ──────────────────────────
  function loadMeta() {
    fetch(withCacheBust(META_URL))
      .then(r => r.json())
      .then(data => {
        if (data.export_timestamp) {
          // export_timestamp is UTC (written by the CI runner). Older exports omit a
          // timezone suffix, which new Date() would misread as local time — coerce those
          // to UTC so the refresh renders in the viewer's local zone, matching the header clock.
          const raw = data.export_timestamp;
          const hasTz = /[zZ]|[+-]\d{2}:?\d{2}$/.test(raw);
          const dt = new Date(hasTz ? raw : raw + 'Z');
          document.getElementById('dataRefresh').textContent =
            'Last refresh: ' + dt.toLocaleDateString() + ' ' + dt.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
        }
      })
      .catch(() => { });
  }

  // ── MACRO DATA (Yahoo Finance) ────────────────────────
  function loadMacroData() {
    fetch(withCacheBust(MACRO_DATA_URL))
      .then(r => r.json())
      .then(data => {
        // Combine all items
        const allItems = [
          ...(data.indices || []), ...(data.crypto || []), ...(data.precious_metals || []),
          ...(data.base_metals || []), ...(data.energy || []), ...(data.yields || []), ...(data.dollar || [])
        ];

        allItems.forEach(item => {
          const symSpan = document.querySelector(`span[data-sym="${item.tv}"]`);
          if (symSpan) {
            const tr = symSpan.closest('tr');
            if (tr) {
              const loadingTd = tr.querySelector('.loading');
              if (loadingTd) {
                loadingTd.remove();

                const tdPrice = document.createElement('td');
                const priceStr = item.price != null
                  ? Number(item.price).toLocaleString(undefined, { maximumFractionDigits: 2 })
                  : '—';
                tdPrice.textContent = priceStr;
                tdPrice.className = 'val-price';
                if (item.d1 != null) {
                  tdPrice.classList.add(pickBand(COLOR_BANDS.macroChange, item.d1));
                }
                tr.appendChild(tdPrice);

                ['d1', 'w1', 'hi52w', 'ytd'].forEach(k => {
                  const val = item[k];
                  const td = document.createElement('td');
                  td.textContent = val != null ? (val > 0 ? '+' : '') + val.toFixed(1) + '%' : '—';
                  td.className = pickBand(COLOR_BANDS.macroChange, val);
                  td.classList.add('val-pct');
                  tr.appendChild(td);
                });

                tr.style.cursor = 'pointer';
                tr.onclick = () => openChart('macro', TV_CHART_SYM_MAP[item.tv] || item.tv);
              }
            }
          }
        });

        // By default open OANDA:SPX500USD
        openChart('macro', 'OANDA:SPX500USD');
      })
      .catch(err => console.error('Error loading macro data:', err));
  }

  // ── MARKET BREADTH DATA ───────────────────────────────
  const SURVEY_MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                       'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

  // Split the ISO string rather than `new Date(iso)`: a bare date parses as UTC
  // midnight, so every viewer west of Greenwich would see the previous day —
  // and this label is the tile's staleness signal, so an off-by-one there reads
  // as a survey that has not updated.
  //
  // Shared by the AAII and NAAIM tiles; both carry a weekly survey date and
  // both need the same UTC-parsing avoidance for the same reason.
  function formatSurveyWeek(iso) {
    const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(iso);
    if (!m) return iso;
    return SURVEY_MONTHS[+m[2] - 1] + ' ' + (+m[3]);
  }

  function loadBreadthData() {
    fetch(withCacheBust(BREADTH_DATA_URL))
      .then(r => r.json())
      .then(data => {
        // CNN Fear & Greed
        if (data.fear_greed && data.fear_greed.score != null) {
          const el = document.getElementById('fg-value');
          el.textContent = data.fear_greed.score.toFixed(1);
          el.className = 'breadth-value ' + pickBand(COLOR_BANDS.fearGreed, data.fear_greed.score);
          if (data.fear_greed.rating) {
            document.getElementById('fg-rating').textContent = data.fear_greed.rating.toUpperCase();
          }
        }

        // NAAIM Exposure.
        //
        // No tint (see the .aaii-parts comment in style.css for the full
        // reasoning). High exposure is contrarian-bearish, but no threshold for
        // NAAIM has been calibrated in this repo, and every other tinted tile
        // here tints off a level that has been. The label carries the meaning;
        // the number stays neutral.
        if (data.naaim) {
          const nv = document.getElementById('naaim-value');
          if (nv && data.naaim.value != null) {
            // Two decimals, unlike the other tiles' one: NAAIM steps weekly, so
            // the second decimal is a real digit distinguishing one survey from
            // the next rather than daily noise. Negative readings (net-short
            // member positioning) print their sign as-is.
            nv.textContent = data.naaim.value.toFixed(2) + '%';
          }
          const na = document.getElementById('naaim-as-of');
          // The date sits on the title row (" - Sep 23"). Name the gap rather
          // than leaving it blank. The reading stands
          // unchanged for six days by design, so the date is the only thing
          // separating a dead fetch from an ordinary mid-week view — its
          // absence has to read as absence.
          if (na) {
            na.textContent = data.naaim.as_of
              ? ' - ' + formatSurveyWeek(data.naaim.as_of)
              : ' - date unknown';
          }
        }

        // AAII Sentiment. Three figures, no tint — see the .aaii-parts comment
        // in style.css. With no aaii block at all (the window between a code
        // deploy and the next workflow run, since code PRs reset docs/data/)
        // this branch does not run and the tile keeps its markup em dashes,
        // which is the intended empty state.
        if (data.aaii) {
          // Keyed by the full element id rather than a suffix so the ids stay
          // greppable against index.html; a concatenated id hides a mismatch
          // from both a reader and the markup guard in tests/.
          const slots = {
            'aaii-bull': 'bullish',
            'aaii-neut': 'neutral',
            'aaii-bear': 'bearish',
          };
          Object.entries(slots).forEach(([elementId, key]) => {
            const el = document.getElementById(elementId);
            const val = data.aaii[key];
            if (el) el.textContent = val != null ? val.toFixed(1) + '%' : '—';
          });
          const wk = document.getElementById('aaii-week');
          // The date sits on the title row (" - Sep 23"). Name the gap rather
          // than leaving it blank: the date is the
          // only thing separating a frozen fetch from an ordinary mid-week
          // view, so its absence has to read as absence.
          if (wk) {
            wk.textContent = data.aaii.week_ending
              ? ' - ' + formatSurveyWeek(data.aaii.week_ending)
              : ' - date unknown';
          }
        }

        // Render each breadth indicator with history as numbers
        ['ncfd', 'mmtw', 'mmfi', 'mmth'].forEach(key => {
          const val = data[key]?.current ?? data[key] ?? null;
          const hist = data[key]?.history ?? (val != null ? [val] : []);
          const valEl = document.getElementById(key + '-value');
          const histEl = document.getElementById(key + '-history');

          if (valEl && val != null) {
            valEl.textContent = val.toFixed(1) + '%';
            valEl.className = 'breadth-value ' + pickBand(COLOR_BANDS[key], val);
          }
          if (histEl) {
            // Only render history if there's more than 1 item, to avoid redundancy
            if (hist.length > 1) {
              renderBreadthHistory(key, histEl, hist);
            } else {
              histEl.innerHTML = ''; // Hide history if redundant
            }
          }
        });
      })
      .catch(err => console.error('Error loading breadth:', err));
  }

  // ── NASI (Nasdaq McClellan Summation + RSI) ───────────
  function loadNasiData() {
    fetch(withCacheBust(NASI_DATA_URL))
      .then(r => (r.ok ? r.json() : null))
      .then(data => {
        if (!data || !data.current) return;
        const cur = data.current;

        const oscEl = document.getElementById('nasi-osc');
        if (oscEl && cur.oscillator != null) {
          oscEl.textContent = (cur.oscillator >= 0 ? '+' : '') + cur.oscillator.toFixed(2);
          oscEl.className = 'nasi-stat-val ' + pickBand(COLOR_BANDS.nasiOsc, cur.oscillator);
        }

        const rsiEl = document.getElementById('nasi-rsi');
        if (rsiEl && cur.rsi != null) {
          rsiEl.textContent = cur.rsi.toFixed(2);
          rsiEl.className = 'nasi-stat-val ' + nasiRsiState(cur.rsi);
        }

        // Slice once, here, so every consumer shares one index space:
        // renderNasiChart plots at it, nasiIndexAt and initNasiCrosshair divide
        // by it, showNasiReadout indexes into it. Slicing further downstream —
        // or leaving one of the two calls below on the full payload — would
        // plot 378 sessions against 252-session pointer math, and the crosshair
        // would silently name a neighbouring session.
        //
        // The oscillator is derived from the whole payload and only then
        // sliced by the same window, so nasiOsc stays parallel to nasiHistory
        // AND the oldest visible session keeps a real oscillator. Deriving
        // after the slice would cost that session its predecessor.
        const full = data.history || [];
        nasiHistory = full.slice(-NASI_CHART_SESSIONS);
        nasiOsc = deriveNasiOscillator(full).slice(-NASI_CHART_SESSIONS);
        renderNasiChart(nasiHistory);
        showNasiReadout(nasiHistory.length - 1);
      })
      .catch(err => console.error('Error loading NASI:', err));
  }

  // The history points carry date/summation/summation_ma/rsi — no oscillator;
  // only `current` has one. Since summation is a running total of the
  // oscillator, diff(summation) recovers it exactly (CLAUDE.md pins this
  // invariant, and tests/test_nasdaq_mcclellan.py tests it). Deriving here
  // rather than adding a field to the exporter keeps the readout working on
  // merge: code PRs reset docs/data/, so a new field would read "—" until the
  // next daily workflow run.
  //
  // Round to 2dp. The stored summations are themselves 2dp, so the raw
  // subtraction carries float noise — -6304.07 - -6328.92 evaluates to
  // 24.849999999999454 — and the header shows two decimals.
  function deriveNasiOscillator(history) {
    return history.map((pt, i) => {
      if (i === 0 || pt.summation == null || history[i - 1].summation == null) return null;
      return Math.round((pt.summation - history[i - 1].summation) * 100) / 100;
    });
  }

  function nasiRsiState(rsi) {
    return pickBand(COLOR_BANDS.nasiRsi, rsi);
  }

  // Geometry of the two stacked panes inside viewBox "0 0 600 192".
  // The RSI pane (80 units) is taller than the summation pane needs to be
  // relative to it: RSI carries the signal, the summation only its shape.
  const NASI_GEO = { w: 600, top: 6, botTop: 94, rsiTop: 108, rsiBot: 188 };

  // Last rendered history, so a resize can redraw at the new x-scale. The
  // oversold markers are sized from the live scale, so a stale render leaves
  // them stretched after the panel is dragged.
  let nasiHistory = null;
  // Oscillator per history index, derived once per load (see
  // deriveNasiOscillator). Parallel to nasiHistory; null where not derivable.
  let nasiOsc = null;
  // The crosshair <line>. renderNasiChart clears the SVG on every redraw, so
  // this ref is reassigned there rather than held across renders.
  let nasiCrosshair = null;

  function redrawNasiNow() {
    if (nasiHistory) renderNasiChart(nasiHistory);
  }

  function initNasiResize() {
    const svg = document.getElementById('nasi-chart');
    if (!svg || typeof ResizeObserver === 'undefined') return;
    // Backstop only. Same lesson as syncRadarClamps: the observer delivers only
    // inside the rendering lifecycle, so the panel-drag and window-resize paths
    // call redrawNasiNow directly rather than depending on it.
    let last = 0;
    new ResizeObserver(entries => {
      const w = Math.round(entries[0].contentRect.width);
      if (w === last) return;
      last = w;
      redrawNasiNow();
    }).observe(svg);
  }

  function renderNasiChart(history) {
    const svg = document.getElementById('nasi-chart');
    if (!svg) return;
    svg.innerHTML = '';
    // The reset above detached the previous crosshair; drop the reference on
    // the early-return path too, so it can never point at an orphaned node.
    nasiCrosshair = null;
    if (!history || history.length < 2) return;

    const g = NASI_GEO;
    const n = history.length;
    const xAt = i => (n === 1 ? 0 : (i / (n - 1)) * g.w);

    const sums = history.map(d => d.summation).filter(v => v != null);
    let lo = Math.min(...sums), hi = Math.max(...sums);
    if (hi === lo) { hi += 1; lo -= 1; }
    const pad = (hi - lo) * 0.08;
    lo -= pad; hi += pad;
    const yNasi = v => g.botTop - ((v - lo) / (hi - lo)) * (g.botTop - g.top);
    const yRsi = v => g.rsiBot - (Math.max(0, Math.min(100, v)) / 100) * (g.rsiBot - g.rsiTop);

    const ns = 'http://www.w3.org/2000/svg';
    const add = (tag, attrs) => {
      const el = document.createElementNS(ns, tag);
      for (const k in attrs) el.setAttribute(k, attrs[k]);
      svg.appendChild(el);
      return el;
    };
    // preserveAspectRatio="none" stretches the box, so every stroke must opt out
    // of scaling or the lines thicken with panel width.
    const line = (d, stroke, width, dash) => add('path', {
      d, fill: 'none', stroke, 'stroke-width': width,
      'vector-effect': 'non-scaling-stroke',
      'stroke-linejoin': 'round',
      ...(dash ? { 'stroke-dasharray': dash } : {})
    });
    const path = (key, yFn) => {
      let d = '', pen = false;
      history.forEach((pt, i) => {
        const v = pt[key];
        if (v == null) { pen = false; return; }
        d += `${pen ? 'L' : 'M'}${xAt(i).toFixed(2)},${yFn(v).toFixed(2)}`;
        pen = true;
      });
      return d;
    };

    // NASI pane: zero reference, then the 10-day MA under the index itself.
    if (lo < 0 && hi > 0) {
      line(`M0,${yNasi(0).toFixed(2)}L${g.w},${yNasi(0).toFixed(2)}`,
           'var(--border2)', 1, '3 3');
    }
    const maPath = path('summation_ma', yNasi);
    if (maPath) line(maPath, 'var(--accent)', 1, '4 3');
    line(path('summation', yNasi), 'var(--text)', 1.4);

    // RSI pane: oversold band, threshold rails, then the RSI track.
    // The band fill stays exclusive to the oversold side; the overbought side
    // is marked by its rail and markers alone.
    add('rect', {
      x: 0, y: yRsi(NASI_OVERSOLD), width: g.w,
      height: g.rsiBot - yRsi(NASI_OVERSOLD),
      fill: 'var(--amber)', opacity: 0.16
    });
    // Two rails, both amber. Rail hue is deliberately NOT the marker hue: the
    // 80 rail sits at y 124 and the markers there cover y 115.8-125.0 (centres
    // 117.8-123.0, plus ry: 2), so a red rail would sit under the red markers
    // it labels and the pair would read as one thickened line. The markers
    // carry the signal colour; the rails stay neutral on both sides. Position
    // separates the two thresholds — they are 55.2 viewBox units apart.
    //
    // Do not add a third rail near the oversold end. The pane gives 80 units to
    // 0-100 at a 1:1 vertical scale, so one RSI point is 0.8 CSS px: the
    // retired 12 rail drew as one thickened line against 11 (at the old 40-unit
    // pane) and marked the same
    // three episodes in the plotted window.
    [
      [NASI_OVERSOLD, 'var(--amber)', null],
      [NASI_OVERBOUGHT, 'var(--amber)', null],
    ].forEach(([lvl, stroke, dash]) => {
      line(`M0,${yRsi(lvl).toFixed(2)}L${g.w},${yRsi(lvl).toFixed(2)}`,
           stroke, 1, dash);
    });
    line(path('rsi', yRsi), 'var(--text2)', 1.2);

    // Mark every session that actually reached either band — these are the
    // signal dates, and they are easy to miss on an 80px-tall pane.
    //
    // Both bands use a *level* test, never a crossing test. The panel reports a
    // phase, not a signal date, so an overbought stretch has to read as a run:
    // in the plotted year, 18 consecutive sessions sat at or above 80 where a
    // crossing test would have drawn one marker. One loop emits both colours so
    // the two rules cannot drift apart.
    //
    // An <ellipse> with rx divided by the live x-scale, not a <circle>: under
    // preserveAspectRatio="none" the x and y scales differ at every panel width
    // except exactly 600px, and a circle's r is a viewBox length, so it would
    // render as an ellipse whose shape drifts as the panel is dragged.
    // vector-effect does not help — it protects stroke width, not geometry, and
    // is inert on a fill-only shape. Since the scale is read at render time,
    // renderNasiChart must re-run on resize (see initNasiResize).
    //
    // Sessions sit ~1.35 CSS px apart against a 4 CSS px marker at the default
    // panel width, so a contiguous run draws as one band with rounded ends
    // rather than separate dots. That is the intended read for a phase.
    const sx = (svg.getBoundingClientRect().width / g.w) || 1;
    history.forEach((pt, i) => {
      if (pt.rsi == null) return;
      let fill = null;
      if (pt.rsi <= NASI_OVERSOLD) fill = 'var(--green)';
      else if (pt.rsi >= NASI_OVERBOUGHT) fill = 'var(--red)';
      if (fill === null) return;
      add('ellipse', {
        cx: xAt(i).toFixed(2), cy: yRsi(pt.rsi).toFixed(2),
        rx: (2 / sx).toFixed(3), ry: 2, fill
      });
    });

    // Crosshair last, so it draws over the plotted series. It is created as
    // part of the render because the innerHTML reset above destroys it on
    // every redraw — and redraws fire on panel drag, window resize, and the
    // ResizeObserver backstop.
    nasiCrosshair = add('line', {
      x1: 0, y1: g.top, x2: 0, y2: g.rsiBot,
      stroke: 'var(--yellow)', 'stroke-width': 1,
      'vector-effect': 'non-scaling-stroke',
      opacity: 0.75, visibility: 'hidden', 'pointer-events': 'none'
    });
  }

  // Pointer x -> nearest session index. The SVG is preserveAspectRatio="none",
  // so viewBox units and CSS pixels diverge at every width except exactly 600 —
  // the mapping has to go through the measured rect. Read the rect per event
  // rather than caching it: the panel is drag-resizable mid-session.
  function nasiIndexAt(svg, clientX) {
    if (!svg || !nasiHistory || nasiHistory.length < 2) return -1;
    const rect = svg.getBoundingClientRect();
    if (!rect.width) return -1;
    const frac = (clientX - rect.left) / rect.width;
    const i = Math.round(frac * (nasiHistory.length - 1));
    return Math.max(0, Math.min(nasiHistory.length - 1, i));
  }

  function showNasiReadout(i) {
    const dateEl = document.getElementById('nasi-ro-date');
    const oscEl = document.getElementById('nasi-ro-osc');
    const rsiEl = document.getElementById('nasi-ro-rsi');
    if (!dateEl || !oscEl || !rsiEl) return;
    if (!nasiHistory || i < 0 || i >= nasiHistory.length) return;

    const pt = nasiHistory[i];
    const osc = nasiOsc ? nasiOsc[i] : null;
    dateEl.textContent = pt.date || '—';
    // Index 0 has no predecessor, so no oscillator is derivable there.
    oscEl.textContent = osc == null ? '—' : (osc >= 0 ? '+' : '') + osc.toFixed(2);
    rsiEl.textContent = pt.rsi == null ? '—' : pt.rsi.toFixed(2);
  }

  function initNasiCrosshair() {
    const svg = document.getElementById('nasi-chart');
    if (!svg) return;

    svg.addEventListener('pointermove', e => {
      // The panel renders before nasi.json resolves; a hover in that window
      // must be a no-op, not a throw.
      if (!nasiHistory || !nasiCrosshair) return;
      const i = nasiIndexAt(svg, e.clientX);
      if (i < 0) return;
      // nasiIndexAt already rejected n < 2, so the divisor is safe.
      const x = ((i / (nasiHistory.length - 1)) * NASI_GEO.w).toFixed(2);
      nasiCrosshair.setAttribute('x1', x);
      nasiCrosshair.setAttribute('x2', x);
      nasiCrosshair.setAttribute('visibility', 'visible');
      showNasiReadout(i);
    });

    svg.addEventListener('pointerleave', () => {
      if (nasiCrosshair) nasiCrosshair.setAttribute('visibility', 'hidden');
      // Fall back to the newest session, which is also what the removed range
      // line used to convey: how current the data is.
      if (nasiHistory) showNasiReadout(nasiHistory.length - 1);
    });
  }

  function renderBreadthHistory(key, el, history) {
    el.innerHTML = '';
    const last5 = history.slice(-5);
    last5.forEach((val, i) => {
      const span = document.createElement('span');
      // Same table as the tile itself, so a history number and the headline
      // figure above it can never tint by different rules.
      span.className = 'breadth-hist-num ' + pickBand(COLOR_BANDS[key], val);
      span.textContent = val.toFixed(1);
      span.title = 'Session ' + (i + 1) + ': ' + val.toFixed(1) + '%';
      el.appendChild(span);
    });
  }

  // ── THEME DATA + TIME TRAVEL ──────────────────────────
  let themesHistory = [];    // Array of radar snapshots (Themes tab), newest first
  let momentumHistory = [];  // Array of momentum snapshots, newest first
  let varsHistory = [];      // Array of vars snapshots, newest first
  let parabolicHistory = []; // Array of parabolic snapshots, newest first
  let siHistory = [];        // Array of SI snapshots, newest first. Grows
                             // forward only — Finviz publishes current short
                             // interest, not a per-session series, so there is
                             // nothing to backfill.
  let industryHistory = [];  // Array of {report_date, data} snapshots
  let etfHistory = [];       // Array of {report_date, data} snapshots
  let etfVizHistory = [];    // Array of {report_date, zone_map} snapshots
  let latestSessionDate = null; // report_meta.json's session: the newest any tab holds
  let activeSessionDate = null;
  let hasUserSelectedSession = false;

  // ── TICKER FILTER CUTOFFS (V / A) ─────────────────────
  // Shared across tabs like activeSessionDate: changing V on VARS leaves it
  // changed on Themes. Session-only — reload returns to the defaults below.
  //
  // These are cutoffs, not toggles: there is no off position. The dashboard
  // dims against $50M / 4% from first paint, and the only choice is how tight.
  // V reads 20-day average DOLLAR volume — the column every screener and the
  // radar universe floor already gate on — not share volume, which it read
  // while the control was a fixed 1M-share on/off button.
  const tickerFilters = { vol: 50e6, adr: 0.04 };

  // Which payload key and data-attribute each cutoff reads.
  const FILTER_SOURCES = { vol: 'dvol', adr: 'adr' };

  /**
   * Dim every rendered ticker that falls under a cutoff.
   *
   * A view-level pass, not a re-render: renderers emit data-dvol/data-adr and
   * this only toggles a class. That keeps scores, breadth counts, ranks, sort
   * order, and the radar's measured "+N more" chip count identical at every
   * cutoff — nothing leaves the DOM and nothing changes size.
   *
   * Fail-open: a ticker with the attribute absent or unparseable passes. Missing
   * metrics are unknown, not disqualifying. This matters more now that the
   * filters are always on: the published JSON carries no dollar volume until the
   * next daily workflow run republishes it, and failing open means the cutoffs
   * dim nothing for one run rather than dimming everything.
   */
  function applyTickerFilters() {
    const scope = document.querySelector('.tab-content.active') || document;
    scope.querySelectorAll('[data-dvol], [data-adr]').forEach(el => {
      const belowCutoff = (key) => {
        const raw = el.dataset[FILTER_SOURCES[key]];
        if (raw === undefined || raw === '') return false;
        const value = parseFloat(raw);
        return Number.isFinite(value) && value < tickerFilters[key];
      };
      el.classList.toggle('filtered-out', belowCutoff('vol') || belowCutoff('adr'));
    });
  }

  function initTickerFilters() {
    document.addEventListener('change', (e) => {
      const select = e.target.closest('.tt-filter-select');
      if (!select) return;
      const key = select.dataset.filter;
      if (!(key in tickerFilters)) return;
      const cutoff = parseFloat(select.value);
      if (!Number.isFinite(cutoff)) return;
      tickerFilters[key] = cutoff;
      // State is shared, so every bar's copy of this select must agree.
      document.querySelectorAll(`.tt-filter-select[data-filter="${key}"]`).forEach(s => {
        s.value = select.value;
      });
      applyTickerFilters();
    });
  }

  /** Emit the filter attributes for a ticker payload. Omitted when unknown. */
  function filterAttrs(t) {
    const parts = [];
    if (typeof t.dollar_vol === 'number') parts.push(` data-dvol="${t.dollar_vol}"`);
    if (typeof t.adr_pct === 'number') parts.push(` data-adr="${t.adr_pct}"`);
    return parts.join('');
  }

  // ── HIGHLIGHT TIERS ───────────────────────────────────
  /* The server runs one ladder per ticker — a tight base, a crowded short,
     stacked averages, a stack below the 50-day — and publishes the winning rung
     as `highlight`. These three helpers turn that one value into a background
     class and a tooltip on the ticker span. They add no `data-` attribute:
     `filterAttrs` and `syncRadarClamps` keep the inputs they already read.

     An unknown value yields no class and no tooltip. Code pull requests reset
     `docs/data/`, so the field is absent until the next daily workflow run, and
     an absent field must render as nothing rather than as a rung. */
  const HL_TIER_CLASS = {
    short: 'hl-short',
    ma_up: 'hl-ma-up',
    ma_split: 'hl-ma-split',
  };

  /* The rung in words, at all nine render sites. The tints are otherwise
     colour-only, and green and orange are the two closest hues on the board, so
     a colour-vision-deficient reader cannot separate the two moving-average
     rungs from the tint alone.

     ⛔ `ma_split` states WHERE the averages sit and stops there. One reading
     covers a stock reclaiming its short-term averages from below and a stock
     that has just lost its 50-day — opposite trades this rung does not
     separate. Never give it a direction: no "turning", "rolling over",
     "recovering", "weakening", "breaking down". */
  const HL_TIER_TIP = {
    tight: 'in a tight range',
    short: 'short interest 20% of float or more',
    ma_up: 'EMA10 above EMA20 above SMA50',
    ma_split: 'EMA10 above EMA20, EMA20 not above SMA50',
  };

  /** The tier class for a bare ticker span, with a leading space, or ''.
   *
   * The tight rung maps here because the eight table tabs carry no `tight`
   * boolean — only the tier. Drop it and the violet tint stays on the Themes
   * chip, leaving the ladder's top rung invisible on eight of the nine tabs.
   */
  function hlClass(tier) {
    const cls = tier === 'tight' ? 'tight-range' : HL_TIER_CLASS[tier];
    return cls ? ' ' + cls : '';
  }

  /** The tier class for a radar chip's class array, or '' for none.
   *
   * Tight is deliberately missing: `renderThemes` pushes `tight-range` from
   * the radar's own boolean, so mapping the tier here too would put one class
   * on the chip twice.
   */
  function hlChipClass(tier) {
    return HL_TIER_CLASS[tier] || '';
  }

  /** The tier tooltip as a ready-to-interpolate title attribute, or ''. */
  function hlTitle(tier) {
    const tip = HL_TIER_TIP[tier];
    return tip ? ` title="${escAttr(tip)}"` : '';
  }

  // ── COLOR SCHEME REGISTRY ─────────────────────────────
  /* Every threshold that picks a colour lives here, once. The render sites
     call `pickBand` on these tables, and the Legends tab (docs/legends.js)
     reads the SAME tables through `window.MM_COLORS` to draw its rows. So a
     retuned threshold changes the tab and its legend in one edit, and a
     retuned CSS colour reaches both because the legend paints its samples with
     the real classes.

     A table is read top-down and the first band whose test passes wins.
     `missing` covers null, '' and anything that does not parse as a number;
     `otherwise` covers a number that no band claims.

     ⛔ Do not inline a threshold back into a render site. The legend cannot
     see an inline literal, so it would keep describing the old rule.
     tests/test_dashboard_legends.py fails if a render site stops calling
     `pickBand` for these columns. */

  // Short interest at or above this share of float reads as crowded. It is the
  // same floor the highlight ladder's short rung uses (HIGHLIGHT_SHORT_FLOOR in
  // create_technical_indicators.py); tests/test_dashboard_highlight_markup.py
  // binds the two, so the tint and the number beside it cannot disagree.
  const SHORT_CROWDED_PCT = 20;

  const COLOR_BANDS = {
    // RS% column — Momentum, Volume, VARS.
    rs: {
      bands: [{ op: '>=', at: 80, out: 'up' }, { op: '<=', at: 20, out: 'dn' }],
      otherwise: '', missing: '',
    },
    // VARS column — Volume, VARS, Industry, Lev ETF.
    vars: {
      bands: [{ op: '>=', at: 6, out: 'up' }, { op: '<', at: 2, out: 'dn' }],
      otherwise: '', missing: '',
    },
    // Short% column (Momentum, Volume, VARS, Parabolic) and SI% (SI tab).
    short: {
      bands: [{ op: '>=', at: SHORT_CROWDED_PCT, out: 'up' }, { op: '>=', at: 10, out: 'short-blue' }],
      otherwise: 'short-white', missing: 'neu',
    },
    // Inst% column — the latest change in institutional ownership.
    inst: {
      bands: [{ op: '>', at: 0, out: 'up' }, { op: '<', at: 0, out: 'dn' }],
      otherwise: 'neu', missing: 'neu',
    },
    // Signed percent columns: EPS%, Sales%, Intra%, Daily%, Monthly%.
    pct: {
      bands: [{ op: '>', at: 0, out: 'up' }, { op: '<', at: 0, out: 'dn' }],
      otherwise: 'neu', missing: 'neu',
    },
    // Parabolic tab, ATRMul: distance from the 50-day SMA in ATR units.
    parabolicAtr: {
      bands: [{ op: '>=', at: 15, out: 'dn' }, { op: '>=', at: 12, out: 'short-blue' }],
      otherwise: 'neu', missing: 'neu',
    },
    // Overview: macro price and change columns. `pos`/`neg` rather than
    // `up`/`dn` because these carry !important over the price cell class.
    macroChange: {
      bands: [{ op: '>', at: 0, out: 'pos' }, { op: '<', at: 0, out: 'neg' }],
      otherwise: 'neu', missing: 'neu',
    },
    // Overview breadth tiles. The four breadth tiles tint by CONTRARIAN
    // meaning: washed-out breadth is a bullish setup, so a low reading is green.
    fearGreed: {
      bands: [{ op: '>=', at: 50, out: 'up' }],
      otherwise: 'dn', missing: 'neu',
    },
    ncfd: {
      bands: [{ op: '<', at: 20, out: 'up' }, { op: '>', at: 85, out: 'dn' }],
      otherwise: 'neu', missing: 'neu',
    },
    mmfi: {
      bands: [{ op: '<', at: 15.5, out: 'up' }],
      otherwise: 'neu', missing: 'neu',
    },
    mmtw: {
      bands: [{ op: '<', at: 20, out: 'up' }],
      otherwise: 'neu', missing: 'neu',
    },
    mmth: {
      bands: [{ op: '<', at: 20, out: 'up' }],
      otherwise: 'neu', missing: 'neu',
    },
    // NASI header figures.
    nasiOsc: {
      bands: [{ op: '>=', at: 0, out: 'pos' }],
      otherwise: 'neg', missing: '',
    },
    nasiRsi: {
      bands: [
        { op: '<=', at: NASI_OVERSOLD, out: 'oversold' },
        { op: '<=', at: NASI_WATCH, out: 'watch' },
        { op: '>=', at: NASI_OVERBOUGHT, out: 'overbought' },
      ],
      otherwise: '', missing: '',
    },
  };

  const BAND_TESTS = {
    '>=': (v, at) => v >= at,
    '>': (v, at) => v > at,
    '<=': (v, at) => v <= at,
    '<': (v, at) => v < at,
  };

  /** The output of the first band `raw` satisfies. Strings like "+12.3%" parse. */
  function pickBand(table, raw) {
    if (raw == null || raw === '') return table.missing;
    const v = typeof raw === 'number' ? raw : parseFloat(String(raw).replace(/[+%,]/g, ''));
    if (!Number.isFinite(v)) return table.missing;
    for (const b of table.bands) {
      if (BAND_TESTS[b.op](v, b.at)) return b.out;
    }
    return table.otherwise;
  }

  /* Rules the SERVER evaluates, mirrored here so the Legends tab can state them
     and draw candles that satisfy them. The browser never re-decides these —
     it reads `ticker_color` and `highlight` off the payload. The mirror is
     pinned to its Python and config sources by tests/test_dashboard_legends.py,
     so a retuned server rule fails that test until this block follows it. */
  const PATTERN_RULES = {
    // Day-pattern green (create_technical_indicators.py).
    tightDayBodyAdr: 0.2,     // tight_day: |close - open| / close < 0.2 x ADR%
    closeToMaAtr: 0.5,        // close_to_ma: |close - EMA10 or EMA20| < 0.5 x ATR14
    // Tight rung (config/workflow_config.yaml `tight_range:`).
    tightMinWindow: 3,        // shortest window of closes
    tightMaxWindow: 10,       // longest window of closes
    tightRatioMax: 0.35,      // close range / (ADR% x sqrt(N)) at or under this
    tightPctileMax: 15,       // ...or in this own-history percentile
    tightPctileLookback: 120, // sessions the percentile reads
    tightMaHoldAdr: 0.6,      // MA-hold: lowest close within this many ADRs of a rising MA
    tightEmaRolloverAdr: 0.1, // MA-hold: EMA10 at most this many ADRs below EMA20
    tightSupportAdr: 1.0,     // support: lowest close within this many ADRs of a swing low
    // Short rung.
    shortFloorPct: SHORT_CROWDED_PCT,
    // HOT badges.
    varsHotRs: 70,            // vars_tab.hot_rs_threshold
    varsHotMinMembers: 3,     // export_dashboard_data._build_vars_snapshot
    siHotRadarRank: 10,       // si_tab.hot_radar_rank
  };

  /* US equity session windows in ET minutes after midnight, for the header
     badge. Anything outside them, and every weekend minute, reads closed. */
  const MARKET_SESSIONS = [
    { from: 240, to: 570, cls: 'premarket', text: 'PRE-MARKET' },
    { from: 570, to: 960, cls: 'open', text: 'US MARKET LIVE' },
    { from: 960, to: 1200, cls: 'premarket', text: 'AFTER HOURS' },
  ];
  const MARKET_CLOSED = { cls: 'closed', text: 'MARKET CLOSED' };

  /* Moving-average line colours on the TradingView chart. The free embed
     applies one colour per study TYPE, so both EMAs share one colour and both
     SMAs share the other. */
  const CHART_MA_COLORS = { ema: '#4CAF50', sma: '#FFD700' };

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

  /* A read-only view for docs/legends.js, which renders outside this closure.
     The Legends tab reads ONLY this object, so anything it describes must be
     reachable from here — never copied into legends.js as a literal. */
  window.MM_COLORS = Object.freeze({
    COLOR_BANDS, pickBand, PATTERN_RULES, MARKET_SESSIONS, MARKET_CLOSED,
    CHART_MA_COLORS, ZONE_RULES, ZONE_STATES, ZONE_FAMILY_CLASS,
    HL_TIER_CLASS, HL_TIER_TIP,
    NASI: { oversold: NASI_OVERSOLD, watch: NASI_WATCH, overbought: NASI_OVERBOUGHT },
    cutoffs: () => ({ vol: tickerFilters.vol, adr: tickerFilters.adr }),
  });

  function loadThemeData() {
    // Load current themes and history in parallel
    Promise.all([
      fetch(withCacheBust(THEME_DATA_URL)).then(r => r.json()),
      fetch(withCacheBust(THEME_HISTORY_URL)).then(r => r.json()).catch(() => []),
    ])
      .then(([current, history]) => {
        // Build history: merge current into history (deduplicate by date)
        const byDate = {};
        (history || []).forEach(h => { byDate[h.report_date] = h; });
        if (current && current.report_date) {
          byDate[current.report_date] = current;
        }
        themesHistory = Object.values(byDate)
          .sort((a, b) => b.report_date.localeCompare(a.report_date));

        // Default to most recent
        if (!hasUserSelectedSession) {
          activeSessionDate = themesHistory.length > 0 ? themesHistory[0].report_date : null;
        }
        renderAllTimeTravelBars();
        renderThemes(current);
        renderZoneMap(current, 'themes');
      })
      .catch(err => {
        console.warn('Theme data not available:', err);
        document.getElementById('themes-container').innerHTML =
          '<div class="no-data">Theme data not available.<br>Run the daily workflow to generate data.</div>';
        const tz = document.getElementById('themeviz-zone');
        if (tz) tz.innerHTML = '<div class="no-data">Theme data not available.</div>';
      });
  }

  function loadMomentumData() {
    Promise.all([
      fetch(withCacheBust(MOMENTUM_DATA_URL)).then(r => r.json()),
      fetch(withCacheBust(MOMENTUM_HISTORY_URL)).then(r => r.json()).catch(() => []),
    ])
      .then(([current, history]) => {
        const byDate = {};
        (history || []).forEach(h => { byDate[h.report_date] = h; });
        if (current && current.report_date) {
          byDate[current.report_date] = current;
        }
        momentumHistory = Object.values(byDate)
          .sort((a, b) => b.report_date.localeCompare(a.report_date));
        renderAllTimeTravelBars();
        renderMomentum(current);
        renderZoneMap(current, 'momentum');
      })
      .catch(err => {
        console.warn('Momentum data not available:', err);
        document.getElementById('momentum-container').innerHTML =
          '<div class="no-data">Momentum data not available.<br>Run the daily workflow to generate data.</div>';
        const mz = document.getElementById('momentumviz-zone');
        if (mz) mz.innerHTML = '<div class="no-data">Momentum data not available.</div>';
      });
  }

  function loadVolumeData() {
    Promise.all([
      fetch(withCacheBust(VOLUME_DATA_URL)).then(r => r.json()),
      fetch(withCacheBust(VOLUME_HISTORY_URL)).then(r => r.json()).catch(() => []),
    ])
      .then(([current, history]) => {
        const byDate = {};
        (history || []).forEach(h => { byDate[h.report_date] = h; });
        if (current && current.report_date) {
          byDate[current.report_date] = current;
        }
        volumeHistory = Object.values(byDate)
          .sort((a, b) => b.report_date.localeCompare(a.report_date));
        renderAllTimeTravelBars();
        renderVolume(current);
        renderZoneMap(current, 'volume');
      })
      .catch(err => {
        console.warn('Volume data not available:', err);
        const c = document.getElementById('volume-container');
        if (c) c.innerHTML = '<div class="no-data">Volume data not available.<br>Run the daily workflow to generate data.</div>';
        const vz = document.getElementById('volumeviz-zone');
        if (vz) vz.innerHTML = '<div class="no-data">Volume data not available.</div>';
      });
  }

  function loadVARSData() {
    Promise.all([
      fetch(withCacheBust(VARS_DATA_URL)).then(r => r.json()),
      fetch(withCacheBust(VARS_HISTORY_URL)).then(r => r.json()).catch(() => []),
    ])
      .then(([current, history]) => {
        const byDate = {};
        (history || []).forEach(h => { byDate[h.report_date] = h; });
        if (current && current.report_date) {
          byDate[current.report_date] = current;
        }
        varsHistory = Object.values(byDate)
          .sort((a, b) => b.report_date.localeCompare(a.report_date));
        renderAllTimeTravelBars();
        renderVARS(current);
        renderZoneMap(current, 'vars');
      })
      .catch(err => {
        console.warn('VARS data not available:', err);
        const c = document.getElementById('vars-container');
        if (c) c.innerHTML = '<div class="no-data">VARS data not available.<br>Run the daily workflow to generate data.</div>';
        const vz = document.getElementById('varsviz-zone');
        if (vz) vz.innerHTML = '<div class="no-data">VARS data not available.</div>';
      });
  }

  // ETF Viz: the long Lev ETFs on the zone map. Its history grows one entry
  // per daily run, like the Lev ETF tab's.
  function loadETFVizData() {
    Promise.all([
      fetch(withCacheBust(ETF_VIZ_DATA_URL)).then(r => { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); }),
      fetch(withCacheBust(ETF_VIZ_HISTORY_URL)).then(r => r.json()).catch(() => []),
      // The newest session, for the stale note. Read here, not from another
      // tab's loader, so it is set before this tab's first render.
      fetch(withCacheBust(META_URL)).then(r => r.json()).catch(() => null),
    ])
      .then(([current, history, meta]) => {
        latestSessionDate = (meta && meta.report_date) || null;
        const byDate = {};
        (history || []).forEach(h => { byDate[h.report_date] = h; });
        if (current && current.report_date) {
          byDate[current.report_date] = current;
        }
        etfVizHistory = Object.values(byDate)
          .sort((a, b) => b.report_date.localeCompare(a.report_date));
        renderAllTimeTravelBars();
        renderZoneMap(current, 'etf');
      })
      .catch(err => {
        console.warn('ETF Viz data not available:', err);
        const vz = document.getElementById('etfviz-zone');
        if (vz) vz.innerHTML = '<div class="no-data">ETF Viz data not available.<br>Run the daily workflow to generate data.</div>';
      });
  }

  function loadSIData() {
    Promise.all([
      fetch(withCacheBust(SI_DATA_URL)).then(r => { if (!r.ok) throw new Error(); return r.json(); }),
      fetch(withCacheBust(SI_HISTORY_URL)).then(r => r.json()).catch(() => []),
    ])
      .then(([current, history]) => {
        const byDate = {};
        (history || []).forEach(h => { byDate[h.report_date] = h; });
        if (current && current.report_date) {
          byDate[current.report_date] = current;
        }
        siHistory = Object.values(byDate)
          .sort((a, b) => b.report_date.localeCompare(a.report_date));
        renderAllTimeTravelBars();
        renderSI(current);
      })
      .catch(err => {
        console.warn('SI data not available:', err);
        const c = document.getElementById('si-container');
        if (c) c.innerHTML = '<div class="no-data">SI data not available.<br>Run the daily workflow to generate data.</div>';
      });
  }

  function loadParabolicData() {
    Promise.all([
      fetch(withCacheBust(PARABOLIC_DATA_URL)).then(r => { if (!r.ok) throw new Error(); return r.json(); }),
      fetch(withCacheBust(PARABOLIC_HISTORY_URL)).then(r => r.json()).catch(() => []),
    ])
      .then(([current, history]) => {
        const byDate = {};
        (history || []).forEach(h => { byDate[h.report_date] = h; });
        if (current && current.report_date) {
          byDate[current.report_date] = current;
        }
        parabolicHistory = Object.values(byDate)
          .sort((a, b) => b.report_date.localeCompare(a.report_date));
        if (!activeSessionDate && parabolicHistory.length > 0) {
          activeSessionDate = parabolicHistory[0].report_date;
        }
        const activeSnap = activeSessionDate
          ? parabolicHistory.find(h => h.report_date === activeSessionDate)
          : null;
        parabolicData = (activeSnap || current || {}).tickers || [];
        renderAllTimeTravelBars();
        sortAndRenderParabolic();
      })
      .catch(err => {
        console.warn('Parabolic data not available:', err);
        document.getElementById('parabolic-body').innerHTML =
          '<tr><td colspan="5" class="no-data">Parabolic data not available.</td></tr>';
      });
  }

  /** Session dates one tab has data for (newest first). Each bar lists only
   *  its own tab's finished sessions — never a calendar date another tab
   *  wrote first. */
  function tabSessionDates(...histories) {
    const dates = new Set();
    histories.forEach(hist => (hist || []).forEach(h => dates.add(h.report_date)));
    return [...dates].sort().reverse();
  }

  function onTimeTravelSelect(date) {
    // Each tab's bar lists only its own history's dates, but the active
    // session is shared across tabs — after switching tabs the active date
    // can be missing from the new tab's history. ALWAYS re-render every
    // tab — render functions show a date-specific empty state when the snap
    // is missing so we never leave stale content on screen.
    //
    // Themes (L1 Radar)
    const themeSnap = themesHistory.find(h => h.report_date === date);
    renderThemes(themeSnap, date);
    renderZoneMap(themeSnap, 'themes', date);
    // Momentum 1/3/6
    const momSnap = momentumHistory.find(h => h.report_date === date);
    renderMomentum(momSnap, date);
    renderZoneMap(momSnap, 'momentum', date);
    // Volume scans
    const volumeSnap = volumeHistory.find(h => h.report_date === date);
    renderVolume(volumeSnap, date);
    renderZoneMap(volumeSnap, 'volume', date);
    // VARS
    const varsSnap = varsHistory.find(h => h.report_date === date);
    renderVARS(varsSnap, date);
    renderZoneMap(varsSnap, 'vars', date);
    // Parabolic
    const parabolicSnap = parabolicHistory.find(h => h.report_date === date);
    parabolicData = parabolicSnap ? (parabolicSnap.tickers || []) : [];
    parabolicEmptyMessage = !parabolicSnap && date
      ? `No parabolic results for ${date}.`
      : 'No parabolic results for this date.';
    sortAndRenderParabolic();
    // SI (heavily shorted, already sold off)
    renderSI(siHistory.find(h => h.report_date === date), date);
    // Industry ETFs
    const indSnap = industryHistory.find(h => h.report_date === date);
    industryData = indSnap ? indSnap.data : [];
    industryEmptyMessage = !indSnap && date
      ? `No industry ETF data for ${date}.`
      : 'No industry ETF data available.';
    sortAndRenderIndustry();
    // Leverage ETFs
    const etfSnap = etfHistory.find(h => h.report_date === date);
    etfData = etfSnap ? etfSnap.data : [];
    etfEmptyMessage = !etfSnap && date
      ? `No ETF data for ${date}.`
      : 'No ETF data available.';
    sortAndRenderETF();
    // ETF Viz
    renderZoneMap(etfVizHistory.find(h => h.report_date === date), 'etf', date);
  }

  function renderAllTimeTravelBars() {
    const themesDates = tabSessionDates(themesHistory);
    const momentumDates = tabSessionDates(momentumHistory);
    const volumeDates = tabSessionDates(volumeHistory);
    const varsDates = tabSessionDates(varsHistory);
    renderTimeTravelBar('time-travel-dates', themesDates, onTimeTravelSelect);
    renderTimeTravelBar('themeviz-tt-dates', themesDates, onTimeTravelSelect);
    renderTimeTravelBar('momentum-tt-dates', momentumDates, onTimeTravelSelect);
    renderTimeTravelBar('momentumviz-tt-dates', momentumDates, onTimeTravelSelect);
    renderTimeTravelBar('volume-tt-dates', volumeDates, onTimeTravelSelect);
    renderTimeTravelBar('volumeviz-tt-dates', volumeDates, onTimeTravelSelect);
    renderTimeTravelBar('vars-tt-dates', varsDates, onTimeTravelSelect);
    renderTimeTravelBar('varsviz-tt-dates', varsDates, onTimeTravelSelect);
    renderTimeTravelBar('industry-tt-dates', tabSessionDates(industryHistory), onTimeTravelSelect);
    renderTimeTravelBar('etf-tt-dates', tabSessionDates(etfHistory), onTimeTravelSelect);
    renderTimeTravelBar('etfviz-tt-dates', tabSessionDates(etfVizHistory), onTimeTravelSelect);
    renderTimeTravelBar('parabolic-tt-dates', tabSessionDates(parabolicHistory), onTimeTravelSelect);
    renderTimeTravelBar('si-tt-dates', tabSessionDates(siHistory), onTimeTravelSelect);
  }

  /**
   * Render a time-travel date-selector bar.
   * Shows the last 3 sessions as clickable buttons and the rest (every session
   * within the last 180 calendar days) in a dropdown to the right so users can
   * jump farther back without clutter.
   *
   * @param {string} containerId  - DOM id of the .time-travel-dates element
   * @param {Array}  dates        - ordered list of report_date strings (newest first)
   * @param {Function} onSelect   - callback(date) when user picks a date
   */
  function renderTimeTravelBar(containerId, dates, onSelect) {
    const container = document.getElementById(containerId);
    if (!container || dates.length === 0) return;

    const weekdays = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
    // First N sessions render as buttons; the rest go in the "+ more" dropdown.
    // One, not three: the V/A cutoff dropdowns are much wider than the toggle
    // buttons they replaced, and three date buttons plus both cutoffs no longer
    // fit one row at the left panel's default width. Raising this again means
    // re-measuring the .left-panel width floor.
    const VISIBLE = 1;
    const fmt = (rd) => {
      const d = new Date(rd + 'T12:00:00');
      const wd = weekdays[d.getDay()];
      const parts = rd.split('-');
      return { label: `${parts[1]}/${parts[2]}`, wd };
    };

    const buttons = dates.slice(0, VISIBLE);
    const dropdown = dates.slice(VISIBLE);

    let html = buttons.map(rd => {
      const { label, wd } = fmt(rd);
      const isActive = rd === activeSessionDate ? ' active' : '';
      return `<button class="tt-date-btn${isActive}" data-date="${rd}">${label}<span class="tt-weekday">${wd}</span></button>`;
    }).join('');

    if (dropdown.length > 0) {
      const activeInDropdown = dropdown.includes(activeSessionDate);
      const selectedDate = activeInDropdown ? activeSessionDate : '';
      const selectLabel = activeInDropdown
        ? (() => { const { label, wd } = fmt(activeSessionDate); return `${label} ${wd}`; })()
        : '+ more';
      const options = dropdown.map(rd => {
        const { label, wd } = fmt(rd);
        const sel = rd === activeSessionDate ? ' selected' : '';
        return `<option value="${rd}"${sel}>${label} ${wd}</option>`;
      }).join('');
      html += `
        <select class="tt-date-select${activeInDropdown ? ' active' : ''}" data-tt-select="1">
          <option value="" disabled${activeInDropdown ? '' : ' selected'}>${selectLabel}</option>
          ${options}
        </select>
      `;
    }

    container.innerHTML = html;

    container.querySelectorAll('.tt-date-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        const date = btn.dataset.date;
        if (date === activeSessionDate && hasUserSelectedSession) return;
        applyTimeTravelDate(date, onSelect);
      });
    });

    const select = container.querySelector('.tt-date-select');
    if (select) {
      select.addEventListener('change', (e) => {
        const date = e.target.value;
        if (!date) return;
        applyTimeTravelDate(date, onSelect);
      });
    }
  }

  function applyTimeTravelDate(date, onSelect) {
    activeSessionDate = date;
    hasUserSelectedSession = true;
    // Re-render every time-travel bar so dropdown selection state stays in sync.
    renderAllTimeTravelBars();
    onSelect(date);
  }

  // ── ZONE MAP — the five Viz tabs ───────────────────────────────────
  // Each ticker sits at (close vs the daily EMA20, close vs the weekly
  // EMA20), both in daily ATR14. The server decides every zone and state
  // (export_dashboard_data.build_zone_map); this code only draws. It paints
  // with CSS classes, never with colour literals. The SVG draws at its own
  // pixel width, so it waits for a visible tab and redraws on a resize.

  const ZONE_MODES = {
    themes: { containerId: 'themeviz-zone', tabId: 'themeviz', metric: 'vars' },
    vars: { containerId: 'varsviz-zone', tabId: 'varsviz', metric: 'vars' },
    momentum: { containerId: 'momentumviz-zone', tabId: 'momentumviz', metric: 'rs' },
    volume: { containerId: 'volumeviz-zone', tabId: 'volumeviz', metric: 'vars' },
    etf: { containerId: 'etfviz-zone', tabId: 'etfviz', metric: 'vars' },
  };

  const ZONE_METRIC = {
    vars: { key: 'vars', label: 'VARS', digits: 1 },
    rs: { key: 'rs', label: 'RS', digits: 0 },
  };

  // Leaf rows shown in the clusters card before the "N other themes" line.
  const ZONE_CLUSTER_ROWS = 8;

  // The SVG draws at its real pixel width, one viewBox unit per CSS pixel,
  // so text keeps its size when the panel divider moves. A width change
  // redraws the map. Margins are in pixels; the height follows the width
  // between two limits.
  const ZM_PAD = { l: 46, r: 14, t: 40, b: 34 };
  const ZM_ASPECT = 0.62;
  const ZM_HEIGHT = [360, 640];

  // Font sizes in px. They match .zm-axis, .zm-label and .zm-head in
  // style.css, and they feed the text measurement of the label placer.
  const ZM_FONT = { axis: 9, label: 10, head: 10 };

  // Label placer: the label row height, and how far a label may move from
  // its dot on a leader line.
  const ZM_LABEL = { h: 11, reach: 150, step: 6, angles: 24 };

  // The placer's passes, strictest first. `dots`: a label may not cover a
  // dot. `far`: the rings run to the edge of the plot, not to `reach`.
  // `cross`: a leader may cross another leader. `under`: a leader may pass
  // under a label. A name ringed by its neighbours' labels, such as an
  // off-scale name in a crowded corner, finds room only in the last pass.
  // Two labels never overlap in any pass.
  const ZM_LABEL_PASSES = [
    { dots: true, far: false, cross: false, under: false },
    { dots: false, far: false, cross: false, under: false },
    { dots: true, far: true, cross: false, under: false },
    { dots: false, far: true, cross: true, under: false },
    { dots: false, far: true, cross: true, under: true },
  ];

  function zmGeom(w) {
    const h = Math.round(Math.min(ZM_HEIGHT[1], Math.max(ZM_HEIGHT[0], w * ZM_ASPECT)));
    return { w, h, ...ZM_PAD };
  }

  function zmX(g, v) {
    const [a, b] = ZONE_RULES.xRange;
    return g.l + (v - a) / (b - a) * (g.w - g.l - g.r);
  }

  function zmY(g, v) {
    const [a, b] = ZONE_RULES.yRange;
    return g.h - g.b - (v - a) / (b - a) * (g.h - g.t - g.b);
  }

  /** Text width in px at the SVG's own font. The map draws only on a
   *  visible tab, so the font is resolved. */
  let zmCanvas = null;
  function zmMeasurer(svg) {
    const family = getComputedStyle(svg).fontFamily || 'monospace';
    zmCanvas = zmCanvas || document.createElement('canvas');
    const ctx = zmCanvas.getContext('2d');
    if (!ctx) return (text, px) => text.length * px * 0.62;
    return (text, px) => {
      ctx.font = `${px}px ${family}`;
      return ctx.measureText(text).width;
    };
  }

  function zmBoxesOverlap(a, b) {
    return a.x < b.x + b.w && b.x < a.x + a.w && a.y < b.y + b.h && b.y < a.y + a.h;
  }

  /** True when segment s (x1,y1 → x2,y2) touches box b (Liang–Barsky). */
  function zmSegHitsBox(s, b) {
    const dx = s.x2 - s.x1;
    const dy = s.y2 - s.y1;
    const p = [-dx, dx, -dy, dy];
    const q = [s.x1 - b.x, b.x + b.w - s.x1, s.y1 - b.y, b.y + b.h - s.y1];
    let t0 = 0;
    let t1 = 1;
    for (let i = 0; i < 4; i++) {
      if (p[i] === 0) {
        if (q[i] < 0) return false;
        continue;
      }
      const r = q[i] / p[i];
      if (p[i] < 0) {
        if (r > t1) return false;
        if (r > t0) t0 = r;
      } else {
        if (r < t0) return false;
        if (r < t1) t1 = r;
      }
    }
    return true;
  }

  /** True when two segments cross at a point inside both. */
  function zmSegsCross(a, b) {
    const side = (x1, y1, x2, y2, x, y) => Math.sign((x2 - x1) * (y - y1) - (y2 - y1) * (x - x1));
    return side(a.x1, a.y1, a.x2, a.y2, b.x1, b.y1) * side(a.x1, a.y1, a.x2, a.y2, b.x2, b.y2) < 0
      && side(b.x1, b.y1, b.x2, b.y2, a.x1, a.y1) * side(b.x1, b.y1, b.x2, b.y2, a.x2, a.y2) < 0;
  }

  /** The label spots for one dot, nearest first. The first eight sit beside
   *  the dot. The rest walk outward in rings and need a leader line. */
  function zmLabelSpots(q, w, h, reach) {
    const spots = [];
    const gap = q.r + 2;
    const d = gap * 0.75;
    [
      [q.x + gap, q.y - h / 2], [q.x - gap - w, q.y - h / 2],
      [q.x - w / 2, q.y - gap - h], [q.x - w / 2, q.y + gap],
      [q.x + d, q.y - d - h], [q.x - d - w, q.y - d - h],
      [q.x + d, q.y + d], [q.x - d - w, q.y + d],
    ].forEach(([x, y]) => spots.push({ x, y, lead: false }));
    for (let rad = gap + 10; rad <= reach; rad += ZM_LABEL.step) {
      for (let k = 0; k < ZM_LABEL.angles; k++) {
        const a = 2 * Math.PI * k / ZM_LABEL.angles;
        const c = Math.cos(a);
        const s = Math.sin(a);
        const px = q.x + rad * c;
        const py = q.y + rad * s;
        // Put the near edge of the box on the ring, so the leader stays short.
        const x = c > 0.3 ? px : c < -0.3 ? px - w : px - w / 2;
        const y = s > 0.3 ? py : s < -0.3 ? py - h : py - h / 2;
        spots.push({ x, y, lead: true });
      }
    }
    return spots;
  }

  /** The leader from the edge of the dot to the nearest point of the box. */
  function zmLeader(q, box) {
    const tx = Math.min(box.x + box.w, Math.max(box.x, q.x));
    const ty = Math.min(box.y + box.h, Math.max(box.y, q.y));
    const len = Math.hypot(tx - q.x, ty - q.y) || 1;
    return { x1: q.x + (tx - q.x) / len * q.r, y1: q.y + (ty - q.y) / len * q.r, x2: tx, y2: ty };
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

  /** The tab's row number for sorting; a missing one sorts last. */
  function zoneMetricValue(t, metric) {
    return t[metric.key] == null ? -Infinity : t[metric.key];
  }

  function zoneMetricText(t, metric) {
    const v = t[metric.key];
    return v == null ? '—' : Number(v).toFixed(metric.digits);
  }

  function zoneNum(t, metric) {
    const v = t[metric.key];
    if (v == null) return '';
    // `neu` is the grey the band tables fall back to elsewhere.
    const cls = (metric.key === 'rs' ? pickBand(COLOR_BANDS.rs, v) : pickBand(COLOR_BANDS.vars, v)) || 'neu';
    return ` <span class="zm-num ${cls}">${zoneMetricText(t, metric)}</span>`;
  }

  function zoneSortByMetric(list, metric) {
    return list.slice().sort((a, b) =>
      zoneMetricValue(b, metric) - zoneMetricValue(a, metric) || a.ticker.localeCompare(b.ticker));
  }

  /** A ticker in the states or clusters card: a real .tn-link, so it wears
   *  the day-pattern text, the tier tint and the selection like every tab. */
  function zoneTickerLink(t, metric, bold) {
    return `<span class="zm-tk${bold ? ' zm-buy' : ''}"><span class="tn-link${t.ticker_color === 'green' ? ' day-pattern-green' : ''}${hlClass(t.highlight)}"${hlTitle(t.highlight)} data-sym="${escAttr(t.ticker)}" data-nm="${escAttr(t.ticker + ' · Zone map')}">${escHtml(t.ticker)}</span>${metric ? zoneNum(t, metric) : ''}</span>`;
  }

  /** An off-scale name: a triangle on the nearest edge, pointing outward. */
  function zoneTriangle(p) {
    const t = p.t;
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

  /** The inside of the scatter SVG, drawn at geometry g (pixels). `measure`
   *  gives text widths; `sel` is the selected ticker, kept across redraws. */
  function zoneScatterSvg(zm, metric, g, measure, sel) {
    const R = ZONE_RULES;
    const tickers = zm.tickers;
    const X = v => zmX(g, v);
    const Y = v => zmY(g, v);
    const xs = [R.xRange[0], R.bandLow, R.bandHigh, R.xRange[1]].map(X);
    const yTop = Y(R.yRange[1]);
    const yBot = Y(R.yRange[0]);
    const families = Object.keys(ZONE_FAMILY_CLASS);
    // Long head first; the short one when the column is too narrow for it.
    const heads = {
      under: ['Under the daily band', 'Under'],
      band: ['In the daily band', 'In band'],
      above: ['Above the daily band', 'Above'],
    };
    const counts = Object.fromEntries(families.map(f => [f, 0]));
    tickers.forEach(t => { counts[t.zone_daily] += 1; });
    const nBuy = tickers.filter(t => t.state === 'buy_area').length;

    let svg = '';
    families.forEach((f, i) => {
      const mid = (xs[i] + xs[i + 1]) / 2;
      const colW = xs[i + 1] - xs[i];
      const head = heads[f].find(h => measure(h, ZM_FONT.head) <= colW - 4) || '';
      svg += `<rect class="zm-bg ${ZONE_FAMILY_CLASS[f]}" x="${zmF(xs[i])}" y="${zmF(yTop)}" width="${zmF(colW)}" height="${zmF(yBot - yTop)}"/>`;
      svg += `<text class="zm-head" x="${zmF(mid)}" y="${zmF(yTop - 20)}">${escHtml(head)}</text>`;
      svg += `<text class="zm-axis" x="${zmF(mid)}" y="${zmF(yTop - 8)}">${counts[f]} of ${tickers.length}</text>`;
    });

    const rail = (x1, y1, x2, y2, extra) =>
      `<line class="zm-rail${extra ? ' ' + extra : ''}" x1="${zmF(x1)}" y1="${zmF(y1)}" x2="${zmF(x2)}" y2="${zmF(y2)}"/>`;
    const yLow = Y(R.weeklyLow);
    const yHigh = Y(R.weeklyHigh);
    const lowText = 'weekly EMA20';
    const highText = `weekly extended · > +${R.weeklyHigh} ATR`;
    const tagText = `Buy area · ${nBuy}`;
    svg += rail(xs[1], yTop, xs[1], yBot) + rail(xs[2], yTop, xs[2], yBot);
    svg += rail(xs[0], yLow, xs[3], yLow);
    svg += rail(xs[0], yHigh, xs[3], yHigh, 'zm-rail-dash');
    svg += `<text class="zm-axis zm-left" x="${zmF(xs[0] + 4)}" y="${zmF(yLow - 4)}">${escHtml(lowText)}</text>`;
    svg += `<text class="zm-axis zm-left" x="${zmF(xs[0] + 4)}" y="${zmF(yHigh - 4)}">${escHtml(highText)}</text>`;

    svg += `<rect class="zm-box" x="${zmF(xs[1])}" y="${zmF(yHigh)}" width="${zmF(xs[2] - xs[1])}" height="${zmF(yLow - yHigh)}"/>`;
    // Drawn above the dots (see the return), so no dot hides it.
    const tag = `<text class="zm-box-tag" x="${zmF((xs[1] + xs[2]) / 2)}" y="${zmF(yHigh + 13)}">${escHtml(tagText)}</text>`;

    for (let v = Math.ceil(R.xRange[0]); v <= R.xRange[1]; v += 1) {
      svg += `<text class="zm-axis" x="${zmF(X(v))}" y="${zmF(yBot + 13)}">${v > 0 ? '+' + v : v}</text>`;
    }
    for (let v = Math.ceil(R.yRange[0] / 2) * 2; v <= R.yRange[1]; v += 2) {
      svg += `<text class="zm-axis zm-right" x="${zmF(xs[0] - 5)}" y="${zmF(Y(v) + 3)}">${v > 0 ? '+' + v : v}</text>`;
    }
    svg += `<text class="zm-axis" x="${zmF((xs[0] + xs[3]) / 2)}" y="${g.h - 4}">daily: close vs EMA20, in ATR →</text>`;
    svg += `<text class="zm-axis" transform="translate(11 ${zmF((yTop + yBot) / 2)}) rotate(-90)">weekly: close vs EMA20, in ATR →</text>`;

    // Positions. `r` is the space the dot (and its tight ring) takes.
    const pts = tickers.map(t => {
      const cx = zmClamp(t.d_daily, R.xRange);
      const cy = zmClamp(t.d_weekly, R.yRange);
      return { t, x: X(cx), y: Y(cy), r: t.tight ? 7 : 5,
        off: cx !== t.d_daily || cy !== t.d_weekly, box: null, lead: null };
    });

    // Labels: every name gets one. A name tries the eight spots beside its
    // dot, then rings farther out on a leader line. A spot must stay in the
    // plot, and must never cover another label or the fixed text. The
    // ZM_LABEL_PASSES rules relax step by step; if no pass finds a spot, the
    // label goes beside its dot. Buy-area names place first, then tight
    // names, then the rest by the row number, so the names that matter most
    // sit closest to their dots.
    const tagW = measure(tagText, ZM_FONT.label);
    const fixedText = [
      { x: (xs[1] + xs[2]) / 2 - tagW / 2, y: yHigh + 4, w: tagW, h: 11 },
      { x: xs[0] + 4, y: yLow - 12, w: measure(lowText, ZM_FONT.axis), h: 10 },
      { x: xs[0] + 4, y: yHigh - 12, w: measure(highText, ZM_FONT.axis), h: 10 },
    ];
    const placed = [];
    placed.push(...fixedText);
    const leaders = [];
    const dots = pts.map(q => ({ x: q.x - q.r, y: q.y - q.r, w: 2 * q.r, h: 2 * q.r, q }));
    const plot = { x0: xs[0] + 1, x1: xs[3] - 1, y0: yTop + 1, y1: yBot - 1 };
    const prio = q => (q.t.state === 'buy_area' ? 0 : q.t.tight ? 1 : 2);
    const order = pts.slice().sort((a, b) => prio(a) - prio(b)
      || zoneMetricValue(b.t, metric) - zoneMetricValue(a.t, metric));
    order.forEach(q => {
      const w = measure(q.t.ticker, ZM_FONT.label) + 2;
      const h = ZM_LABEL.h;
      const fits = (spot, pass) => {
        const box = { x: spot.x, y: spot.y, w, h };
        if (box.x < plot.x0 || box.x + w > plot.x1 || box.y < plot.y0 || box.y + h > plot.y1) return null;
        if (placed.some(o => zmBoxesOverlap(box, o))) return null;
        if (pass.dots && dots.some(d => d.q !== q && zmBoxesOverlap(box, d))) return null;
        if (!pass.under && leaders.some(s => zmSegHitsBox(s, box))) return null;
        const lead = spot.lead ? zmLeader(q, box) : null;
        if (lead && ((!pass.under && placed.some(o => zmSegHitsBox(lead, o)))
          || (!pass.cross && leaders.some(s => zmSegsCross(lead, s))))) return null;
        return { box, lead };
      };
      let hit = null;
      for (const pass of ZM_LABEL_PASSES) {
        const reach = pass.far ? Math.max(g.w, g.h) : ZM_LABEL.reach;
        for (const spot of zmLabelSpots(q, w, h, reach)) {
          hit = fits(spot, pass);
          if (hit) break;
        }
        if (hit) break;
      }
      if (!hit) {
        const x = q.x + q.r + 2 + w > plot.x1 ? q.x - q.r - 2 - w : q.x + q.r + 2;
        hit = { box: { x, y: q.y - h / 2, w, h }, lead: null };
      }
      const box = hit.box;
      placed.push(box);
      q.box = box;
      if (hit.lead) {
        leaders.push(hit.lead);
        q.lead = hit.lead;
      }
    });

    // Two layers: dots and leaders first, then every label above them, so no
    // dot or leader hides a label. Both groups of a name carry its symbol,
    // so a click or the selection reaches either one.
    let marks = '';
    let labels = '';
    pts.forEach(q => {
      const t = q.t;
      const title = `<title>${escHtml(`${t.ticker} · ${zoneStateLabel(t.state)}\n`
        + `Daily ${zmSigned(t.d_daily)} ATR · Weekly ${zmSigned(t.d_weekly)} ATR\n`
        + `${metric.label} ${zoneMetricText(t, metric)}`
        + (t.tight ? `\nTight range${t.tight_len ? ' ' + t.tight_len + 'd' : ''}` : ''))}</title>`;
      const shape = q.off
        ? zoneTriangle(q)
        : `<circle class="zm-dot ${ZONE_FAMILY_CLASS[t.zone_daily]}" cx="${zmF(q.x)}" cy="${zmF(q.y)}" r="4"/>`;
      const ring = t.tight ? `<circle class="zm-ring" cx="${zmF(q.x)}" cy="${zmF(q.y)}" r="7"/>` : '';
      const lead = q.lead
        ? `<line class="zm-leader" x1="${zmF(q.lead.x1)}" y1="${zmF(q.lead.y1)}" x2="${zmF(q.lead.x2)}" y2="${zmF(q.lead.y2)}"/>`
        : '';
      const label = `<text class="zm-label${t.state === 'buy_area' ? ' zm-label-buy' : ''}" x="${zmF(q.box.x + 1)}" y="${zmF(q.box.y + q.box.h / 2 + 3.5)}">${escHtml(t.ticker)}</text>`;
      const open = `<g class="zm-pt${t.ticker === sel ? ' zm-sel' : ''}" data-zm-sym="${escAttr(t.ticker)}">`;
      marks += `${open}${title}${lead}${ring}${shape}</g>`;
      labels += `${open}${title}${label}</g>`;
    });
    return svg + marks + tag + labels;
  }

  /** Draw one tab's scatter to fill its card, in pixels. The SVG carries a
   *  fixed pixel size, so between a resize and the redraw it is clipped or
   *  short, never scaled. A hidden tab has no width, so it draws when shown. */
  function drawZoneScatter(mode) {
    const el = document.getElementById(ZONE_MODES[mode].containerId);
    const svg = el && el.querySelector('.zm-svg');
    if (!svg || !el._zm) return;
    const card = svg.parentNode;
    const cs = getComputedStyle(card);
    const w = Math.floor(card.clientWidth - parseFloat(cs.paddingLeft) - parseFloat(cs.paddingRight));
    if (!(w >= 120) || w === el._zmW) return;
    el._zmW = w;
    const g = zmGeom(w);
    svg.setAttribute('viewBox', `0 0 ${g.w} ${g.h}`);
    svg.setAttribute('width', g.w);
    svg.setAttribute('height', g.h);
    svg.innerHTML = zoneScatterSvg(el._zm.zm, el._zm.metric, g, zmMeasurer(svg), el._zmSel);
  }

  /** Redraw every visible map at its new width. The tab switch and the end
   *  of a drag call this directly. A map whose width did not change, or
   *  whose tab is hidden, returns at once. */
  function redrawZoneMapsNow() {
    clearTimeout(zmRedrawTimer);
    Object.keys(ZONE_MODES).forEach(drawZoneScatter);
  }

  // A crowded map takes tens of milliseconds to lay out, so a drag or a
  // window resize redraws once the pointer rests, not on every event.
  let zmRedrawTimer = 0;
  function redrawZoneMapsSoon() {
    clearTimeout(zmRedrawTimer);
    zmRedrawTimer = setTimeout(redrawZoneMapsNow, 60);
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
    // Count only the hidden leaves that hold a name no shown row lists.
    const remainingSet = new Set(remaining.map(t => t.ticker));
    const rest = leaves.filter(l => !shownNames.has(l.name)
      && l.members.some(t => remainingSet.has(t.ticker))).length;
    const foot = (remaining.length
      ? `${remaining.length} more ${remaining.length === 1 ? 'name sits' : 'names sit'} in ${rest} other ${rest === 1 ? 'theme' : 'themes'} or carry no theme tag. `
      : '')
      + `Buy area / total. Names in the buy area are bold. The number after each ticker is its ${metric.label}.`;
    return `<div class="card zm-card"><div class="card-lbl">Where they cluster</div>${rows || '<div class="no-data">No theme tags.</div>'}`
      + `<p class="zm-foot">${escHtml(foot)}</p></div>`;
  }

  /** Only the ETF payload carries `unmapped`: sheet ETFs with no theme, so
   *  with an unknown side, that stay off the map. Empty for the other tabs. */
  function zoneUnmappedNote(zm) {
    if (!zm.unmapped || !zm.unmapped.length) return '';
    return `<p class="zm-foot">Not on the map, no theme in config/etf_themes.yaml: ${escHtml(zm.unmapped.join(', '))}.</p>`;
  }

  /** The ETF Viz snapshot's age, shown only when it is older than the newest
   *  session. A failed ETF fetch writes no etf_viz.json, so the tab keeps the
   *  last map; without this note a dead fetch reads as a fresh one. The other
   *  Viz tabs rebuild their history every run and are never behind. A date
   *  picked in the time-travel bar is the user's choice, so it gets no note. */
  function zoneStaleNote(snap, mode, date) {
    if (mode !== 'etf' || date || !latestSessionDate || !snap.report_date) return '';
    if (snap.report_date >= latestSessionDate) return '';
    return `<div class="zm-stale-note">No ETF Viz map was published for the newest session `
      + `(${escHtml(latestSessionDate)}). This map is from ${escHtml(snap.report_date)}.</div>`;
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
      el.innerHTML = zoneStaleNote(snap, mode, date)
        + `<div class="no-data">No ticker passed the zone-map floors for ${escHtml(snap.report_date || date || 'this session')}: `
        + `${zm.n_candidates} candidates, ${zm.n_floor_dropped} under the floors, `
        + `${zm.n_no_weekly} without a weekly EMA20, ${zm.n_no_daily} without a daily reading`
        + (zm.n_no_bars ? `, ${zm.n_no_bars} with no price bars` : '')
        + '.</div>'
        + zoneUnmappedNote(zm);
      return;
    }
    const metric = ZONE_METRIC[cfg.metric];
    el.innerHTML = zoneStaleNote(snap, mode, date)
      + '<div class="card zm-card zm-plot"><svg class="zm-svg" role="img" aria-label="Zone map"></svg></div>'
      + zoneStatesCard(tickers, metric)
      + zoneClustersCard(tickers, metric)
      + zoneUnmappedNote(zm);
    // The scatter draws from this at the SVG's pixel width, now if the tab
    // is visible, else when it is shown.
    el._zm = { zm, metric };
    el._zmW = 0;
    drawZoneScatter(mode);
  }

  /** Mark one ticker selected on the dot and on every list link of the tab. */
  function selectZoneTicker(mode, sym) {
    const el = document.getElementById(ZONE_MODES[mode].containerId);
    if (!el) return;
    const q = CSS.escape(sym);
    // A redraw reads this, so the selection survives a divider drag.
    el._zmSel = sym;
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
      // Backstop only, the same lesson as syncRadarClamps: the observer
      // delivers inside the rendering lifecycle, so the drag, resize and
      // tab-switch paths call the redraw directly.
      if (typeof ResizeObserver !== 'undefined') {
        new ResizeObserver(redrawZoneMapsSoon).observe(el);
      }
    });
    window.addEventListener('resize', redrawZoneMapsSoon);
  }
  // ── END ZONE MAP ───────────────────────────────────────────────────

  // ── THEMES TAB (L1 Radar) ─────────────────────────
  // Screener-independent lens: every tagged liquid ticker scores, leaves roll
  // up into their taxonomy L1, and co-firing L1s carry a boost. Layout
  // mirrors the ranked L1 table: one block per L1, one row per
  // sub-theme with its global rank, N, ticker chips, raw and boosted scores.

  // Legacy pre-L1-rename schema shim: radar snapshots written before the L1
  // consolidation carry the L1 list under `ecosystems`. Removable once the
  // daily workflow has rewritten radar.json + radar_history.json with `l1s`.
  function radarL1s(data) {
    return (data && (data.l1s || data.ecosystems)) || null;
  }

  // Tight sort state. Off by default and reset on reload, like tickerFilters —
  // the repo uses no browser storage. It re-orders only: no score changes, no
  // ticker is hidden, and the payload is re-read rather than re-scored.
  let radarTightSort = false;
  let lastThemeSnapshot = null;
  let lastThemeDate = null;

  // Tight signal for ordering. SHARE, not raw count: L1 member counts run from
  // 2 to 214 on live data, so a raw count ranks by roster size and buries
  // exactly the small, dense theme this control exists to surface.
  // The repo has learned this twice — the tape-pressure board where group score
  // summed member margins and no single-name group ever reached the screen, and
  // the SI tab where summing was rejected for a top-K mean with breadth as a
  // tiebreak only. A minimum count keeps one tight name in a two-stock theme
  // from topping the board on a 50% share.
  const TIGHT_SORT_MIN = 2;
  // Pins shown above the board. Small on purpose: the strip has to be readable
  // in one glance to beat a scroll, and a long list is another thing to scan.
  const TIGHT_PINNED_MAX = 5;

  function tightKey(entry) {
    const n = entry.n_tight || 0;
    const total = entry.n_members || entry.n || 0;
    if (n < TIGHT_SORT_MIN || !total) return [-1, -1];
    return [n / total, n];
  }

  function byTight(a, b) {
    const ka = tightKey(a);
    const kb = tightKey(b);
    // Share, then raw count, then leave the payload's existing order alone.
    return (kb[0] - ka[0]) || (kb[1] - ka[1]);
  }

  function toggleTightSort() {
    radarTightSort = !radarTightSort;
    renderThemes(lastThemeSnapshot, lastThemeDate);
  }

  const TIGHT_GATE_TEXT = { trend: 'trend', ma_hold: 'rising MA hold', support: 'support' };

  /* One tight member in the note's own output shape:
     "tight 5d · ratio 0.25 (pctile 5) · gate: trend". Each part drops out
     when the payload lacks it, so an older payload still reads cleanly. */
  function tightSummary(t) {
    const parts = [t.tight_len ? `tight ${t.tight_len}d` : 'tight range'];
    if (typeof t.tightness === 'number') {
      const pct = (typeof t.tight_pctile === 'number')
        ? ` (pctile ${Math.round(t.tight_pctile)})` : '';
      parts.push(`ratio ${t.tightness.toFixed(2)}${pct}`);
    }
    if (t.tight_gate) parts.push(`gate: ${TIGHT_GATE_TEXT[t.tight_gate] || t.tight_gate}`);
    return parts.join(' · ');
  }

  function renderThemes(data, date) {
    const container = document.getElementById('themes-container');
    if (!container) return;

    lastThemeSnapshot = data;
    lastThemeDate = date;
    const l1s = radarL1s(data);
    if (!l1s || l1s.length === 0) {
      const msg = (date && !data) ? `No theme data for ${date}.` : 'No theme data for this date.';
      container.innerHTML = `<div class="no-data">${msg}</div>`;
      return;
    }

    const fmt = (v, d = 3) => (typeof v === 'number') ? v.toFixed(d) : '—';
    // A payload with no tight data at all (a code-only deploy resets
    // docs/data, so the fields are absent until the next workflow run) hides
    // the control rather than offering a sort that would do nothing.
    const hasTightData = l1s.some(g => typeof g.n_tight === 'number');
    const totalTight = l1s.reduce((s, g) => s + (g.n_tight || 0), 0);

    let html = '';
    if (hasTightData) {
      // The pinned strip is the whole point of the marker. A tint on a chip
      // inside the rank-27 block is only reachable by someone already
      // scrolling there, and the miss this feature exists to prevent happened
      // at rank 11 — below where the user stops reading. So the leading tight
      // themes are named ABOVE the board, at any rank, with no click needed.
      // Ranked by share, not raw count, for the byTight reason: rosters run
      // 2 to 214 members, so a raw count just lists the biggest L1s.
      const pinned = l1s
        .filter(g => (g.n_tight || 0) >= TIGHT_SORT_MIN)
        .sort(byTight)
        .slice(0, TIGHT_PINNED_MAX);
      const pins = pinned.map(g => {
        const share = Math.round((g.n_tight / (g.n_members || 1)) * 100);
        return `<button type="button" class="tight-pin" data-l1="${escAttr(g.name)}"
                 title="${g.n_tight} of ${g.n_members} members in a tight range — currently ranked #${g.rank}. Click to jump.">
                  ${escHtml(g.name)} <span class="tight-pin-n">${g.n_tight}/${g.n_members}</span>
                  <span class="tight-pin-pct">${share}%</span>
                </button>`;
      }).join('');
      html += `
        <div class="tight-strip${pinned.length ? '' : ' tight-strip-empty'}">
          <span class="tight-strip-label">TIGHT</span>
          ${pinned.length
            ? `<div class="tight-pins">${pins}</div>`
            : `<span class="radar-n">no theme has ${TIGHT_SORT_MIN}+ members in a tight range today</span>`}
        </div>
        <div class="radar-controls">
          <button type="button" id="tight-sort-btn" class="tight-sort-btn${radarTightSort ? ' on' : ''}"
                  title="Re-order themes by the share of members in a tight range. Changes order only — no score changes, nothing hidden.">
            ${radarTightSort ? '◉' : '○'} Tight first
          </button>
          <span class="radar-n">${totalTight} tight across ${l1s.length} themes</span>
        </div>
      `;
    }

    const ordered = radarTightSort ? l1s.slice().sort(byTight) : l1s;
    ordered.forEach(grp => {
      const delta = (typeof grp.delta === 'number')
        ? (grp.delta >= 0 ? `+${grp.delta.toFixed(3)}` : grp.delta.toFixed(3))
        : '—';
      html += `
        <div class="theme-block" data-l1="${escAttr(grp.name)}">
          <div class="theme-header">
            <span class="theme-rank">#${grp.rank}</span>
            <span class="theme-name">${escHtml(grp.name)}</span>
            <span class="theme-score">boosted ${fmt(grp.boosted)} · raw ${fmt(grp.raw)} · Δ ${delta} · ${grp.n_leaves} theme${grp.n_leaves === 1 ? '' : 's'} · ${grp.n_members} stocks (${grp.n_screened} screened)</span>
            ${grp.n_tight ? `<span class="tight-badge" title="${grp.n_tight} of ${grp.n_members} members in a tight range">TIGHT ${grp.n_tight}</span>` : ''}
          </div>
          <div class="theme-body radar-body">
      `;
      // Each leaf is a two-line block, not a table row: the metadata line
      // carries rank/name/N/scores, and the chips below get the panel's full
      // width so every ticker is reachable in a narrow left panel (a 5-column
      // table pushed Raw/Boosted off-screen and clipped the ticker list).
      const leaves = radarTightSort
        ? (grp.leaves || []).slice().sort(byTight)
        : (grp.leaves || []);
      leaves.forEach(leaf => {
        const label = leaf.l2 ? (leaf.l3 ? `${leaf.l2} / ${leaf.l3}` : leaf.l2) : leaf.name;
        const chips = (leaf.tickers || []).map(t => {
          const cls = ['tn-link', 'radar-chip', t.is_screened ? 'chip-screened' : 'chip-quiet'];
          if (t.ticker_color === 'green') cls.push('day-pattern-green');
          if (t.tight) cls.push('tight-range');
          // Only the three lower rungs come from the tier field. The tight rung
          // arrives on the line above, from the radar's own boolean, and the
          // server returns 'tight' exactly when that boolean is true — so a
          // chip never carries two tints.
          const tierCls = hlChipClass(t.highlight);
          if (tierCls) cls.push(tierCls);
          const tightTip = t.tight ? ` · ${tightSummary(t)}` : '';
          // A tight chip already names its rung, and names the window with it.
          const tierTip = t.tight ? '' : (HL_TIER_TIP[t.highlight] || '');
          const tip = `RS ${t.rs ?? '—'} · VARS ${t.vars ?? '—'} · $${t.price ?? '—'}${tightTip}${tierTip ? ' · ' + tierTip : ''}`;
          return `<span class="${cls.join(' ')}"${filterAttrs(t)} data-sym="${escAttr(t.ticker)}" data-nm="${escAttr(grp.name + ' · ' + t.ticker)}" title="${escAttr(tip)}">${escHtml(t.ticker)}</span>`;
        }).join('');
        html += `
            <div class="radar-leaf">
              <div class="radar-leaf-hdr">
                <span class="radar-rank">#${leaf.global_rank}</span>
                <span class="radar-leaf-name">${escHtml(label)}</span>
                <span class="radar-n">N=${leaf.n}</span>
                ${leaf.n_tight ? `<span class="tight-badge" title="${leaf.n_tight} of ${leaf.n} members in a tight range">◉ ${leaf.n_tight}</span>` : ''}
                <span class="radar-scores" title="raw → boosted">${fmt(leaf.raw)}<span class="radar-arrow">→</span><span class="radar-boosted">${fmt(leaf.boosted)}</span></span>
              </div>
              <div class="radar-stocks-wrap">
                <div class="radar-stocks">${chips}</div>
                <button type="button" class="radar-more" hidden></button>
              </div>
            </div>
        `;
      });
      html += `
          </div>
        </div>
      `;
    });

    container.innerHTML = html;
    const tightBtn = container.querySelector('#tight-sort-btn');
    if (tightBtn) tightBtn.addEventListener('click', toggleTightSort);
    container.querySelectorAll('.tight-pin').forEach(pin => {
      pin.addEventListener('click', () => {
        const block = container.querySelector(
          `.theme-block[data-l1="${CSS.escape(pin.dataset.l1)}"]`);
        if (!block) return;
        // Instant, not smooth. The jump is routinely thousands of pixels —
        // measured 8,441px to reach a mid-board theme — and a smooth scroll
        // over that distance animates through ~30 blocks, is interruptible,
        // and did not reliably finish. `block: 'start'` puts the target at the
        // top of the list pane, which is the scroller; the strip sits outside
        // it and stays put.
        block.scrollIntoView({ behavior: 'auto', block: 'start' });
        block.classList.add('tight-jump');
        setTimeout(() => block.classList.remove('tight-jump'), 1200);
      });
    });
    // Order matters: the cutoffs re-dim the fresh DOM, then the clamp measures
    // the chip rows it produced. Re-ordering changes which chips sit in which
    // row, so the "+N more" count has to be recomputed, not carried over.
    applyTickerFilters();
    observeRadarClamps(container);
    syncRadarClamps(container);
  }

  // ── RADAR CHIP CLAMPING ───────────────────────────────
  // Collapsed chip rows are capped by CSS (`.radar-stocks` max-height); the
  // "+N" toggle only appears when chips actually overflow that cap, which
  // depends on the live panel width — so it is measured, never guessed.
  function syncRadarClamps(container) {
    container.querySelectorAll('.radar-stocks-wrap').forEach(wrap => {
      const box = wrap.querySelector('.radar-stocks');
      const btn = wrap.querySelector('.radar-more');
      if (!box || !btn) return;
      // Tab is hidden (display:none) — nothing measurable yet; the tab-switch
      // hook re-runs this once the panel has a box.
      if (!box.clientHeight) return;
      const expanded = wrap.classList.contains('expanded');
      if (!expanded) {
        const limit = box.clientHeight + 1;
        const hidden = Array.prototype.filter.call(
          box.querySelectorAll('.radar-chip'),
          c => c.offsetTop + c.offsetHeight > limit,
        ).length;
        btn.dataset.hidden = String(hidden);
      }
      const n = Number(btn.dataset.hidden || 0);
      const shouldHide = n === 0;
      if (btn.hidden !== shouldHide) btn.hidden = shouldHide;
      const label = expanded ? '− less' : `+${n} more`;
      if (btn.textContent !== label) btn.textContent = label;
    });
  }

  function syncRadarClampsNow() {
    const container = document.getElementById('themes-container');
    if (container) syncRadarClamps(container);
  }

  function observeRadarClamps(container) {
    if (container._radarObs || typeof ResizeObserver === 'undefined') return;
    // Catch-all for reflows the explicit hooks miss — most importantly a late
    // web-font load, which changes chip widths and therefore the hidden count.
    // The writes below are no-ops when nothing changed, so this converges
    // instead of looping.
    container._radarObs = new ResizeObserver(() => syncRadarClamps(container));
    container._radarObs.observe(container);
  }

  function initRadarClamps() {
    document.addEventListener('click', (e) => {
      const btn = e.target.closest('.radar-more');
      if (!btn) return;
      const wrap = btn.closest('.radar-stocks-wrap');
      if (!wrap) return;
      const expanded = wrap.classList.toggle('expanded');
      btn.textContent = expanded ? '− less' : `+${btn.dataset.hidden || 0} more`;
    });
    // The ResizeObserver only delivers inside the rendering lifecycle, so the
    // measurement never depends on it alone: the tab-shown and panel-width
    // paths call sync directly.
    window.addEventListener('resize', syncRadarClampsNow);
    window.addEventListener('resize', redrawNasiNow);
  }

  function renderMomentum(data, date) {
    const container = document.getElementById('momentum-container');
    if (!container) return;

    if (!data || !data.themes || data.themes.length === 0) {
      const msg = (date && !data) ? `No momentum data for ${date}.` : 'No momentum stocks found for this date.';
      container.innerHTML = `<div class="no-data">${msg}</div>`;
      return;
    }

    let html = '';
    data.themes.forEach((theme, idx) => {
      const count = (theme.tickers || []).length;
      html += `
        <div class="theme-block">
          <div class="theme-header">
            <span class="theme-rank">#${idx + 1}</span>
            <span class="theme-name">${escHtml(theme.name)}</span>
            <span class="theme-score">${count} ticker${count === 1 ? '' : 's'}</span>
          </div>
          <div class="theme-body">
            <table>
              <thead><tr>
                <th class="l">Ticker</th>
                <th>RS%</th>
                <th>Float(M)</th>
                <th>EPS%</th>
                <th>Sales%</th>
                <th>Inst%</th>
                <th>Short%</th>
              </tr></thead>
              <tbody>
      `;

      (theme.tickers || []).forEach(t => {
        const rsClass = pickBand(COLOR_BANDS.rs, t.rs);
        const instVal = parseFloat(String(t.inst).replace(/[+%]/g, ''));
        const instClass = pickBand(COLOR_BANDS.inst, instVal);
        const shortVal = parseFloat(t.short);
        const shortClass = pickBand(COLOR_BANDS.short, shortVal);
        html += `
                <tr${filterAttrs(t)}>
                  <td class="l">
                    <span class="tn-link${t.ticker_color === 'green' ? ' day-pattern-green' : ''}${hlClass(t.highlight)}"${hlTitle(t.highlight)} data-sym="${escAttr(t.ticker)}" data-nm="${escAttr(theme.name + ' · ' + t.ticker)}">
                      ${escHtml(t.ticker)}
                    </span>
                  </td>
                  <td class="${rsClass}">${t.rs ?? '—'}</td>
                  <td>${t.float ?? '—'}</td>
                  <td class="${pctClass(t.eps)}">${t.eps ?? '—'}</td>
                  <td class="${pctClass(t.sales)}">${t.sales ?? '—'}</td>
                  <td class="${instClass}">${t.inst ?? '—'}</td>
                  <td class="${shortClass}">${t.short ?? '—'}</td>
                </tr>
        `;
      });

      html += `
              </tbody>
            </table>
          </div>
        </div>
      `;
    });

    container.innerHTML = html;
    applyTickerFilters();
  }

  function renderVolume(data, date) {
    const container = document.getElementById('volume-container');
    if (!container) return;

    if (!data || !data.themes || data.themes.length === 0) {
      const msg = (date && !data) ? `No volume data for ${date}.` : 'No volume leaders found for this date.';
      container.innerHTML = `<div class="no-data">${msg}</div>`;
      return;
    }

    let html = '';
    data.themes.forEach((theme, idx) => {
      const tickers = theme.tickers || [];
      const count = tickers.length;
      const avgVars = (typeof theme.avg_vars === 'number')
        ? theme.avg_vars
        : (count ? tickers.reduce((s, t) => s + (t.vars ?? 0), 0) / count : 0);
      html += `
        <div class="theme-block">
          <div class="theme-header">
            <span class="theme-rank">#${idx + 1}</span>
            <span class="theme-name">${escHtml(theme.name)}</span>
            <span class="theme-score">avg VARS ${avgVars.toFixed(2)} · ${count} ticker${count === 1 ? '' : 's'}</span>
          </div>
          <div class="theme-body">
            <table>
              <thead><tr>
                <th class="l">Ticker</th>
                <th>Scan</th>
                <th>VARS</th>
                <th>RS%</th>
                <th>Float(M)</th>
                <th>EPS%</th>
                <th>Sales%</th>
                <th>Inst%</th>
                <th>Short%</th>
              </tr></thead>
              <tbody>
      `;

      tickers.forEach(t => {
        const varsClass = pickBand(COLOR_BANDS.vars, t.vars);
        const rsClass = pickBand(COLOR_BANDS.rs, t.rs);
        const instVal = parseFloat(String(t.inst).replace(/[+%]/g, ''));
        const instClass = pickBand(COLOR_BANDS.inst, instVal);
        const shortVal = parseFloat(t.short);
        const shortClass = pickBand(COLOR_BANDS.short, shortVal);
        const scan = t.scan || '';
        const days = (typeof t.days_since_hv === 'number') ? `${t.days_since_hv}d` : '';
        const scanLabel = scan ? (days ? `${scan} · ${days}` : scan) : '—';
        html += `
                <tr${filterAttrs(t)}>
                  <td class="l">
                    <span class="tn-link${t.ticker_color === 'green' ? ' day-pattern-green' : ''}${hlClass(t.highlight)}"${hlTitle(t.highlight)} data-sym="${escAttr(t.ticker)}" data-nm="${escAttr(theme.name + ' · ' + t.ticker)}">
                      ${escHtml(t.ticker)}
                    </span>
                  </td>
                  <td class="scan-cell">${escHtml(scanLabel)}</td>
                  <td class="${varsClass}">${(t.vars ?? 0).toFixed(2)}</td>
                  <td class="${rsClass}">${t.rs ?? '—'}</td>
                  <td>${t.float ?? '—'}</td>
                  <td class="${pctClass(t.eps)}">${t.eps ?? '—'}</td>
                  <td class="${pctClass(t.sales)}">${t.sales ?? '—'}</td>
                  <td class="${instClass}">${t.inst ?? '—'}</td>
                  <td class="${shortClass}">${t.short ?? '—'}</td>
                </tr>
        `;
      });

      html += `
              </tbody>
            </table>
          </div>
        </div>
      `;
    });

    container.innerHTML = html;
    applyTickerFilters();
  }

  function renderVARS(data, date) {
    const container = document.getElementById('vars-container');
    if (!container) return;

    if (!data || !data.themes || data.themes.length === 0) {
      const msg = (date && !data) ? `No VARS data for ${date}.` : 'No VARS leaders found for this date.';
      container.innerHTML = `<div class="no-data">${msg}</div>`;
      return;
    }

    let html = '';
    data.themes.forEach((entry, idx) => {
      // L1 section (nested shape); a legacy flat snapshot entry renders
      // as a single-leaf section so pre-clustering history still displays.
      const grp = entry.leaves ? entry : {
        name: entry.name,
        score: entry.avg_vars,
        avg_rs: null,
        n: (entry.tickers || []).length,
        hot: false,
        leaves: [entry],
      };
      const meta = [
        `${entry.leaves ? 'top-5 VARS' : 'avg VARS'} ${(grp.score ?? 0).toFixed(2)}`,
        (typeof grp.avg_rs === 'number') ? `avg RS ${grp.avg_rs.toFixed(1)}%` : '',
        `${grp.n} ticker${grp.n === 1 ? '' : 's'}`,
      ].filter(Boolean).join(' · ');
      html += `
        <div class="theme-block">
          <div class="theme-header${grp.hot ? ' l1-hot' : ''}">
            <span class="theme-rank">#${idx + 1}</span>
            <span class="theme-name">${escHtml(grp.name)}${grp.hot ? '<span class="hot-badge">HOT</span>' : ''}</span>
            <span class="theme-score">${meta}</span>
          </div>
          <div class="theme-body">
      `;

      const showLeafHdr = !(grp.leaves.length === 1 && grp.leaves[0].name === grp.name);
      grp.leaves.forEach(leaf => {
        const tickers = leaf.tickers || [];
        if (showLeafHdr) {
          const sub = leaf.name.startsWith(grp.name + ' / ')
            ? leaf.name.slice(grp.name.length + 3)
            : leaf.name;
          html += `
            <div class="leaf-subheader">
              <span class="leaf-name">${escHtml(sub)}</span>
              <span class="leaf-meta">avg VARS ${(leaf.avg_vars ?? 0).toFixed(2)} · ${tickers.length} ticker${tickers.length === 1 ? '' : 's'}</span>
            </div>
          `;
        }
        html += `
            <table>
              <thead><tr>
                <th class="l">Ticker</th>
                <th>VARS</th>
                <th>RS%</th>
                <th>Float(M)</th>
                <th>EPS%</th>
                <th>Sales%</th>
                <th>Inst%</th>
                <th>Short%</th>
              </tr></thead>
              <tbody>
        `;

        tickers.forEach(t => {
          const varsClass = pickBand(COLOR_BANDS.vars, t.vars);
          const rsClass = pickBand(COLOR_BANDS.rs, t.rs);
          const instVal = parseFloat(String(t.inst).replace(/[+%]/g, ''));
          const instClass = pickBand(COLOR_BANDS.inst, instVal);
          const shortVal = parseFloat(t.short);
          const shortClass = pickBand(COLOR_BANDS.short, shortVal);
          const accel = (typeof t.vars_20ema === 'number')
            ? (t.vars > t.vars_20ema
              ? '<span class="accel accel-up">▲</span>'
              : '<span class="accel accel-dn">▼</span>')
            : '';
          html += `
                <tr${filterAttrs(t)}>
                  <td class="l">
                    <span class="tn-link${t.ticker_color === 'green' ? ' day-pattern-green' : ''}${hlClass(t.highlight)}"${hlTitle(t.highlight)} data-sym="${escAttr(t.ticker)}" data-nm="${escAttr(leaf.name + ' · ' + t.ticker)}">
                      ${escHtml(t.ticker)}
                    </span>
                  </td>
                  <td class="${varsClass}">${(t.vars ?? 0).toFixed(2)}${accel}</td>
                  <td class="${rsClass}">${t.rs ?? '—'}</td>
                  <td>${t.float ?? '—'}</td>
                  <td class="${pctClass(t.eps)}">${t.eps ?? '—'}</td>
                  <td class="${pctClass(t.sales)}">${t.sales ?? '—'}</td>
                  <td class="${instClass}">${t.inst ?? '—'}</td>
                  <td class="${shortClass}">${t.short ?? '—'}</td>
                </tr>
          `;
        });

        html += `
              </tbody>
            </table>
        `;
      });

      html += `
          </div>
        </div>
      `;
    });

    container.innerHTML = html;
    applyTickerFilters();
  }

  /**
   * Render the SI tab: L1 sections, each holding its leaf tables.
   *
   * Shaped like renderVARS on purpose, with two deliberate differences.
   * Rows carry no filterAttrs and nothing calls applyTickerFilters — this tab
   * has no V/A cutoffs, because Finviz already gates price, volume and
   * volatility upstream. And the section meta reports short interest, which
   * is what ranks the sections; the HOT badge is decoration from the radar
   * and never reorders anything.
   */
  function renderSI(data, date) {
    const container = document.getElementById('si-container');
    if (!container) return;

    if (!data || !data.themes || data.themes.length === 0) {
      const msg = (date && !data)
        ? `No SI data for ${date}.`
        : 'No heavily shorted names in a drawdown for this date.';
      container.innerHTML = `<div class="no-data">${msg}</div>`;
      return;
    }

    let html = '';

    // The roster's age, shown only when it is not this session's. Step 7b is
    // non-critical and short_interest.json is committed, so a failed Finviz
    // fetch leaves the PREVIOUS roster joined to today's prices. Undated reads
    // as stale. Without this the tab is indistinguishable from a fresh one --
    // the frozen-NAAIM-tile failure in a new costume.
    if (data.si_stale) {
      html += `<div class="si-stale-note">Short interest dated `
        + `${escHtml(data.si_date || 'unknown')}, not this session `
        + `(${escHtml(data.report_date || 'unknown')}). Prices below are current.</div>`;
    }

    data.themes.forEach((grp, idx) => {
      const meta = [
        `top-3 SI ${(grp.score ?? 0).toFixed(2)}%`,
        `${grp.n} ticker${grp.n === 1 ? '' : 's'}`,
        (typeof grp.radar_rank === 'number') ? `radar #${grp.radar_rank}` : '',
      ].filter(Boolean).join(' · ');
      html += `
        <div class="theme-block">
          <div class="theme-header${grp.hot ? ' l1-hot' : ''}">
            <span class="theme-rank">#${idx + 1}</span>
            <span class="theme-name">${escHtml(grp.name)}${grp.hot ? '<span class="hot-badge">HOT</span>' : ''}</span>
            <span class="theme-score">${meta}</span>
          </div>
          <div class="theme-body">
      `;

      (grp.leaves || []).forEach(leaf => {
        const tickers = leaf.tickers || [];
        const sub = leaf.name.startsWith(grp.name + ' / ')
          ? leaf.name.slice(grp.name.length + 3)
          : leaf.name;
        html += `
            <div class="leaf-subheader">
              <span class="leaf-name">${escHtml(sub)}</span>
              <span class="leaf-meta">top-3 SI ${(leaf.score ?? 0).toFixed(2)}% · ${tickers.length} ticker${tickers.length === 1 ? '' : 's'}</span>
            </div>
            <table>
              <thead><tr>
                <th class="l">Ticker</th>
                <th>SI%</th>
                <th>DD60%</th>
                <th>Drop15%</th>
                <th>Streak</th>
                <th>Price</th>
                <th>Float(M)</th>
                <th>Inst%</th>
              </tr></thead>
              <tbody>
        `;

        tickers.forEach(t => {
          // Matches the VARS tab's Short% banding so one number reads the
          // same on both tabs.
          const siClass = pickBand(COLOR_BANDS.short, t.si);
          const instVal = parseFloat(String(t.inst).replace(/[+%]/g, ''));
          const instClass = pickBand(COLOR_BANDS.inst, instVal);
          const fmtPct = (v) => (typeof v === 'number') ? v.toFixed(1) : '—';
          html += `
                <tr>
                  <td class="l">
                    <span class="tn-link${t.ticker_color === 'green' ? ' day-pattern-green' : ''}${hlClass(t.highlight)}"${hlTitle(t.highlight)} data-sym="${escAttr(t.ticker)}" data-nm="${escAttr(leaf.name + ' · ' + t.ticker)}">
                      ${escHtml(t.ticker)}
                    </span>
                  </td>
                  <td class="${siClass}"><strong>${(t.si ?? 0).toFixed(2)}</strong></td>
                  <td class="dn">${fmtPct(t.dd60)}</td>
                  <td class="dn">${fmtPct(t.drop15)}</td>
                  <td>${t.streak ?? '—'}</td>
                  <td>${(typeof t.price === 'number') ? t.price.toFixed(2) : '—'}</td>
                  <td>${t.float ?? '—'}</td>
                  <td class="${instClass}">${t.inst ?? '—'}</td>
                </tr>
          `;
        });

        html += `
              </tbody>
            </table>
        `;
      });

      html += `
          </div>
        </div>
      `;
    });

    container.innerHTML = html;
  }

  // ── PARABOLIC DATA ────────────────────────────────────
  function sortAndRenderParabolic() {
    const s = sortState.parabolic;
    parabolicData.sort((a, b) => {
      const av = parseFloat(a[s.column]);
      const bv = parseFloat(b[s.column]);
      const aVal = Number.isNaN(av) ? (s.dir === 'asc' ? Infinity : -Infinity) : av;
      const bVal = Number.isNaN(bv) ? (s.dir === 'asc' ? Infinity : -Infinity) : bv;
      return s.dir === 'desc' ? bVal - aVal : aVal - bVal;
    });
    renderParabolicTable();
  }

  function renderParabolicTable() {
    const tbody = document.getElementById('parabolic-body');
    const countEl = document.getElementById('parabolic-count');
    if (!tbody) return;

    if (countEl) {
      countEl.textContent = `${parabolicData.length} ticker${parabolicData.length === 1 ? '' : 's'}`;
    }

    if (!parabolicData.length) {
      tbody.innerHTML = `<tr><td colspan="5" class="no-data">${parabolicEmptyMessage}</td></tr>`;
      return;
    }

    let html = '';
    parabolicData.forEach(row => {
      const instVal = parseFloat(String(row.inst).replace(/[+%]/g, ''));
      const instClass = pickBand(COLOR_BANDS.inst, instVal);
      const shortVal = parseFloat(row.short);
      const shortClass = pickBand(COLOR_BANDS.short, shortVal);
      const atrVal = parseFloat(row.atr_multi_50sma);
      const atrClass = pickBand(COLOR_BANDS.parabolicAtr, atrVal);
      const atrStr = isNaN(atrVal) ? '—' : atrVal.toFixed(1) + 'x';

      html += `
        <tr${filterAttrs(row)}>
          <td class="l">
            <span class="tn-link${row.ticker_color === 'green' ? ' day-pattern-green' : ''}${hlClass(row.highlight)}"${hlTitle(row.highlight)} data-sym="${escAttr(row.ticker)}" data-nm="${escAttr(row.ticker + ' · Parabolic')}">${escHtml(row.ticker)}</span>
          </td>
          <td>${row.float ?? '—'}</td>
          <td class="${instClass}">${row.inst ?? '—'}</td>
          <td class="${shortClass}">${row.short ?? '—'}</td>
          <td class="${atrClass}"><strong>${atrStr}</strong></td>
        </tr>
      `;
    });

    tbody.innerHTML = html;
    applyTickerFilters();
  }

  // ── INDUSTRY ETF DATA ─────────────────────────────────
  function loadIndustryETFData() {
    Promise.all([
      fetch(withCacheBust(INDUSTRY_ETF_URL)).then(r => { if (!r.ok) throw new Error(); return r.json(); }),
      fetch(withCacheBust(INDUSTRY_ETF_HISTORY_URL)).then(r => r.json()).catch(() => []),
    ])
      .then(([data, history]) => {
        industryData = data;

        // Build history, merge current using activeSessionDate
        const byDate = {};
        (history || []).forEach(h => { byDate[h.report_date] = h; });
        if (activeSessionDate && data) {
          byDate[activeSessionDate] = { report_date: activeSessionDate, data };
        }
        industryHistory = Object.values(byDate)
          .sort((a, b) => b.report_date.localeCompare(a.report_date));

        renderAllTimeTravelBars();
        sortAndRenderIndustry();
      })
      .catch(() => {
        fetchCSV(INDUSTRY_SHEET_URL)
          .then(rows => {
            industryData = parseIndustryRows(rows);
            sortAndRenderIndustry();
          })
          .catch(() => {
            document.getElementById('industry-body').innerHTML =
              '<tr><td colspan="6" class="no-data">Industry ETF data not available.</td></tr>';
          });
      });
  }

  function parseIndustryRows(rows) {
    const result = [];
    const seen = new Set();
    let currentSection = '';
    let inIndustrySection = false;

    rows.forEach(r => {
      if (r['1 Month RS'] === '1 Month RS') {
        currentSection = r['Index'] || r['Segment'] || r['EW Sector'] || r['SPDR Sector'] || r['Industry'] || '';
        inIndustrySection = currentSection === 'Industry';
        return;
      }

      // Only include Industry section rows
      if (!inIndustrySection) return;

      const ticker = (r['Index'] || r['Segment'] || r['EW Sector'] || r['SPDR Sector'] || r['Industry'] || '').trim();
      if (!ticker || seen.has(ticker)) return;
      seen.add(ticker);

      const clean = ticker.includes(':') ? ticker.split(':')[1] : ticker;

      result.push({
        ticker: clean,
        display_ticker: ticker,
        name: (r['Name'] || '').trim(),
        rs_sts: parsePercent(r['RS_STS%']),
        intraday: parsePercent(r['Intraday %']),
        daily: parsePercent(r['Daily %']),
        monthly: parsePercent(r['Monthly %']),
        lev_long: (r['Leveraged Long'] || '').trim(),
        lev_short: (r['Leveraged Short'] || '').trim(),
        vars: null,
      });
    });
    return result;
  }

  function sortAndRenderIndustry() {
    const s = sortState.industry;
    industryData.sort((a, b) => {
      const av = a[s.column] ?? -Infinity;
      const bv = b[s.column] ?? -Infinity;
      return s.dir === 'desc' ? bv - av : av - bv;
    });
    renderIndustryTable();
  }

  function renderIndustryTable() {
    const tbody = document.getElementById('industry-body');
    if (!industryData.length) {
      tbody.innerHTML = `<tr><td colspan="6" class="no-data">${industryEmptyMessage}</td></tr>`;
      return;
    }

    let html = '';
    industryData.forEach(row => {
      const varsClass = pickBand(COLOR_BANDS.vars, row.vars);
      const varsText = row.vars == null ? '—' : row.vars.toFixed(2);
      html += `
        <tr>
          <td class="l">
            <span class="tn-link${row.ticker_color === 'green' ? ' day-pattern-green' : ''}${hlClass(row.highlight)}"${hlTitle(row.highlight)} data-sym="${escAttr(row.display_ticker || row.ticker)}" data-nm="${escAttr(row.name)}">${escHtml(row.ticker)}</span>
          </td>
          <td class="l" style="font-size:11px;color:var(--text2);max-width:220px;overflow:hidden;text-overflow:ellipsis">${escHtml(truncate(row.name, 40))}</td>
          <td class="${varsClass}"><strong>${varsText}</strong></td>
          <td class="${pctClass(row.intraday)}">${fmtPct(row.intraday)}</td>
          <td class="${pctClass(row.daily)}">${fmtPct(row.daily)}</td>
          <td class="${pctClass(row.monthly)}">${fmtPct(row.monthly)}</td>
        </tr>
      `;
    });
    tbody.innerHTML = html;
  }

  // ── LEVERAGE ETF DATA ─────────────────────────────────
  function loadETFData() {
    Promise.all([
      fetch(withCacheBust(ETF_FALLBACK_URL)).then(r => { if (!r.ok) throw new Error(); return r.json(); }),
      fetch(withCacheBust(ETF_DATA_HISTORY_URL)).then(r => r.json()).catch(() => []),
    ])
      .then(([data, history]) => {
        etfData = data;

        // Build history, merge current using activeSessionDate
        const byDate = {};
        (history || []).forEach(h => { byDate[h.report_date] = h; });
        if (activeSessionDate && data) {
          byDate[activeSessionDate] = { report_date: activeSessionDate, data };
        }
        etfHistory = Object.values(byDate)
          .sort((a, b) => b.report_date.localeCompare(a.report_date));

        renderAllTimeTravelBars();
        sortAndRenderETF();
      })
      .catch(() => {
        console.warn('JSON fetch failed, trying CSV fallback');
        fetchCSV(ETF_SHEET_URL)
          .then(rows => {
            etfData = parseETFRows(rows);
            sortAndRenderETF();
          })
          .catch(() => {
            document.getElementById('etf-body').innerHTML =
              '<tr><td colspan="6" class="no-data">ETF data not available.</td></tr>';
          });
      });
  }

  function fetchCSV(url) {
    return fetch(url)
      .then(r => {
        if (!r.ok) throw new Error('HTTP ' + r.status);
        return r.text();
      })
      .then(txt => {
        const lines = txt.trim().split('\n');
        const headers = lines[0].split(',').map(h => h.trim());
        return lines.slice(1).map(line => {
          const vals = line.split(',').map(v => v.trim());
          const obj = {};
          headers.forEach((h, i) => obj[h] = vals[i] || '');
          return obj;
        });
      });
  }

  function parseETFRows(rows) {
    return rows
      .filter(r => r.Ticker && r.Ticker.length > 0)
      .map(r => ({
        ticker: r.Ticker,
        name: r.Name || '',
        rs: r['Relative Strength'] || '',
        rs_sts: parsePercent(r['RS_STS%']),
        intraday: parsePercent(r['Intraday %']),
        daily: parsePercent(r['Daily %']),
        monthly: parsePercent(r['Monthly %']),
        vars: null
      }))
      .filter((item, idx, arr) => arr.findIndex(x => x.ticker === item.ticker) === idx);
  }

  function parsePercent(val) {
    if (!val) return null;
    return parseFloat(val.replace('%', ''));
  }

  function sortAndRenderETF() {
    const s = sortState.etf;
    etfData.sort((a, b) => {
      const av = a[s.column] ?? -Infinity;
      const bv = b[s.column] ?? -Infinity;
      return s.dir === 'desc' ? bv - av : av - bv;
    });
    renderETFTable();
  }

  function renderETFTable() {
    const tbody = document.getElementById('etf-body');
    if (!etfData.length) {
      tbody.innerHTML = `<tr><td colspan="6" class="no-data">${etfEmptyMessage}</td></tr>`;
      return;
    }

    let html = '';
    etfData.forEach(row => {
      const varsClass = pickBand(COLOR_BANDS.vars, row.vars);
      const varsText = row.vars == null ? '—' : row.vars.toFixed(2);
      html += `
        <tr>
          <td class="l">
            <span class="tn-link${row.ticker_color === 'green' ? ' day-pattern-green' : ''}${hlClass(row.highlight)}"${hlTitle(row.highlight)} data-sym="${escAttr(row.ticker)}" data-nm="${escAttr(row.name)}">${escHtml(row.ticker)}</span>
          </td>
          <td class="l" style="font-size:11px;color:var(--text2);max-width:220px;overflow:hidden;text-overflow:ellipsis">${escHtml(truncate(row.name, 40))}</td>
          <td class="${varsClass}"><strong>${varsText}</strong></td>
          <td class="${pctClass(row.intraday)}">${fmtPct(row.intraday)}</td>
          <td class="${pctClass(row.daily)}">${fmtPct(row.daily)}</td>
          <td class="${pctClass(row.monthly)}">${fmtPct(row.monthly)}</td>
        </tr>
      `;
    });
    tbody.innerHTML = html;
  }

  // ── TABLE SORT (generic for ETF, Industry, and Parabolic) ────
  function initTableSort() {
    document.querySelectorAll('th.sortable').forEach(th => {
      th.addEventListener('click', () => {
        const col = th.dataset.sort;
        const tab = th.dataset.tab || 'etf';
        const state = sortState[tab];

        if (state.column === col) {
          state.dir = state.dir === 'desc' ? 'asc' : 'desc';
        } else {
          state.column = col;
          state.dir = 'desc';
        }

        const table = th.closest('table');
        table.querySelectorAll('th.sortable').forEach(h => {
          h.classList.remove('sorted-desc', 'sorted-asc');
        });
        th.classList.add(state.dir === 'desc' ? 'sorted-desc' : 'sorted-asc');

        if (tab === 'industry') sortAndRenderIndustry();
        else if (tab === 'parabolic') sortAndRenderParabolic();
        else sortAndRenderETF();
      });
    });
  }

  // ── STATUS UPDATE ─────────────────────────────────────
  function updateStatus(msg) {
    const el = document.getElementById('dataStatus');
    el.textContent = msg;
    el.style.color = 'var(--green)';
    el.style.borderColor = 'rgba(0,230,118,0.3)';
  }

  // ── UTILITIES ─────────────────────────────────────────
  function escHtml(str) {
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
  }

  function escAttr(str) {
    return String(str).replace(/"/g, '&quot;').replace(/'/g, '&#39;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }

  function pctClass(val) {
    return pickBand(COLOR_BANDS.pct, val);
  }

  function fmtPct(val) {
    if (val == null) return '—';
    const n = parseFloat(val);
    if (isNaN(n)) return '—';
    return (n > 0 ? '+' : '') + n.toFixed(1) + '%';
  }

  function formatNum(val) {
    if (val == null) return '—';
    const n = parseFloat(val);
    if (isNaN(n)) return '—';
    if (n >= 1000) return n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    return n.toFixed(2);
  }

  function truncate(str, len) {
    if (!str) return '';
    return str.length > len ? str.substring(0, len) + '…' : str;
  }

})();
