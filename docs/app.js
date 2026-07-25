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
  const PARABOLIC_DATA_URL = 'data/parabolic.json';
  const PARABOLIC_HISTORY_URL = 'data/parabolic_history.json';
  const EP_AFTERNOON_URL = 'data/ep_scan_afternoon.json';
  const EP_AFTERNOON_HISTORY_URL = 'data/ep_scan_afternoon_history.json';
  const EP_MORNING_URL = 'data/ep_scan_morning.json';
  const EP_MORNING_HISTORY_URL = 'data/ep_scan_morning_history.json';
  const INDUSTRY_ETF_HISTORY_URL = 'data/industry_etf_history.json';
  const ETF_DATA_HISTORY_URL = 'data/etf_data_history.json';
  const BREADTH_DATA_URL = 'data/market_breadth.json';
  const MACRO_DATA_URL = 'data/macro_data.json';
  const INDUSTRY_ETF_URL = 'data/industry_etf.json';
  const META_URL = 'data/report_meta.json';
  const EVENTS_URL = 'data/events.json';
  const ETF_SHEET_URL = 'https://docs.google.com/spreadsheets/d/1zwmK5YnbBHyin0n0DHIEEydPapCkln1WCvlKv4IhwSg/export?format=csv&gid=1565194920';
  const ETF_FALLBACK_URL = 'data/etf_data.json';
  const INDUSTRY_SHEET_URL = 'https://docs.google.com/spreadsheets/d/1zwmK5YnbBHyin0n0DHIEEydPapCkln1WCvlKv4IhwSg/export?format=csv&gid=549753148';
  const PAGE_LOAD_CACHE_KEY = Date.now();
  // Time-travel retention window in calendar days. The server-side exporters
  // already prune every *_history.json to this window; this client-side bound
  // mirrors it so EP history (which it caps directly) stays consistent.
  const SESSION_HISTORY_DAYS = 180;

  // Symbols that need a different symbol for TradingView widget vs data fetch.
  // NOTE: TVC/CAPITALCOM/CBOE treasury yield symbols are all restricted in the
  // embedded widget. FRED is the only embeddable source (line chart, no candlestick).
  const TV_CHART_SYM_MAP = {
    'CAPITALCOM:US2YR':  'FRED:DGS2',
    'CAPITALCOM:US10YR': 'FRED:DGS10',
    'CAPITALCOM:US30YR': 'FRED:DGS30',
  };

  // Active chart per tab
  let activeCharts = { macro: null, themes: null, momentum: null, momentumviz: null, volume: null, volumeviz: null, vars: null, varsviz: null, industry: null, etf: null, ep: null, parabolic: null };

  // Sort state per table
  let sortState = {
    etf: { column: 'vars', dir: 'desc' },
    industry: { column: 'vars', dir: 'desc' },
    ep_afternoon: { column: 'float', dir: 'asc' },
    ep_morning: { column: 'float', dir: 'asc' },
    parabolic: { column: 'atr_multi_50sma', dir: 'desc' },
  };
  let etfData = [];
  let industryData = [];
  let epAfternoonData = [];
  let epMorningData = [];
  let epAfternoonHistory = [];
  let epMorningHistory = [];
  let epAfternoonEmptyMessage = 'No afternoon EP results.';
  let epMorningEmptyMessage = 'No morning EP results.';
  let parabolicData = [];
  let volumeHistory = [];
  let parabolicEmptyMessage = 'No parabolic results for this date.';
  let industryEmptyMessage = 'No industry ETF data available.';
  let etfEmptyMessage = 'No ETF data available.';
  // Combined lookup for news by ticker
  let epAllTickers = {};

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
    initTickerFilters();
    initResizablePanels();
    initRadarClamps();
    initArrowKeyNav();
    loadMeta();
    loadMacroData();
    loadBreadthData();
    loadThemeData();
    loadMomentumData();
    loadVolumeData();
    loadVARSData();
    loadIndustryETFData();
    loadETFData();
    loadParabolicData();
    loadMacroEvents();
    loadEPAfternoonData();
    loadEPMorningData();
    initTableSort();
    initEPNewsClick();
    // Auto-refresh EP data every 10 minutes
    setInterval(loadEPAfternoonData, 10 * 60 * 1000);
    setInterval(loadEPMorningData, 10 * 60 * 1000);
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

      if (day === 0 || day === 6) {
        el.className = 'market-status closed';
        txt.textContent = 'MARKET CLOSED';
        return;
      }
      if (mins >= 240 && mins < 570) {
        el.className = 'market-status premarket';
        txt.textContent = 'PRE-MARKET';
      } else if (mins >= 570 && mins < 960) {
        el.className = 'market-status open';
        txt.textContent = 'US MARKET LIVE';
      } else if (mins >= 960 && mins < 1200) {
        el.className = 'market-status premarket';
        txt.textContent = 'AFTER HOURS';
      } else {
        el.className = 'market-status closed';
        txt.textContent = 'MARKET CLOSED';
      }
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
          const newWidth = Math.max(250, Math.min(startWidth + dx, window.innerWidth - 300));
          leftPanel.style.width = newWidth + 'px';
          syncRadarClampsNow();
        }
        function onUp() {
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

      let tabId;
      if (tabContent.id === 'content-macro') tabId = 'macro';
      else if (tabContent.id === 'content-themes') tabId = 'themes';
      else if (tabContent.id === 'content-momentum') tabId = 'momentum';
      else if (tabContent.id === 'content-volume') tabId = 'volume';
      else if (tabContent.id === 'content-volumeviz') tabId = 'volumeviz';
      else if (tabContent.id === 'content-vars') tabId = 'vars';
      else if (tabContent.id === 'content-industry') tabId = 'industry';
      else if (tabContent.id === 'content-etf') tabId = 'etf';
      else if (tabContent.id === 'content-ep') tabId = 'ep';
      else if (tabContent.id === 'content-parabolic') tabId = 'parabolic';
      else return;

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
        "studies": [
          { "id": "MAExp@tv-basicstudies", "inputs": { "length": 10 } },
          { "id": "MAExp@tv-basicstudies", "inputs": { "length": 20 } },
          { "id": "MASimple@tv-basicstudies", "inputs": { "length": 50 } },
          { "id": "MASimple@tv-basicstudies", "inputs": { "length": 200 } },
          { "id": "STD;Volume" }
        ],
        "studies_overrides": {
          "moving average exponential.ma.color": "#4CAF50",
          "moving average exponential.ma.linewidth": 1,
          "moving average exponential.ma.transparency": 20,
          "moving average.ma.color": "#FFD700",
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
  let navIndices = { macro: -1, themes: -1, momentum: -1, momentumviz: -1, volume: -1, volumeviz: -1, vars: -1, varsviz: -1, industry: -1, etf: -1, ep: -1, parabolic: -1 };

  function getActiveTabId() {
    const activeBtn = document.querySelector('.tab-btn.active');
    return activeBtn ? activeBtn.dataset.tab : 'macro';
  }

  function getTickerLinksForTab(tabId) {
    const container = document.getElementById('content-' + tabId);
    if (!container) return [];
    const leftPanel = container.querySelector('.left-panel');
    if (!leftPanel) return [];
    return Array.from(leftPanel.querySelectorAll('.tn-link'));
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

      let idx = navIndices[tabId];

      if (e.key === 'ArrowDown') {
        idx = (idx < links.length - 1) ? idx + 1 : idx;
      } else {
        idx = (idx > 0) ? idx - 1 : 0;
      }

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
                  tdPrice.classList.add(item.d1 > 0 ? 'pos' : item.d1 < 0 ? 'neg' : 'neu');
                }
                tr.appendChild(tdPrice);

                ['d1', 'w1', 'hi52w', 'ytd'].forEach(k => {
                  const val = item[k];
                  const td = document.createElement('td');
                  td.textContent = val != null ? (val > 0 ? '+' : '') + val.toFixed(1) + '%' : '—';
                  td.className = val != null ? (val > 0 ? 'pos' : val < 0 ? 'neg' : 'neu') : 'neu';
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
  function loadBreadthData() {
    fetch(withCacheBust(BREADTH_DATA_URL))
      .then(r => r.json())
      .then(data => {
        // CNN Fear & Greed
        if (data.fear_greed && data.fear_greed.score != null) {
          const el = document.getElementById('fg-value');
          el.textContent = data.fear_greed.score.toFixed(1);
          el.className = 'breadth-value ' + (data.fear_greed.score >= 50 ? 'up' : 'dn');
          if (data.fear_greed.rating) {
            document.getElementById('fg-rating').textContent = data.fear_greed.rating.toUpperCase();
          }
        }

        // NAAIM
        if (data.naaim && data.naaim.value != null) {
          const el = document.getElementById('naaim-value');
          const val = data.naaim.value;
          el.textContent = val.toFixed(2) + '%';
          el.className = 'breadth-value ' + (val < 40 ? 'up' : val > 95 ? 'dn' : 'neu');
        }

        // Render each breadth indicator with history as numbers
        ['ncfd', 'mmtw', 'mmfi', 'mmth'].forEach(key => {
          const val = data[key]?.current ?? data[key] ?? null;
          const hist = data[key]?.history ?? (val != null ? [val] : []);
          const valEl = document.getElementById(key + '-value');
          const histEl = document.getElementById(key + '-history');

          if (valEl && val != null) {
            valEl.textContent = val.toFixed(1) + '%';
            let colorClass = 'neu';
            if (key === 'ncfd') {
              if (val < 20) colorClass = 'up';
              else if (val > 85) colorClass = 'dn';
            } else if (key === 'mmfi') {
              if (val < 15.5) colorClass = 'up';
            } else if (key === 'mmth' || key === 'mmtw') {
              if (val < 20) colorClass = 'up';
            }
            valEl.className = 'breadth-value ' + colorClass;
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

  function renderBreadthHistory(key, el, history) {
    el.innerHTML = '';
    const last5 = history.slice(-5);
    last5.forEach((val, i) => {
      const span = document.createElement('span');
      span.className = 'breadth-hist-num';
      span.textContent = val.toFixed(1);
      let color = 'var(--text3)';
      if (key === 'ncfd') {
        if (val < 20) color = 'var(--green)';
        else if (val > 85) color = 'var(--red)';
      } else if (key === 'mmfi') {
        if (val < 15.5) color = 'var(--green)';
      } else if (key === 'mmth' || key === 'mmtw') {
        if (val < 20) color = 'var(--green)';
      }
      span.style.color = color;
      span.title = 'Session ' + (i + 1) + ': ' + val.toFixed(1) + '%';
      el.appendChild(span);
    });
  }

  // ── THEME DATA + TIME TRAVEL ──────────────────────────
  let themesHistory = [];    // Array of radar snapshots (Themes tab), newest first
  let momentumHistory = [];  // Array of momentum snapshots, newest first
  let varsHistory = [];      // Array of vars snapshots, newest first
  let parabolicHistory = []; // Array of parabolic snapshots, newest first
  let industryHistory = [];  // Array of {report_date, data} snapshots
  let etfHistory = [];       // Array of {report_date, data} snapshots
  let activeSessionDate = null;
  let hasUserSelectedSession = false;

  // ── TICKER FILTER TOGGLES (V / A) ─────────────────────
  // Shared across tabs like activeSessionDate: arming V on VARS leaves it armed
  // on Themes. Session-only — no persistence across reloads.
  const FILTER_MIN_AVG_VOL = 1e6;    // 50-day avg share volume (vol_sma50)
  const FILTER_MIN_ADR_PCT = 0.04;   // 20-day ADR, fractional (0.04 == 4%)
  const tickerFilters = { vol: false, adr: false };

  /**
   * Dim every rendered ticker that fails an armed floor.
   *
   * A view-level pass, not a re-render: renderers emit data-avgvol/data-adr and
   * this only toggles a class. That keeps scores, breadth counts, ranks, sort
   * order, and the radar's measured "+N more" chip count identical armed or
   * disarmed — nothing leaves the DOM and nothing changes size.
   *
   * Fail-open: a ticker with the attribute absent or unparseable passes. Missing
   * metrics are unknown, not disqualifying, and the published JSON carries none
   * of them until the next daily workflow run republishes it.
   */
  function applyTickerFilters() {
    const scope = document.querySelector('.tab-content.active') || document;
    scope.querySelectorAll('[data-avgvol], [data-adr]').forEach(el => {
      const belowFloor = (attr, floor) => {
        if (!tickerFilters[attr === 'avgvol' ? 'vol' : 'adr']) return false;
        const raw = el.dataset[attr === 'avgvol' ? 'avgvol' : 'adr'];
        if (raw === undefined || raw === '') return false;
        const value = parseFloat(raw);
        return Number.isFinite(value) && value < floor;
      };
      const dim = belowFloor('avgvol', FILTER_MIN_AVG_VOL)
        || belowFloor('adr', FILTER_MIN_ADR_PCT);
      el.classList.toggle('filtered-out', dim);
    });
  }

  function initTickerFilters() {
    document.addEventListener('click', (e) => {
      const btn = e.target.closest('.tt-filter-btn');
      if (!btn) return;
      const key = btn.dataset.filter;
      if (!(key in tickerFilters)) return;
      tickerFilters[key] = !tickerFilters[key];
      // State is shared, so every bar's copy of this button must agree.
      document.querySelectorAll(`.tt-filter-btn[data-filter="${key}"]`).forEach(b => {
        b.classList.toggle('on', tickerFilters[key]);
        b.setAttribute('aria-pressed', String(tickerFilters[key]));
      });
      applyTickerFilters();
    });
  }

  /** Emit the filter attributes for a ticker payload. Omitted when unknown. */
  function filterAttrs(t) {
    const parts = [];
    if (typeof t.avg_vol === 'number') parts.push(` data-avgvol="${t.avg_vol}"`);
    if (typeof t.adr_pct === 'number') parts.push(` data-adr="${t.adr_pct}"`);
    return parts.join('');
  }

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
        renderThemeNetwork(radarVizSnap(current));
      })
      .catch(err => {
        console.warn('Theme data not available:', err);
        document.getElementById('themes-container').innerHTML =
          '<div class="no-data">Theme data not available.<br>Run the daily workflow to generate data.</div>';
        const tnContainer = document.getElementById('theme-network');
        if (tnContainer) tnContainer.innerHTML = '<div class="no-data">Theme data not available.</div>';
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
        renderMomentumNetwork(current);
      })
      .catch(err => {
        console.warn('Momentum data not available:', err);
        document.getElementById('momentum-container').innerHTML =
          '<div class="no-data">Momentum data not available.<br>Run the daily workflow to generate data.</div>';
        const mnContainer = document.getElementById('momentum-network');
        if (mnContainer) mnContainer.innerHTML = '<div class="no-data">Momentum data not available.</div>';
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
        renderVolumeNetwork(current);
      })
      .catch(err => {
        console.warn('Volume data not available:', err);
        const c = document.getElementById('volume-container');
        if (c) c.innerHTML = '<div class="no-data">Volume data not available.<br>Run the daily workflow to generate data.</div>';
        const cn = document.getElementById('volume-network');
        if (cn) cn.innerHTML = '<div class="no-data">Volume data not available.</div>';
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
        renderVARSNetwork(current);
      })
      .catch(err => {
        console.warn('VARS data not available:', err);
        const c = document.getElementById('vars-container');
        if (c) c.innerHTML = '<div class="no-data">VARS data not available.<br>Run the daily workflow to generate data.</div>';
        const vn = document.getElementById('vars-network');
        if (vn) vn.innerHTML = '<div class="no-data">VARS data not available.</div>';
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
   *  wrote first (e.g. the 5:45 AM EP scan writes today's entry hours before
   *  the 1:30 PM pipeline produces the other tabs' data). */
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
    renderThemeNetwork(radarVizSnap(themeSnap), date);
    // Momentum 1/3/6
    const momSnap = momentumHistory.find(h => h.report_date === date);
    renderMomentum(momSnap, date);
    renderMomentumNetwork(momSnap, date);
    // Volume scans
    const volumeSnap = volumeHistory.find(h => h.report_date === date);
    renderVolume(volumeSnap, date);
    renderVolumeNetwork(volumeSnap, date);
    // VARS
    const varsSnap = varsHistory.find(h => h.report_date === date);
    renderVARS(varsSnap, date);
    renderVARSNetwork(varsSnap, date);
    // Parabolic
    const parabolicSnap = parabolicHistory.find(h => h.report_date === date);
    parabolicData = parabolicSnap ? (parabolicSnap.tickers || []) : [];
    parabolicEmptyMessage = !parabolicSnap && date
      ? `No parabolic results for ${date}.`
      : 'No parabolic results for this date.';
    sortAndRenderParabolic();
    // EP Scanner
    applyEPAfternoonSnapshot(
      epAfternoonHistory.find(h => h.report_date === date),
      date
    );
    applyEPMorningSnapshot(
      epMorningHistory.find(h => h.report_date === date),
      date
    );
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
    renderTimeTravelBar('ep-tt-dates',
      tabSessionDates(epMorningHistory, epAfternoonHistory), onTimeTravelSelect);
    renderTimeTravelBar('parabolic-tt-dates', tabSessionDates(parabolicHistory), onTimeTravelSelect);
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
    const VISIBLE = 3;  // first N as buttons; remainder go in the dropdown
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

  // ── NETWORK VIZ — Theme, Momentum, Volume, and VARS Viz ─────────────
  // Both tabs share one Cytoscape force-directed renderer. They differ only
  // in (a) where strength comes from — server-computed theme/setup scores,
  // client-derived momentum strength, or avg VARS — and (b) which DOM
  // containers they target.

  const VIZ_MODES = {
    themes: {
      containerId: 'theme-network',
      tooltipId: 'themeviz-tooltip',
      overlayId: 'themeviz-overlay',
      tabBtnId: 'tab-themeviz',
    },
    momentum: {
      containerId: 'momentum-network',
      tooltipId: 'momentumviz-tooltip',
      overlayId: 'momentumviz-overlay',
      tabBtnId: 'tab-momentumviz',
    },
    volume: {
      containerId: 'volume-network',
      tooltipId: 'volumeviz-tooltip',
      overlayId: 'volumeviz-overlay',
      tabBtnId: 'tab-volumeviz',
    },
    vars: {
      containerId: 'vars-network',
      tooltipId: 'varsviz-tooltip',
      overlayId: 'varsviz-overlay',
      tabBtnId: 'tab-varsviz',
    },
  };

  // Per-mode runtime state — cytoscape instance, tab handler flag, pending render
  const vizState = {
    themes:   { cy: null, pending: null, tabHandlerInstalled: false },
    momentum: { cy: null, pending: null, tabHandlerInstalled: false },
    volume:   { cy: null, pending: null, tabHandlerInstalled: false },
    vars:     { cy: null, pending: null, tabHandlerInstalled: false },
  };

  function isVizVisible(mode) {
    const c = document.getElementById(VIZ_MODES[mode].containerId);
    return !!(c && c.offsetHeight > 0 && c.offsetWidth > 0);
  }

  function installVizTabHandler(mode) {
    const state = vizState[mode];
    if (state.tabHandlerInstalled) return;
    const cfg = VIZ_MODES[mode];
    const tabBtn = document.getElementById(cfg.tabBtnId);
    if (!tabBtn) return;
    tabBtn.addEventListener('click', () => {
      // Defer one tick so the tab content's display has switched on
      setTimeout(() => {
        if (state.pending) {
          const { snap, date } = state.pending;
          state.pending = null;
          actuallyRenderNetwork(snap, mode, date);
          return;
        }
        if (state.cy) {
          state.cy.resize();
          state.cy.fit(undefined, 40);
          syncTightPulses(state.cy, mode);
        }
      }, 60);
    });
    state.tabHandlerInstalled = true;
  }

  function computeAvgRs(theme) {
    const tk = theme.tickers || [];
    if (tk.length === 0) return 0;
    return tk.reduce((s, t) => s + (t.rs ?? 0), 0) / tk.length;
  }

  function computeAvgVars(theme) {
    if (typeof theme.avg_vars === 'number') return theme.avg_vars;
    const tk = theme.tickers || [];
    if (tk.length === 0) return 0;
    return tk.reduce((s, t) => s + (t.vars ?? 0), 0) / tk.length;
  }

  function computeAvgSetup(theme) {
    if (typeof theme.avg_setup_score === 'number') return theme.avg_setup_score;
    const tk = theme.tickers || [];
    if (tk.length === 0) return 0;
    return tk.reduce((s, t) => s + (t.score ?? 0), 0) / tk.length;
  }

  // Theme strength signal — server `score` for themes, derived for momentum/vars.
  function computeStrength(theme, mode) {
    if (mode === 'themes') return theme.score ?? 0;
    const tk = theme.tickers || [];
    if (tk.length === 0) return 0;
    const breadthFactor = Math.min(tk.length / 8, 1.5); // saturates around 8-12 tickers
    if (mode === 'vars' || mode === 'volume') {
      // Scale avg_vars (typical 2-10 range) into strength bands aligned with momentum (60/80/100)
      const avgVars = computeAvgVars(theme);
      return Math.round(avgVars * 15 * (0.6 + 0.4 * breadthFactor) * 10) / 10;
    }
    return Math.round(computeAvgRs(theme) * (0.6 + 0.4 * breadthFactor) * 10) / 10;
  }

  function actionabilityScore(theme, mode) {
    const tk = theme.tickers || [];
    if (tk.length === 0) return 0;
    if (mode === 'vars' || mode === 'volume') {
      const leaderDensity = tk.filter(t => (t.vars ?? 0) >= 6).length / tk.length;
      const scoreQuality = Math.min(computeAvgVars(theme) / 6, 1.2);
      return scoreQuality * (0.55 + 0.45 * leaderDensity);
    }
    if (mode === 'themes') {
      // Theme mode uses per-ticker composite (with RS fallback for entries
      // that lack a server-emitted score). Leader threshold sits on the new
      // composite scale (typical 30-80) rather than the RS scale (0-100).
      const leaderDensity = tk.filter(t => (t.score ?? t.rs ?? 0) >= 60).length / tk.length;
      const tightDensity  = tk.filter(t => t.ticker_color === 'green').length / tk.length;
      const scoreQuality = Math.min((theme.score ?? 0) / 70, 1.2);
      return scoreQuality * (0.45 + 0.30 * leaderDensity + 0.25 * tightDensity);
    }
    const leaderDensity = tk.filter(t => (t.rs ?? 0) >= 90).length / tk.length;
    const tightDensity  = tk.filter(t => t.ticker_color === 'green').length / tk.length;
    const scoreQuality = Math.min(computeAvgRs(theme) / 90, 1.2);
    return scoreQuality * (0.45 + 0.30 * leaderDensity + 0.25 * tightDensity);
  }

  function themeFill(strength, action) {
    // Warm-scale by strength; saturation modulated by actionability.
    // Strength bands tuned for the new theme-score range (~30-80 typical).
    let hue, baseSat, light;
    if (strength >= 65)       { hue = 14;  baseSat = 92; light = 56; }   // scarlet — blazing
    else if (strength >= 55)  { hue = 35;  baseSat = 88; light = 53; }   // orange — strong
    else if (strength >= 45)  { hue = 50;  baseSat = 70; light = 48; }   // gold   — solid
    else                      { hue = 215; baseSat = 14; light = 40; }   // slate  — faded
    const sat = Math.round(baseSat * Math.max(0.45, Math.min(1.0, action)));
    return `hsl(${hue}, ${sat}%, ${light}%)`;
  }

  function rsFill(rs) {
    // Match dashboard 4-tier ticker palette
    if (rs >= 90) return '#00e676';   // --green
    if (rs >= 80) return '#00c8ff';   // --accent (cyan)
    if (rs >= 50) return '#ffb300';   // --amber
    return '#ff3355';                  // --red
  }

  function varsFill(v) {
    // 4-tier palette aligned to VARS bands (>2 is screener gate; >6 is exceptional)
    if (v >= 6) return '#00e676';
    if (v >= 4) return '#00c8ff';
    if (v >= 2) return '#ffb300';
    return '#ff3355';
  }

  function actionabilityLabel(a) {
    if (a >= 0.95) return 'Highly actionable';
    if (a >= 0.75) return 'Actionable';
    if (a >= 0.55) return 'Mixed';
    return 'Late / extended';
  }

  function buildVizTooltip(node, mode) {
    const d = node.data();
    if (d.kind === 'l1') {
      return (
        `<div class="tip-title">${d.label}</div>` +
        `<div class="tip-sub">Narrative hub · ${d.themes} sub-themes</div>`
      );
    }
    if (d.kind === 'theme') {
      const strengthLabel = mode === 'themes' ? 'Score' : 'Strength';
      const avgLabel = (mode === 'vars' || mode === 'volume') ? 'Avg VARS' : 'Avg RS';
      const avgFmt = (mode === 'vars' || mode === 'volume')
        ? (d.avg_rs?.toFixed?.(2) ?? '—')
        : ((d.avg_rs?.toFixed?.(1) ?? '—') + '%');
      return (
        `<div class="tip-title">${d.label}</div>` +
        `<div class="tip-sub">Rank #${d.rank} · ${actionabilityLabel(d.action)}</div>` +
        `<div class="tip-grid">` +
        `<span class="tip-k">${strengthLabel}</span><span class="tip-v">${d.strength?.toFixed?.(1) ?? '—'}</span>` +
        `<span class="tip-k">${avgLabel}</span><span class="tip-v">${avgFmt}</span>` +
        `<span class="tip-k">Breadth</span><span class="tip-v">${d.breadth} tickers</span>` +
        `<span class="tip-k">Action</span><span class="tip-v">${(d.action * 100).toFixed(0)}%</span>` +
        `</div>`
      );
    }
    const tags = [];
    if (d.isLeader) tags.push('<span class="tip-tag tag-leader">LEADER</span>');
    if (d.isBridge) tags.push('<span class="tip-tag tag-bridge">BRIDGE</span>');
    if (d.isTight)  tags.push('<span class="tip-tag tag-tight">TIGHT</span>');
    const headLine = (mode === 'vars' || mode === 'volume')
      ? `VARS ${d.vars?.toFixed?.(2) ?? '—'} · 20EMA ${d.vars_20ema?.toFixed?.(2) ?? '—'}`
      : `RS ${d.rs?.toFixed?.(1) ?? '—'}%`;
    return (
      `<div class="tip-title">${d.label} ${tags.join(' ')}</div>` +
      `<div class="tip-sub">${headLine}</div>` +
      `<div class="tip-grid">` +
      `<span class="tip-k">Price</span><span class="tip-v">$${d.price ?? '—'}</span>` +
      `<span class="tip-k">Float</span><span class="tip-v">${d.float ?? '—'}M</span>` +
      `<span class="tip-k">EPS</span><span class="tip-v">${d.eps ?? '—'}</span>` +
      `<span class="tip-k">Sales</span><span class="tip-v">${d.sales ?? '—'}</span>` +
      `<span class="tip-k">Short</span><span class="tip-v">${d.short ?? '—'}%</span>` +
      `</div>`
    );
  }

  function syncTightPulses(cy, mode) {
    const overlay = document.getElementById(VIZ_MODES[mode].overlayId);
    if (!overlay) return;
    overlay.innerHTML = '';
    cy.nodes('.tight').forEach(node => {
      const pos = node.renderedPosition();
      const r = (node.renderedWidth() / 2) + 7;
      const ring = document.createElement('div');
      ring.className = 'tight-pulse-ring';
      ring.style.left = `${pos.x - r}px`;
      ring.style.top  = `${pos.y - r}px`;
      ring.style.width  = `${r * 2}px`;
      ring.style.height = `${r * 2}px`;
      overlay.appendChild(ring);
    });
  }

  function filterAndRankThemes(snap, mode) {
    const HOT_RS = 70, HOT_BREADTH = 3, HOT_VARS = 2, HOT_VARS_BREADTH = 1;
    if (mode === 'vars' || mode === 'volume') {
      // VARS/Volume export — keep singletons so a lone leader still shows its theme
      let hot = (snap.themes || []).filter(t => (t.tickers || []).length >= HOT_VARS_BREADTH);
      hot = hot.filter(t => computeAvgVars(t) >= HOT_VARS);
      return hot
        .map(t => Object.assign({}, t, {
          _strength: computeStrength(t, mode),
          _avg_rs:   computeAvgVars(t),  // reused as primary metric in tooltip/meta
        }))
        .sort((a, b) => b._strength - a._strength)
        .map((t, i) => Object.assign(t, { _rank: i + 1 }));
    }
    let hot = (snap.themes || []).filter(t => (t.tickers || []).length >= HOT_BREADTH);
    if (mode === 'themes') {
      hot = hot.filter(t => (t.avg_rs ?? 0) >= HOT_RS);
    } else {
      hot = hot.filter(t => computeAvgRs(t) >= HOT_RS);
    }
    return hot
      .map(t => Object.assign({}, t, {
        _strength: computeStrength(t, mode),
        _avg_rs:   mode === 'themes' ? (t.avg_rs ?? 0) : computeAvgRs(t),
      }))
      .sort((a, b) => b._strength - a._strength)
      .map((t, i) => Object.assign(t, { _rank: i + 1 }));
  }

  // Public entry — defers heavy work until the target tab is visible so that
  // cose layout and fit() see real container dimensions.
  function renderNetwork(snap, mode, date) {
    installVizTabHandler(mode);
    if (isVizVisible(mode)) {
      actuallyRenderNetwork(snap, mode, date);
    } else {
      vizState[mode].pending = { snap, date };
    }
  }

  function renderThemeNetwork(snap, date)    { renderNetwork(snap, 'themes', date); }
  function renderMomentumNetwork(snap, date) { renderNetwork(snap, 'momentum', date); }
  function renderVolumeNetwork(snap, date)   { renderNetwork(snap, 'volume', date); }
  function renderVARSNetwork(snap, date)     { renderNetwork(varsVizSnap(snap), 'vars', date); }

  function actuallyRenderNetwork(snap, mode, date) {
    const cfg = VIZ_MODES[mode];
    const state = vizState[mode];
    const container = document.getElementById(cfg.containerId);
    const tooltip = document.getElementById(cfg.tooltipId);
    const overlay = document.getElementById(cfg.overlayId);
    if (!container) return;
    if (typeof cytoscape === 'undefined') {
      container.innerHTML = '<div class="no-data">Cytoscape library failed to load.</div>';
      return;
    }

    // Always tear down the previous render so a date-switch never leaves a
    // stale graph on screen — even when the new date has no snapshot.
    if (state.cy) { try { state.cy.destroy(); } catch (e) {} state.cy = null; }
    if (overlay) overlay.innerHTML = '';
    if (tooltip) tooltip.style.display = 'none';

    if (!snap) {
      const dateLabel = date ? ` for ${date}` : ' for this session';
      container.innerHTML = `<div class="no-data" style="margin:60px auto;text-align:center">No data${dateLabel}.</div>`;
      return;
    }

    const hot = filterAndRankThemes(snap, mode);

    if (hot.length === 0) {
      container.innerHTML = '<div class="no-data" style="margin:60px auto;text-align:center">No qualifying themes for this date.</div>';
      return;
    }
    container.innerHTML = '';

    // Index ticker → list of themes (bridge detection)
    const tickerThemes = {};
    for (const theme of hot) {
      for (const tk of (theme.tickers || [])) {
        (tickerThemes[tk.ticker] = tickerThemes[tk.ticker] || []).push(theme.name);
      }
    }
    // Per-theme leader (highest RS within theme; highest VARS/setup score for specialized modes)
    const leaderByTheme = {};
    for (const theme of hot) {
      const sortKey = (mode === 'vars' || mode === 'volume') ? 'vars' : 'rs';
      const top = [...(theme.tickers || [])].sort((a, b) => (b[sortKey] ?? 0) - (a[sortKey] ?? 0))[0];
      if (top) leaderByTheme[theme.name] = top.ticker;
    }

    const elements = [];
    const seen = new Set();
    const l1Seen = new Set();
    // L1 narratives we know about — used to recognise legacy "AI - Memory"
    // labels whose prefix is still a real hub. Populated from any theme's
    // server-side `l1` field, plus what's discovered while parsing names.
    const knownL1s = new Set();
    for (const theme of hot) {
      if (theme.l1) knownL1s.add(theme.l1);
    }
    // L1s with proper L2/L3 children present in this view. A theme named
    // just "Space" (bare L1) while "Space / Launch" etc. also exist would
    // otherwise render as an orphan circle next to the L1 hexagon hub —
    // skip the bare entry so future tagging regressions don't reintroduce
    // the duplicate-node bug.
    const l1sWithChildren = new Set();
    for (const theme of hot) {
      if (theme.name && theme.name.includes(' / ')) {
        l1sWithChildren.add(theme.name.split(' / ', 1)[0].trim());
      }
    }
    const l1Of = (name) => {
      if (!name) return '';
      // 1. Canonical " / " path.
      if (name.includes(' / ')) {
        return name.split(' / ', 1)[0].trim();
      }
      // 2. Legacy " - " labels (e.g. "AI - Memory & Storage") — only when the
      //    prefix matches an L1 we already know about, so we don't accidentally
      //    bucket "Metals - Gold, Silver, Copper" under "Metals".
      if (name.includes(' - ')) {
        const prefix = name.split(' - ', 1)[0].trim();
        if (knownL1s.has(prefix)) return prefix;
      }
      return name;
    };
    // Strength aggregates per L1 — used to size the hub bubbles
    const l1Stats = {};
    for (const theme of hot) {
      const l1 = theme.l1 || l1Of(theme.name);
      const s = l1Stats[l1] = l1Stats[l1] || { strength: 0, themes: 0 };
      s.strength += theme._strength || 0;
      s.themes += 1;
    }
    for (const theme of hot) {
      const themeId = `theme::${theme.name}`;
      const action = actionabilityScore(theme, mode);
      const l1 = theme.l1 || l1Of(theme.name);
      const l1Id = `l1::${l1}`;
      // Defensive: skip a degenerate bare-L1 entry when proper L2 children
      // for the same L1 are present in this view. The hub will still be
      // emitted from one of those child iterations.
      if (theme.name === l1 && l1sWithChildren.has(l1)) {
        continue;
      }
      // Emit L1 hub node once
      if (!l1Seen.has(l1) && l1 !== theme.name) {
        l1Seen.add(l1);
        const stats = l1Stats[l1] || { strength: 0, themes: 1 };
        elements.push({
          data: {
            id: l1Id, kind: 'l1',
            label: l1,
            strength: Math.min(140, stats.strength / Math.max(1, stats.themes) + 10 * stats.themes),
            themes: stats.themes,
          },
        });
      }
      // is-a edge: theme -> L1 hub (only when there's a real hierarchy)
      if (l1 !== theme.name) {
        elements.push({
          data: {
            id: `is_a::${themeId}::${l1Id}`,
            source: themeId, target: l1Id,
            kind: 'is_a',
          },
        });
      }
      elements.push({
        data: {
          id: themeId, kind: 'theme',
          label: theme.name,
          l1,
          strength: theme._strength,
          avg_rs:   theme._avg_rs,
          breadth: (theme.tickers || []).length,
          rank: theme._rank,
          action,
          fill: themeFill(theme._strength, action),
          ringWidth: 2 + Math.round(action * 4),
        },
      });
      for (const tk of (theme.tickers || [])) {
        if (!seen.has(tk.ticker)) {
          seen.add(tk.ticker);
          const isLeader = leaderByTheme[theme.name] === tk.ticker;
          const isBridge = (tickerThemes[tk.ticker] || []).length >= 2;
          const isTight  = tk.ticker_color === 'green';
          const cls = [
            isLeader ? 'leader' : '',
            isBridge ? 'bridge' : '',
            isTight  ? 'tight'  : '',
          ].filter(Boolean).join(' ');
          elements.push({
            data: {
              id: tk.ticker, kind: 'ticker',
              label: tk.ticker,
              rs: tk.rs ?? 0, price: tk.price, float: tk.float,
              eps: tk.eps, sales: tk.sales, short: tk.short,
              vars: tk.vars ?? 0, vars_20ema: tk.vars_20ema ?? 0,
              setup_score: tk.score ?? 0, flags: tk.flags || '',
              fill: (mode === 'vars' || mode === 'volume') ? varsFill(tk.vars ?? 0) : rsFill(tk.rs ?? 0),
              isLeader, isBridge, isTight,
            },
            classes: cls,
          });
        }
        elements.push({
          data: {
            source: themeId, target: tk.ticker,
            weight: (mode === 'vars' || mode === 'volume') ? (tk.vars ?? 0) * 10 : (tk.rs ?? 0),
            isLeader: leaderByTheme[theme.name] === tk.ticker,
          },
        });
      }
    }

    // Pre-position top-3 themes near canvas center
    const seed = {};
    if (hot[0]) seed[`theme::${hot[0].name}`] = { x: 0,    y: 0    };
    if (hot[1]) seed[`theme::${hot[1].name}`] = { x: -260, y: -150 };
    if (hot[2]) seed[`theme::${hot[2].name}`] = { x: 260,  y: -150 };
    for (const el of elements) {
      if (el.data && el.data.kind === 'theme' && seed[el.data.id]) {
        el.position = seed[el.data.id];
      }
    }

    state.cy = cytoscape({
      container,
      elements,
      layout: {
        name: 'cose',
        animate: false,
        randomize: false,
        nodeRepulsion: function (n) {
          const k = n.data('kind');
          if (k === 'l1')    return 55000;
          if (k === 'theme') return 22000;
          return 4500;
        },
        idealEdgeLength: function (e) {
          return e.data('kind') === 'is_a' ? 90 : 130;
        },
        gravity: 0.18,
        numIter: 1500,
        padding: 30,
        fit: true,
      },
      style: [
        { selector: 'node[kind = "l1"]', style: {
            'background-color': '#1a2434',
            'background-opacity': 0.65,
            'label': 'data(label)',
            'color': '#fde68a',
            'font-size': 18,
            'font-weight': 'bold',
            'font-family': 'DM Sans, system-ui, sans-serif',
            'width':  'mapData(strength, 30, 160, 70, 160)',
            'height': 'mapData(strength, 30, 160, 70, 160)',
            'shape': 'round-hexagon',
            'border-width': 3,
            'border-color': '#fde68a',
            'border-opacity': 0.75,
            'text-valign': 'center', 'text-halign': 'center',
            'text-outline-color': '#000000', 'text-outline-width': 3,
            'text-wrap': 'wrap', 'text-max-width': 140,
            'z-index': 1,
        }},
        { selector: 'node[kind = "theme"]', style: {
            'background-color': 'data(fill)',
            'label': 'data(label)',
            'color': '#ffffff',
            'font-size': 13,
            'font-weight': 'bold',
            'font-family': 'DM Sans, system-ui, sans-serif',
            'width':  'mapData(strength, 30, 80, 32, 110)',
            'height': 'mapData(strength, 30, 80, 32, 110)',
            'border-width': 'data(ringWidth)',
            'border-color': '#0c0f15',
            'border-opacity': 0.95,
            'text-valign': 'center', 'text-halign': 'center',
            'text-outline-color': '#07090d', 'text-outline-width': 2,
            'text-wrap': 'wrap', 'text-max-width': 110,
        }},
        { selector: 'node[kind = "theme"][rank = 1]', style: {
            'border-color': '#fde68a',
            'border-width': 6,
            'shadow-blur': 30,
            'shadow-color': '#fde68a',
            'shadow-opacity': 0.55,
        }},
        { selector: 'node[kind = "ticker"]', style: {
            'background-color': 'data(fill)',
            'label': 'data(label)',
            'color': '#c8d8ea',
            'font-size': 10,
            'font-family': 'IBM Plex Mono, monospace',
            'width':  'mapData(rs, 0, 100, 14, 32)',
            'height': 'mapData(rs, 0, 100, 14, 32)',
            'text-valign': 'bottom',
            'text-margin-y': 4,
            'border-width': 0,
        }},
        { selector: 'node.leader', style: {
            'font-size': 12,
            'font-weight': 'bold',
            'shadow-blur': 22,
            'shadow-color': 'data(fill)',
            'shadow-opacity': 0.9,
            'shadow-offset-x': 0,
            'shadow-offset-y': 0,
        }},
        { selector: 'node.bridge', style: {
            'border-width': 2,
            'border-color': '#ffffff',
            'border-opacity': 0.85,
        }},
        { selector: 'node.leader.bridge', style: {
            'border-width': 2,
            'border-color': '#ffffff',
        }},
        { selector: 'node.active-ticker', style: {
            'border-width': 4,
            'border-color': '#ffd700',
            'border-opacity': 1.0,
            'shadow-blur': 28,
            'shadow-color': '#ffd700',
            'shadow-opacity': 0.95,
            'shadow-offset-x': 0,
            'shadow-offset-y': 0,
            'z-index': 50,
        }},
        { selector: 'edge', style: {
            'width': 'mapData(weight, 0, 100, 1, 4.5)',
            'line-color': '#243044',
            'opacity': 0.55,
            'curve-style': 'bezier',
        }},
        { selector: 'edge[?isLeader]', style: {
            'line-color': '#7292b0',
            'opacity': 0.85,
        }},
        { selector: 'edge[kind = "is_a"]', style: {
            'width': 2,
            'line-color': '#fde68a',
            'line-style': 'dashed',
            'line-dash-pattern': [6, 4],
            'opacity': 0.35,
            'curve-style': 'straight',
            'target-arrow-shape': 'none',
        }},
      ],
      wheelSensitivity: 0.25,
      minZoom: 0.3,
      maxZoom: 3.0,
    });

    // Tight-pulse rings — repositioned on pan/zoom/render. The closures
    // null-check state.cy because cytoscape's internal ResizeObserver can
    // fire one more 'resize' event after destroy() has nulled state.cy
    // (e.g. on date-switch with no snapshot for the new date).
    syncTightPulses(state.cy, mode);
    state.cy.on('pan zoom resize', () => { if (state.cy) syncTightPulses(state.cy, mode); });
    state.cy.on('layoutstop', () => { if (state.cy) syncTightPulses(state.cy, mode); });
    state.cy.on('position', 'node', () => { if (state.cy) syncTightPulses(state.cy, mode); });

    // ResizeObserver — keeps the canvas filling the container when the user
    // drags the resize-handle between left/right panels.
    if (state.resizeObs) { try { state.resizeObs.disconnect(); } catch (e) {} }
    if (typeof ResizeObserver !== 'undefined') {
      let resizeTimer;
      state.resizeObs = new ResizeObserver(() => {
        clearTimeout(resizeTimer);
        resizeTimer = setTimeout(() => {
          if (!state.cy) return;
          state.cy.resize();
          syncTightPulses(state.cy, mode);
        }, 80);
      });
      state.resizeObs.observe(container);
    }

    installVizTabHandler(mode);
    if (container.offsetHeight > 0) {
      setTimeout(() => {
        if (!state.cy) return;  // user may have switched dates inside the 30ms window
        state.cy.resize();
        state.cy.fit(undefined, 40);
        syncTightPulses(state.cy, mode);
      }, 30);
    }

    // Hover tooltip
    if (tooltip) {
      state.cy.on('mouseover', 'node', (evt) => {
        tooltip.innerHTML = buildVizTooltip(evt.target, mode);
        tooltip.style.display = 'block';
      });
      state.cy.on('mousemove', 'node', (evt) => {
        const pos = evt.target.renderedPosition();
        tooltip.style.left = `${pos.x + 18}px`;
        tooltip.style.top  = `${pos.y + 18}px`;
      });
      state.cy.on('mouseout', 'node', () => {
        tooltip.style.display = 'none';
      });
    }

    // Click ticker node → open TradingView chart in the right panel
    state.cy.on('tap', 'node[kind = "ticker"]', (evt) => {
      const ticker = evt.target.data('label');
      if (!ticker) return;
      // Highlight selected ticker
      state.cy.nodes('node[kind = "ticker"]').removeClass('active-ticker');
      evt.target.addClass('active-ticker');
      const tabIdMap = { themes: 'themeviz', momentum: 'momentumviz', volume: 'volumeviz', vars: 'varsviz' };
      const tabId = tabIdMap[mode] || 'momentumviz';
      if (typeof openChart === 'function') openChart(tabId, ticker);
    });
  }

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

  // Members averaged into a viz leaf's score. radar.json ships every member
  // while radar_history.json stays capped at `radar.tickers_per_leaf` (10), so
  // the viz takes the same leading slice from both — otherwise today's leaves
  // would be diluted by their long tail and read cooler than the same leaf one
  // session back in the time-travel bar.
  const RADAR_VIZ_MEMBERS = 10;

  // Adapter: flatten a radar snapshot's L1 groups into the {themes:[...]}
  // shape the shared Cytoscape network renderer consumes (Theme Viz tab).
  // Per-leaf `score` is the mean member composite (0-100 scale, matching the
  // viz strength bands) and `avg_rs` the mean member RS (hot filter ≥ 70).
  function radarVizSnap(data) {
    const l1s = radarL1s(data);
    if (!l1s) return data;
    const themes = [];
    l1s.forEach(grp => {
      (grp.leaves || []).forEach(leaf => {
        const tickers = (leaf.tickers || []).slice(0, RADAR_VIZ_MEMBERS);
        const n = tickers.length;
        const avgRs = n ? tickers.reduce((s, t) => s + (t.rs ?? 0), 0) / n : 0;
        const avgScore = n ? tickers.reduce((s, t) => s + (t.score ?? 0), 0) / n : 0;
        themes.push({
          name: leaf.name,
          l1: grp.name,
          score: avgScore,
          avg_rs: avgRs,
          tickers,
        });
      });
    });
    return { report_date: data.report_date, themes };
  }

  // Flatten the L1-clustered VARS snapshot (themes = L1 sections carrying
  // `leaves`) back to leaf-level entries for the shared network viz. Legacy
  // flat snapshots (pre-clustering history) pass through unchanged.
  function varsVizSnap(data) {
    if (!data || !data.themes) return data;
    const themes = data.themes.flatMap(t => t.leaves || [t]);
    return { report_date: data.report_date, themes };
  }

  function renderThemes(data, date) {
    const container = document.getElementById('themes-container');
    if (!container) return;

    const l1s = radarL1s(data);
    if (!l1s || l1s.length === 0) {
      const msg = (date && !data) ? `No theme data for ${date}.` : 'No theme data for this date.';
      container.innerHTML = `<div class="no-data">${msg}</div>`;
      return;
    }

    const fmt = (v, d = 3) => (typeof v === 'number') ? v.toFixed(d) : '—';
    let html = '';
    l1s.forEach(grp => {
      const delta = (typeof grp.delta === 'number')
        ? (grp.delta >= 0 ? `+${grp.delta.toFixed(3)}` : grp.delta.toFixed(3))
        : '—';
      html += `
        <div class="theme-block">
          <div class="theme-header">
            <span class="theme-rank">#${grp.rank}</span>
            <span class="theme-name">${escHtml(grp.name)}</span>
            <span class="theme-score">boosted ${fmt(grp.boosted)} · raw ${fmt(grp.raw)} · Δ ${delta} · ${grp.n_leaves} theme${grp.n_leaves === 1 ? '' : 's'} · ${grp.n_members} stocks (${grp.n_screened} screened)</span>
          </div>
          <div class="theme-body radar-body">
      `;
      // Each leaf is a two-line block, not a table row: the metadata line
      // carries rank/name/N/scores, and the chips below get the panel's full
      // width so every ticker is reachable in a narrow left panel (a 5-column
      // table pushed Raw/Boosted off-screen and clipped the ticker list).
      (grp.leaves || []).forEach(leaf => {
        const label = leaf.l2 ? (leaf.l3 ? `${leaf.l2} / ${leaf.l3}` : leaf.l2) : leaf.name;
        const chips = (leaf.tickers || []).map(t => {
          const cls = ['tn-link', 'radar-chip', t.is_screened ? 'chip-screened' : 'chip-quiet'];
          if (t.ticker_color === 'green') cls.push('day-pattern-green');
          const tip = `RS ${t.rs ?? '—'} · VARS ${t.vars ?? '—'} · $${t.price ?? '—'}`;
          return `<span class="${cls.join(' ')}"${filterAttrs(t)} data-sym="${escAttr(t.ticker)}" data-nm="${escAttr(grp.name + ' · ' + t.ticker)}" title="${escAttr(tip)}">${escHtml(t.ticker)}</span>`;
        }).join('');
        html += `
            <div class="radar-leaf">
              <div class="radar-leaf-hdr">
                <span class="radar-rank">#${leaf.global_rank}</span>
                <span class="radar-leaf-name">${escHtml(label)}</span>
                <span class="radar-n">N=${leaf.n}</span>
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
        const rsClass = t.rs >= 80 ? 'up' : t.rs <= 20 ? 'dn' : '';
        const instVal = parseFloat(String(t.inst).replace(/[+%]/g, ''));
        const instClass = isNaN(instVal) ? 'neu' : instVal > 0 ? 'up' : instVal < 0 ? 'dn' : 'neu';
        const shortVal = parseFloat(t.short);
        const shortClass = isNaN(shortVal) ? 'neu' : shortVal >= 20 ? 'up' : shortVal >= 10 ? 'short-blue' : 'short-white';
        html += `
                <tr${filterAttrs(t)}>
                  <td class="l">
                    <span class="tn-link${t.ticker_color === 'green' ? ' day-pattern-green' : ''}" data-sym="${escAttr(t.ticker)}" data-nm="${escAttr(theme.name + ' · ' + t.ticker)}">
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
        const varsClass = t.vars >= 6 ? 'up' : t.vars < 2 ? 'dn' : '';
        const rsClass = t.rs >= 80 ? 'up' : t.rs <= 20 ? 'dn' : '';
        const instVal = parseFloat(String(t.inst).replace(/[+%]/g, ''));
        const instClass = isNaN(instVal) ? 'neu' : instVal > 0 ? 'up' : instVal < 0 ? 'dn' : 'neu';
        const shortVal = parseFloat(t.short);
        const shortClass = isNaN(shortVal) ? 'neu' : shortVal >= 20 ? 'up' : shortVal >= 10 ? 'short-blue' : 'short-white';
        const scan = t.scan || '';
        const days = (typeof t.days_since_hv === 'number') ? `${t.days_since_hv}d` : '';
        const scanLabel = scan ? (days ? `${scan} · ${days}` : scan) : '—';
        html += `
                <tr${filterAttrs(t)}>
                  <td class="l">
                    <span class="tn-link${t.ticker_color === 'green' ? ' day-pattern-green' : ''}" data-sym="${escAttr(t.ticker)}" data-nm="${escAttr(theme.name + ' · ' + t.ticker)}">
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
          const varsClass = t.vars >= 6 ? 'up' : t.vars < 2 ? 'dn' : '';
          const rsClass = t.rs >= 80 ? 'up' : t.rs <= 20 ? 'dn' : '';
          const instVal = parseFloat(String(t.inst).replace(/[+%]/g, ''));
          const instClass = isNaN(instVal) ? 'neu' : instVal > 0 ? 'up' : instVal < 0 ? 'dn' : 'neu';
          const shortVal = parseFloat(t.short);
          const shortClass = isNaN(shortVal) ? 'neu' : shortVal >= 20 ? 'up' : shortVal >= 10 ? 'short-blue' : 'short-white';
          const accel = (typeof t.vars_20ema === 'number')
            ? (t.vars > t.vars_20ema
              ? '<span class="accel accel-up">▲</span>'
              : '<span class="accel accel-dn">▼</span>')
            : '';
          html += `
                <tr${filterAttrs(t)}>
                  <td class="l">
                    <span class="tn-link${t.ticker_color === 'green' ? ' day-pattern-green' : ''}" data-sym="${escAttr(t.ticker)}" data-nm="${escAttr(leaf.name + ' · ' + t.ticker)}">
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
      const instClass = isNaN(instVal) ? 'neu' : instVal > 0 ? 'up' : instVal < 0 ? 'dn' : 'neu';
      const shortVal = parseFloat(row.short);
      const shortClass = isNaN(shortVal) ? 'neu' : shortVal >= 20 ? 'up' : shortVal >= 10 ? 'short-blue' : 'short-white';
      const atrVal = parseFloat(row.atr_multi_50sma);
      const atrClass = isNaN(atrVal) ? 'neu' : atrVal >= 15 ? 'dn' : atrVal >= 12 ? 'short-blue' : 'neu';
      const atrStr = isNaN(atrVal) ? '—' : atrVal.toFixed(1) + 'x';

      html += `
        <tr${filterAttrs(row)}>
          <td class="l">
            <span class="tn-link${row.ticker_color === 'green' ? ' day-pattern-green' : ''}" data-sym="${escAttr(row.ticker)}" data-nm="${escAttr(row.ticker + ' · Parabolic')}">${escHtml(row.ticker)}</span>
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
      const varsClass = row.vars == null ? '' : (row.vars >= 6 ? 'up' : row.vars < 2 ? 'dn' : '');
      const varsText = row.vars == null ? '—' : row.vars.toFixed(2);
      html += `
        <tr>
          <td class="l">
            <span class="tn-link${row.ticker_color === 'green' ? ' day-pattern-green' : ''}" data-sym="${escAttr(row.display_ticker || row.ticker)}" data-nm="${escAttr(row.name)}">${escHtml(row.ticker)}</span>
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
      const varsClass = row.vars == null ? '' : (row.vars >= 6 ? 'up' : row.vars < 2 ? 'dn' : '');
      const varsText = row.vars == null ? '—' : row.vars.toFixed(2);
      html += `
        <tr>
          <td class="l">
            <span class="tn-link${row.ticker_color === 'green' ? ' day-pattern-green' : ''}" data-sym="${escAttr(row.ticker)}" data-nm="${escAttr(row.name)}">${escHtml(row.ticker)}</span>
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

  // ── TABLE SORT (generic for ETF, Industry, and EP) ────
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
        else if (tab === 'ep_afternoon') sortAndRenderEPAfternoon();
        else if (tab === 'ep_morning') sortAndRenderEPMorning();
        else if (tab === 'parabolic') sortAndRenderParabolic();
        else sortAndRenderETF();
      });
    });
  }

  // ── EP SCANNER DATA (Afternoon + Morning) ──────────────────

  function normalizeEPSnapshot(snapshot) {
    if (!snapshot) return null;
    const reportDate = snapshot.report_date || snapshot.scan_date;
    if (!reportDate) return null;
    return { ...snapshot, report_date: reportDate };
  }

  function buildEPHistory(current, history) {
    const byDate = {};
    (history || []).forEach(item => {
      const snap = normalizeEPSnapshot(item);
      if (snap) byDate[snap.report_date] = snap;
    });

    const currentSnap = normalizeEPSnapshot(current);
    if (currentSnap) {
      byDate[currentSnap.report_date] = currentSnap;
    }

    const sorted = Object.values(byDate)
      .sort((a, b) => b.report_date.localeCompare(a.report_date));

    // Keep snapshots within the last SESSION_HISTORY_DAYS calendar days,
    // anchored to the newest snapshot (mirrors the server-side prune).
    if (sorted.length === 0) return sorted;
    const anchor = new Date(sorted[0].report_date + 'T12:00:00');
    const cutoff = new Date(anchor);
    cutoff.setDate(cutoff.getDate() - SESSION_HISTORY_DAYS);
    return sorted.filter(s => new Date(s.report_date + 'T12:00:00') >= cutoff);
  }

  function refreshEPAllTickers() {
    epAllTickers = {};
    epAfternoonData.forEach(t => { epAllTickers[t.ticker] = t; });
    epMorningData.forEach(t => { epAllTickers[t.ticker] = t; });
  }

  function formatEPSnapshotInfo(snapshot, selectedDate) {
    if (!snapshot) {
      return selectedDate ? `Session: ${selectedDate} | no scan` : '—';
    }

    const dateLabel = snapshot.scan_date || snapshot.report_date || selectedDate || '';
    if (!snapshot.timestamp) {
      return dateLabel ? `Session: ${dateLabel}` : '—';
    }

    const dt = new Date(snapshot.timestamp);
    const updated = dt.toLocaleDateString() + ' ' +
      dt.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    return dateLabel ? `Session: ${dateLabel} | Updated: ${updated}` : `Updated: ${updated}`;
  }

  function applyEPAfternoonSnapshot(snapshot, selectedDate = null) {
    const snap = normalizeEPSnapshot(snapshot);
    epAfternoonData = snap ? (snap.tickers || []) : [];
    epAfternoonEmptyMessage = selectedDate && !snap
      ? 'No afternoon EP scan for this date.'
      : 'No afternoon EP results.';
    sortAndRenderEPAfternoon();
    refreshEPAllTickers();

    const infoEl = document.getElementById('ep-afternoon-refresh-info');
    if (infoEl) infoEl.textContent = formatEPSnapshotInfo(snap, selectedDate);
  }

  function applyEPMorningSnapshot(snapshot, selectedDate = null) {
    const snap = normalizeEPSnapshot(snapshot);
    epMorningData = snap ? (snap.tickers || []) : [];
    epMorningEmptyMessage = selectedDate && !snap
      ? 'No morning EP scan for this date.'
      : 'No morning EP results.';
    sortAndRenderEPMorning();
    refreshEPAllTickers();

    const infoEl = document.getElementById('ep-morning-refresh-info');
    if (infoEl) infoEl.textContent = formatEPSnapshotInfo(snap, selectedDate);
  }

  function preferredEPSnapshot(current, history) {
    if (hasUserSelectedSession && activeSessionDate) {
      return history.find(h => h.report_date === activeSessionDate) || null;
    }
    return normalizeEPSnapshot(current) || history[0] || null;
  }

  function loadEPAfternoonData() {
    Promise.all([
      fetch(withCacheBust(EP_AFTERNOON_URL)).then(r => { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); }),
      fetch(withCacheBust(EP_AFTERNOON_HISTORY_URL)).then(r => r.json()).catch(() => []),
    ])
      .then(([current, history]) => {
        epAfternoonHistory = buildEPHistory(current, history);
        renderAllTimeTravelBars();
        applyEPAfternoonSnapshot(
          preferredEPSnapshot(current, epAfternoonHistory),
          hasUserSelectedSession ? activeSessionDate : null
        );
      })
      .catch(() => {
        epAfternoonHistory = [];
        renderAllTimeTravelBars();
        document.getElementById('ep-afternoon-body').innerHTML =
          '<tr><td colspan="8" class="no-data">Afternoon EP data not available.</td></tr>';
      });
  }

  function loadEPMorningData() {
    Promise.all([
      fetch(withCacheBust(EP_MORNING_URL)).then(r => { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); }),
      fetch(withCacheBust(EP_MORNING_HISTORY_URL)).then(r => r.json()).catch(() => []),
    ])
      .then(([current, history]) => {
        epMorningHistory = buildEPHistory(current, history);
        renderAllTimeTravelBars();
        applyEPMorningSnapshot(
          preferredEPSnapshot(current, epMorningHistory),
          hasUserSelectedSession ? activeSessionDate : null
        );
      })
      .catch(() => {
        epMorningHistory = [];
        renderAllTimeTravelBars();
        document.getElementById('ep-morning-body').innerHTML =
          '<tr><td colspan="8" class="no-data">Morning EP data not available.</td></tr>';
      });
  }

  function sortAndRenderEPAfternoon() {
    const s = sortState.ep_afternoon;
    epAfternoonData.sort((a, b) => {
      const av = a[s.column] ?? (s.dir === 'asc' ? Infinity : -Infinity);
      const bv = b[s.column] ?? (s.dir === 'asc' ? Infinity : -Infinity);
      return s.dir === 'desc' ? bv - av : av - bv;
    });
    renderEPAfternoonTable();
  }

  function sortAndRenderEPMorning() {
    const s = sortState.ep_morning;
    epMorningData.sort((a, b) => {
      const av = a[s.column] ?? (s.dir === 'asc' ? Infinity : -Infinity);
      const bv = b[s.column] ?? (s.dir === 'asc' ? Infinity : -Infinity);
      return s.dir === 'desc' ? bv - av : av - bv;
    });
    renderEPMorningTable();
  }

  function renderEPAfternoonTable() {
    const tbody = document.getElementById('ep-afternoon-body');
    if (!epAfternoonData.length) {
      tbody.innerHTML = `<tr><td colspan="8" class="no-data">${escHtml(epAfternoonEmptyMessage)}</td></tr>`;
      return;
    }
    let html = '';
    epAfternoonData.forEach(row => {
      html += epRow(row, 'ah_chg_pct', 'ah_price', 'AH');
    });
    tbody.innerHTML = html;
  }

  function renderEPMorningTable() {
    const tbody = document.getElementById('ep-morning-body');
    if (!epMorningData.length) {
      tbody.innerHTML = `<tr><td colspan="8" class="no-data">${escHtml(epMorningEmptyMessage)}</td></tr>`;
      return;
    }
    let html = '';
    epMorningData.forEach(row => {
      html += epRow(row, 'pm_chg_pct', 'pm_price', 'PM');
    });
    tbody.innerHTML = html;
  }

  function epRow(row, chgKey, priceKey, label) {
    const floatClass = (row.float != null && row.float < 150) ? 'ep-float-green' : 'neu';
    const shortClass = epShortClass(row.short);
    const dist52wClass = epDist52wClass(row.dist_52w_high);
    const atrClass = epAtrClass(row.atr_multiple);
    const rvolClass = epRvolClass(row.rvol);

    const floatStr = row.float != null ? row.float.toFixed(1) + 'M' : '—';
    const shortStr = row.short != null ? row.short.toFixed(1) + '%' : '—';
    const dist52wStr = row.dist_52w_high != null
      ? (row.dist_52w_high > 0 ? '+' : '') + row.dist_52w_high.toFixed(1) + '%' : '—';
    const atrStr = row.atr_multiple != null ? row.atr_multiple.toFixed(1) + '×' : '—';
    const chgVal = row[chgKey];
    const chgStr = chgVal != null ? (chgVal > 0 ? '+' : '') + chgVal.toFixed(2) + '%' : '—';
    const chgClass = pctClass(chgVal);
    const priceStr = row[priceKey] != null ? row[priceKey].toFixed(2) : '—';
    const rvolStr = row.rvol != null ? row.rvol.toFixed(1) + 'x' : '—';

    return `
      <tr>
        <td class="l">
          <span class="tn-link" data-sym="${escAttr(row.ticker)}" data-nm="${escAttr(row.ticker + ' · EP ' + label)}">${escHtml(row.ticker)}</span>
        </td>
        <td class="${floatClass}">${floatStr}</td>
        <td class="${shortClass}">${shortStr}</td>
        <td class="${dist52wClass}">${dist52wStr}</td>
        <td class="${atrClass}">${atrStr}</td>
        <td class="${chgClass}">${chgStr}</td>
        <td>${priceStr}</td>
        <td class="${rvolClass}">${rvolStr}</td>
      </tr>
    `;
  }

  // EP color helpers
  function epShortClass(val) {
    if (val == null) return 'neu';
    if (val > 20) return 'up';
    if (val > 10) return 'short-blue';
    return 'neu';
  }

  function epDist52wClass(val) {
    if (val == null) return 'neu';
    return val > -10 ? 'up' : 'neu';
  }

  function epAtrClass(val) {
    if (val == null) return 'neu';
    if (val < 5) return 'up';
    if (val < 7) return 'short-blue';
    if (val < 9) return 'neu';
    return 'dn';
  }

  function epRvolClass(val) {
    if (val == null) return 'neu';
    if (val >= 3) return 'rvol-high';
    if (val >= 1.5) return 'rvol-medium';
    return 'rvol-low';
  }

  // ── EP NEWS (shown on ticker click) ───────────────────────
  function initEPNewsClick() {
    document.getElementById('content-ep').addEventListener('click', (e) => {
      const link = e.target.closest('.tn-link');
      if (!link) return;
      const sym = link.dataset.sym;
      if (!sym) return;
      const tickerData = epAllTickers[sym];
      if (!tickerData || !tickerData.news || tickerData.news.length === 0) {
        hideEPNews();
        return;
      }
      showEPNews(sym, tickerData.news);
    });
  }

  function showEPNews(ticker, newsItems) {
    const section = document.getElementById('ep-news-section');
    const tickerEl = document.getElementById('ep-news-ticker');
    const contentEl = document.getElementById('ep-news-content');

    tickerEl.textContent = ticker;
    let html = '';
    newsItems.forEach(item => {
      html += `
        <div class="ep-news-item">
          <a href="${escAttr(item.link)}" target="_blank" rel="noopener">${escHtml(item.title)}</a>
          <div class="ep-news-meta">${escHtml(item.source)} · ${escHtml(item.date)}</div>
        </div>
      `;
    });
    contentEl.innerHTML = html;
    section.style.display = '';
  }

  function hideEPNews() {
    document.getElementById('ep-news-section').style.display = 'none';
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
    if (val == null || val === '') return 'neu';
    const n = parseFloat(val);
    if (isNaN(n)) return 'neu';
    return n > 0 ? 'up' : n < 0 ? 'dn' : 'neu';
  }

  // RS_STS% color: >=90 green, >=80 blue, >=50 yellow, <50 red
  function rsStsPctClass(val) {
    if (val == null) return 'neu';
    const n = parseFloat(val);
    if (isNaN(n)) return 'neu';
    if (n >= 90) return 'rs-green';
    if (n >= 80) return 'rs-blue';
    if (n >= 50) return 'rs-yellow';
    return 'rs-red';
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
