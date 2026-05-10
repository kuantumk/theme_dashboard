// ═══════════════════════════════════════════════════════
// MARKET MONITOR — App Logic V2.1
// ═══════════════════════════════════════════════════════

(function () {
  'use strict';

  // ── CONFIG ────────────────────────────────────────────
  const THEME_DATA_URL = 'data/themes.json';
  const THEME_HISTORY_URL = 'data/themes_history.json';
  const MOMENTUM_DATA_URL = 'data/momentum_136.json';
  const MOMENTUM_HISTORY_URL = 'data/momentum_136_history.json';
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

  // Symbols that need a different symbol for TradingView widget vs data fetch.
  // NOTE: TVC/CAPITALCOM/CBOE treasury yield symbols are all restricted in the
  // embedded widget. FRED is the only embeddable source (line chart, no candlestick).
  const TV_CHART_SYM_MAP = {
    'CAPITALCOM:US2YR':  'FRED:DGS2',
    'CAPITALCOM:US10YR': 'FRED:DGS10',
    'CAPITALCOM:US30YR': 'FRED:DGS30',
  };

  // Active chart per tab
  let activeCharts = { macro: null, themes: null, momentum: null, vars: null, varsviz: null, industry: null, etf: null, ep: null, parabolic: null };

  // Sort state per table
  let sortState = {
    etf: { column: 'rs_sts', dir: 'desc' },
    industry: { column: 'rs_sts', dir: 'desc' },
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
    initResizablePanels();
    initArrowKeyNav();
    loadMeta();
    loadMacroData();
    loadBreadthData();
    loadThemeData();
    loadMomentumData();
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
      const name = link.dataset.nm || sym;
      if (!sym) return;

      const tabContent = link.closest('.tab-content');
      if (!tabContent) return;

      let tabId;
      if (tabContent.id === 'content-macro') tabId = 'macro';
      else if (tabContent.id === 'content-themes') tabId = 'themes';
      else if (tabContent.id === 'content-momentum') tabId = 'momentum';
      else if (tabContent.id === 'content-vars') tabId = 'vars';
      else if (tabContent.id === 'content-industry') tabId = 'industry';
      else if (tabContent.id === 'content-etf') tabId = 'etf';
      else if (tabContent.id === 'content-ep') tabId = 'ep';
      else if (tabContent.id === 'content-parabolic') tabId = 'parabolic';
      else return;

      tabContent.querySelectorAll('.tn-link').forEach(l => l.classList.remove('active-ticker'));
      link.classList.add('active-ticker');

      openChart(tabId, chartSym, name);
    });
  }

  function openChart(tabId, sym, name) {
    const headerEl = document.getElementById(tabId + '-chart-header');
    const symEl = document.getElementById(tabId + '-chart-sym');
    const nameEl = document.getElementById(tabId + '-chart-name');
    const alertEl = document.getElementById(tabId + '-alert-link');
    const areaEl = document.getElementById(tabId + '-chart-area');

    if (!headerEl || !areaEl) return;

    headerEl.style.display = 'flex';
    symEl.textContent = sym;
    nameEl.textContent = name;

    alertEl.href = 'https://www.tradingview.com/chart/?symbol=' + encodeURIComponent(sym) + '&interval=D';

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
          "MAExp@tv-basicstudies",
          "MASimple@tv-basicstudies",
          "STD;Volume"
        ],
        "studies_overrides": {
          "moving average exponential.length": 20,
          "moving average exponential.ma.color": "#4CAF50",
          "moving average exponential.ma.linewidth": 2,
          "moving average.length": 50,
          "moving average.ma.color": "#FFD700",
          "moving average.ma.linewidth": 2
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
        setTimeout(() => openChart(tabId, sym, name), 300);
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
  let navIndices = { macro: -1, themes: -1, momentum: -1, vars: -1, industry: -1, etf: -1, ep: -1, parabolic: -1 };

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
      const name = link.dataset.nm || sym;

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
      openChart(tabId, link.dataset.chartSym || sym, name);
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
          const dt = new Date(data.export_timestamp);
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
                tr.onclick = () => openChart('macro', TV_CHART_SYM_MAP[item.tv] || item.tv, item.name);
              }
            }
          }
        });

        // By default open OANDA:SPX500USD
        openChart('macro', 'OANDA:SPX500USD', 'S&P 500 Futures');
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
  let themesHistory = [];    // Array of theme snapshots, newest first
  let momentumHistory = [];  // Array of momentum snapshots, newest first
  let varsHistory = [];      // Array of vars snapshots, newest first
  let parabolicHistory = []; // Array of parabolic snapshots, newest first
  let industryHistory = [];  // Array of {report_date, data} snapshots
  let etfHistory = [];       // Array of {report_date, data} snapshots
  let activeSessionDate = null;
  let hasUserSelectedSession = false;

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
        renderThemeNetwork(current);
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

  /** Collect all available session dates and render all time-travel bars. */
  function getSessionDates() {
    const dates = new Set();
    themesHistory.forEach(h => dates.add(h.report_date));
    momentumHistory.forEach(h => dates.add(h.report_date));
    varsHistory.forEach(h => dates.add(h.report_date));
    parabolicHistory.forEach(h => dates.add(h.report_date));
    epAfternoonHistory.forEach(h => dates.add(h.report_date));
    epMorningHistory.forEach(h => dates.add(h.report_date));
    industryHistory.forEach(h => dates.add(h.report_date));
    etfHistory.forEach(h => dates.add(h.report_date));
    return [...dates].sort().reverse();
  }

  function onTimeTravelSelect(date) {
    // Each tab's history accumulates independently (e.g. vars_history can lag
    // themes_history if the screener was added recently). The shared session
    // dropdown is the union of all dates, so a click can land on a date that
    // exists in one tab's history but not another's. ALWAYS re-render every
    // tab — render functions show a date-specific empty state when the snap
    // is missing so we never leave stale content on screen.
    //
    // Themes
    const themeSnap = themesHistory.find(h => h.report_date === date);
    renderThemes(themeSnap, date);
    renderThemeNetwork(themeSnap, date);
    // Momentum 1/3/6
    const momSnap = momentumHistory.find(h => h.report_date === date);
    renderMomentum(momSnap, date);
    renderMomentumNetwork(momSnap, date);
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
    const dates = getSessionDates();
    renderTimeTravelBar('time-travel-dates', dates, onTimeTravelSelect);
    renderTimeTravelBar('momentum-tt-dates', dates, onTimeTravelSelect);
    renderTimeTravelBar('industry-tt-dates', dates, onTimeTravelSelect);
    renderTimeTravelBar('etf-tt-dates', dates, onTimeTravelSelect);
    renderTimeTravelBar('ep-tt-dates', dates, onTimeTravelSelect);
    renderTimeTravelBar('parabolic-tt-dates', dates, onTimeTravelSelect);
    renderTimeTravelBar('themeviz-tt-dates', dates, onTimeTravelSelect);
    renderTimeTravelBar('momentumviz-tt-dates', dates, onTimeTravelSelect);
    renderTimeTravelBar('vars-tt-dates', dates, onTimeTravelSelect);
    renderTimeTravelBar('varsviz-tt-dates', dates, onTimeTravelSelect);
  }

  /**
   * Render a time-travel date-selector bar.
   * Shows the last 5 sessions as clickable buttons and the rest (up to 40 total)
   * in a dropdown to the right so users can jump farther back without clutter.
   *
   * @param {string} containerId  - DOM id of the .time-travel-dates element
   * @param {Array}  dates        - ordered list of report_date strings (newest first)
   * @param {Function} onSelect   - callback(date) when user picks a date
   */
  function renderTimeTravelBar(containerId, dates, onSelect) {
    const container = document.getElementById(containerId);
    if (!container || dates.length === 0) return;

    const weekdays = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
    const VISIBLE = 5;  // first N as buttons; remainder go in the dropdown
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
        : '+ older sessions…';
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

  // ── NETWORK VIZ — Theme Viz + Momentum Viz ─────────────
  // Both tabs share one Cytoscape force-directed renderer. They differ only
  // in (a) where strength comes from — themes carry a server-computed `score`,
  // momentum derives strength client-side from avg RS + breadth — and
  // (b) which DOM containers they target.

  const VIZ_MODES = {
    themes: {
      containerId: 'theme-network',
      metaId: 'themeviz-meta',
      tooltipId: 'themeviz-tooltip',
      overlayId: 'themeviz-overlay',
      tabBtnId: 'tab-themeviz',
      hotLabel: 'hot themes',
    },
    momentum: {
      containerId: 'momentum-network',
      metaId: 'momentumviz-meta',
      tooltipId: 'momentumviz-tooltip',
      overlayId: 'momentumviz-overlay',
      tabBtnId: 'tab-momentumviz',
      hotLabel: 'momentum themes',
    },
    vars: {
      containerId: 'vars-network',
      metaId: 'varsviz-meta',
      tooltipId: 'varsviz-tooltip',
      overlayId: 'varsviz-overlay',
      tabBtnId: 'tab-varsviz',
      hotLabel: 'VARS themes',
    },
  };

  // Per-mode runtime state — cytoscape instance, tab handler flag, pending render
  const vizState = {
    themes:   { cy: null, pending: null, tabHandlerInstalled: false },
    momentum: { cy: null, pending: null, tabHandlerInstalled: false },
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

  // Theme strength signal — server `score` for themes, derived for momentum/vars.
  function computeStrength(theme, mode) {
    if (mode === 'themes') return theme.score ?? 0;
    const tk = theme.tickers || [];
    if (tk.length === 0) return 0;
    const breadthFactor = Math.min(tk.length / 8, 1.5); // saturates around 8-12 tickers
    if (mode === 'vars') {
      // Scale avg_vars (typical 2-10 range) into strength bands aligned with momentum (60/80/100)
      const avgVars = computeAvgVars(theme);
      return Math.round(avgVars * 15 * (0.6 + 0.4 * breadthFactor) * 10) / 10;
    }
    return Math.round(computeAvgRs(theme) * (0.6 + 0.4 * breadthFactor) * 10) / 10;
  }

  function actionabilityScore(theme, mode) {
    const tk = theme.tickers || [];
    if (tk.length === 0) return 0;
    if (mode === 'vars') {
      const leaderDensity = tk.filter(t => (t.vars ?? 0) >= 6).length / tk.length;
      const scoreQuality = Math.min(computeAvgVars(theme) / 6, 1.2);
      return scoreQuality * (0.55 + 0.45 * leaderDensity);
    }
    const leaderDensity = tk.filter(t => (t.rs ?? 0) >= 90).length / tk.length;
    const tightDensity  = tk.filter(t => t.ticker_color === 'green').length / tk.length;
    const scoreQuality = mode === 'themes'
      ? Math.min((theme.score ?? 0) / 100, 1.2)
      : Math.min(computeAvgRs(theme) / 90, 1.2);
    return scoreQuality * (0.45 + 0.30 * leaderDensity + 0.25 * tightDensity);
  }

  function themeFill(strength, action) {
    // Warm-scale by strength; saturation modulated by actionability
    let hue, baseSat, light;
    if (strength >= 100)      { hue = 14;  baseSat = 92; light = 56; }   // scarlet — blazing
    else if (strength >= 80)  { hue = 35;  baseSat = 88; light = 53; }   // orange — strong
    else if (strength >= 60)  { hue = 50;  baseSat = 70; light = 48; }   // gold   — solid
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
    if (d.kind === 'theme') {
      const strengthLabel = mode === 'themes' ? 'Score' : 'Strength';
      const avgLabel = mode === 'vars' ? 'Avg VARS' : 'Avg RS';
      const avgFmt = mode === 'vars'
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
    const headLine = mode === 'vars'
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

  function buildVizMetaHtml(snap, hot, mode) {
    const date = `<span class="meta-date">${snap.report_date ?? '—'}</span>`;
    const count = `<span class="meta-pill">${hot.length} ${VIZ_MODES[mode].hotLabel}</span>`;
    if (mode === 'themes') {
      return date +
        `<span class="meta-pill">NCFD ${snap.ncfd != null ? snap.ncfd.toFixed(1) + '%' : '—'}</span>` +
        `<span class="meta-pill">MMFI ${snap.mmfi != null ? snap.mmfi.toFixed(1) + '%' : '—'}</span>` +
        count;
    }
    // Momentum / VARS snapshots don't carry NCFD/MMFI; just date + count
    return date + count;
  }

  function filterAndRankThemes(snap, mode) {
    const HOT_RS = 70, HOT_BREADTH = 3, HOT_VARS = 2, HOT_VARS_BREADTH = 1;
    if (mode === 'vars') {
      // VARS export is already filtered to vars > 2 by the screener — keep singletons
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
    const cfg = VIZ_MODES[mode];
    const meta = document.getElementById(cfg.metaId);
    if (meta) {
      if (snap) {
        const hot = filterAndRankThemes(snap, mode);
        meta.innerHTML = buildVizMetaHtml(snap, hot, mode);
      } else if (date) {
        meta.innerHTML = `<span class="meta-date">${date}</span>`;
      }
    }
    installVizTabHandler(mode);
    if (isVizVisible(mode)) {
      actuallyRenderNetwork(snap, mode, date);
    } else {
      vizState[mode].pending = { snap, date };
    }
  }

  function renderThemeNetwork(snap, date)    { renderNetwork(snap, 'themes', date); }
  function renderMomentumNetwork(snap, date) { renderNetwork(snap, 'momentum', date); }
  function renderVARSNetwork(snap, date)     { renderNetwork(snap, 'vars', date); }

  function actuallyRenderNetwork(snap, mode, date) {
    const cfg = VIZ_MODES[mode];
    const state = vizState[mode];
    const container = document.getElementById(cfg.containerId);
    const meta = document.getElementById(cfg.metaId);
    const tooltip = document.getElementById(cfg.tooltipId);
    const overlay = document.getElementById(cfg.overlayId);
    if (!container || !meta) return;
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
      if (date) meta.innerHTML = `<span class="meta-date">${date}</span>`;
      const dateLabel = date ? ` for ${date}` : ' for this session';
      container.innerHTML = `<div class="no-data" style="margin:60px auto;text-align:center">No data${dateLabel}.</div>`;
      return;
    }

    const hot = filterAndRankThemes(snap, mode);
    meta.innerHTML = buildVizMetaHtml(snap, hot, mode);

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
    // Per-theme leader (highest RS within theme; highest VARS for vars mode)
    const leaderByTheme = {};
    for (const theme of hot) {
      const sortKey = mode === 'vars' ? 'vars' : 'rs';
      const top = [...(theme.tickers || [])].sort((a, b) => (b[sortKey] ?? 0) - (a[sortKey] ?? 0))[0];
      if (top) leaderByTheme[theme.name] = top.ticker;
    }

    const elements = [];
    const seen = new Set();
    for (const theme of hot) {
      const themeId = `theme::${theme.name}`;
      const action = actionabilityScore(theme, mode);
      elements.push({
        data: {
          id: themeId, kind: 'theme',
          label: theme.name,
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
              fill: mode === 'vars' ? varsFill(tk.vars ?? 0) : rsFill(tk.rs ?? 0),
              isLeader, isBridge, isTight,
            },
            classes: cls,
          });
        }
        elements.push({
          data: {
            source: themeId, target: tk.ticker,
            weight: mode === 'vars' ? (tk.vars ?? 0) * 10 : (tk.rs ?? 0),
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
        nodeRepulsion: function (n) { return n.data('kind') === 'theme' ? 22000 : 4500; },
        idealEdgeLength: function () { return 130; },
        gravity: 0.18,
        numIter: 1500,
        padding: 30,
        fit: true,
      },
      style: [
        { selector: 'node[kind = "theme"]', style: {
            'background-color': 'data(fill)',
            'label': 'data(label)',
            'color': '#ffffff',
            'font-size': 13,
            'font-weight': 'bold',
            'font-family': 'DM Sans, system-ui, sans-serif',
            'width':  'mapData(strength, 30, 130, 32, 110)',
            'height': 'mapData(strength, 30, 130, 32, 110)',
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
      const tabIdMap = { themes: 'themeviz', momentum: 'momentumviz', vars: 'varsviz' };
      const tabId = tabIdMap[mode] || 'momentumviz';
      if (typeof openChart === 'function') openChart(tabId, ticker, ticker);
    });
  }

  function renderThemes(data, date) {
    const container = document.getElementById('themes-container');
    if (!container) return;

    if (!data || !data.themes || data.themes.length === 0) {
      const msg = (date && !data) ? `No themes data for ${date}.` : 'No themes found for this date.';
      container.innerHTML = `<div class="no-data">${msg}</div>`;
      return;
    }

    let html = '';
    data.themes.forEach((theme, idx) => {
      html += `
        <div class="theme-block">
          <div class="theme-header">
            <span class="theme-rank">#${idx + 1}</span>
            <span class="theme-name">${escHtml(theme.name)}</span>
            <span class="theme-score">Score: ${theme.score?.toFixed(1) || '—'} · Avg RS: ${theme.avg_rs?.toFixed(1) || '—'}%</span>
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
                <tr>
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
                <tr>
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
                <th>VARS</th>
                <th>VARS 20EMA</th>
                <th>RS%</th>
                <th>Price</th>
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
        const ema20Class = t.vars_20ema >= 6 ? 'up' : t.vars_20ema < 2 ? 'dn' : '';
        const rsClass = t.rs >= 80 ? 'up' : t.rs <= 20 ? 'dn' : '';
        const instVal = parseFloat(String(t.inst).replace(/[+%]/g, ''));
        const instClass = isNaN(instVal) ? 'neu' : instVal > 0 ? 'up' : instVal < 0 ? 'dn' : 'neu';
        const shortVal = parseFloat(t.short);
        const shortClass = isNaN(shortVal) ? 'neu' : shortVal >= 20 ? 'up' : shortVal >= 10 ? 'short-blue' : 'short-white';
        html += `
                <tr>
                  <td class="l">
                    <span class="tn-link${t.ticker_color === 'green' ? ' day-pattern-green' : ''}" data-sym="${escAttr(t.ticker)}" data-nm="${escAttr(theme.name + ' · ' + t.ticker)}">
                      ${escHtml(t.ticker)}
                    </span>
                  </td>
                  <td class="${varsClass}">${(t.vars ?? 0).toFixed(2)}</td>
                  <td class="${ema20Class}">${(t.vars_20ema ?? 0).toFixed(2)}</td>
                  <td class="${rsClass}">${t.rs ?? '—'}</td>
                  <td>${t.price ?? '—'}</td>
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
        <tr>
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
      html += `
        <tr>
          <td class="l">
            <span class="tn-link${row.ticker_color === 'green' ? ' day-pattern-green' : ''}" data-sym="${escAttr(row.display_ticker || row.ticker)}" data-nm="${escAttr(row.name)}">${escHtml(row.ticker)}</span>
          </td>
          <td class="l" style="font-size:11px;color:var(--text2);max-width:220px;overflow:hidden;text-overflow:ellipsis">${escHtml(truncate(row.name, 40))}</td>
          <td class="${rsStsPctClass(row.rs_sts)}"><strong>${fmtPct(row.rs_sts)}</strong></td>
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
        monthly: parsePercent(r['Monthly %'])
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
      html += `
        <tr>
          <td class="l">
            <span class="tn-link${row.ticker_color === 'green' ? ' day-pattern-green' : ''}" data-sym="${escAttr(row.ticker)}" data-nm="${escAttr(row.name)}">${escHtml(row.ticker)}</span>
          </td>
          <td class="l" style="font-size:11px;color:var(--text2);max-width:220px;overflow:hidden;text-overflow:ellipsis">${escHtml(truncate(row.name, 40))}</td>
          <td class="${rsStsPctClass(row.rs_sts)}"><strong>${fmtPct(row.rs_sts)}</strong></td>
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

    return Object.values(byDate)
      .sort((a, b) => b.report_date.localeCompare(a.report_date))
      .slice(0, 5);
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
