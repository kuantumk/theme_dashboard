// ═══════════════════════════════════════════════════════
// MARKET MONITOR — App Logic V2.1
// ═══════════════════════════════════════════════════════

(function () {
  'use strict';

  // ── CONFIG ────────────────────────────────────────────
  const THEME_DATA_URL = 'data/themes.json';
  const BREADTH_DATA_URL = 'data/market_breadth.json';
  const MACRO_DATA_URL = 'data/macro_data.json';
  const INDUSTRY_ETF_URL = 'data/industry_etf.json';
  const META_URL = 'data/report_meta.json';
  const EVENTS_URL = 'data/events.json';
  const ETF_SHEET_URL = 'https://docs.google.com/spreadsheets/d/1zwmK5YnbBHyin0n0DHIEEydPapCkln1WCvlKv4IhwSg/export?format=csv&gid=1565194920';
  const ETF_FALLBACK_URL = 'data/etf_data.json';
  const INDUSTRY_SHEET_URL = 'https://docs.google.com/spreadsheets/d/1zwmK5YnbBHyin0n0DHIEEydPapCkln1WCvlKv4IhwSg/export?format=csv&gid=549753148';
  const PAGE_LOAD_CACHE_KEY = Date.now();

  // Active chart per tab
  let activeCharts = { macro: null, themes: null, industry: null, etf: null, ep: null };

  // Sort state per table
  let sortState = {
    etf: { column: 'rs_sts', dir: 'desc' },
    industry: { column: 'rs_sts', dir: 'desc' },
    ep: { column: 'float', dir: 'asc' },
  };
  let etfData = [];
  let industryData = [];
  let epData = [];

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
    loadIndustryETFData();
    loadETFData();
    loadMacroEvents();
    loadEPScannerData();
    initTableSort();
    // Auto-refresh EP data every 10 minutes
    setInterval(loadEPScannerData, 10 * 60 * 1000);
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
      const name = link.dataset.nm || sym;
      if (!sym) return;

      const tabContent = link.closest('.tab-content');
      if (!tabContent) return;

      let tabId;
      if (tabContent.id === 'content-macro') tabId = 'macro';
      else if (tabContent.id === 'content-themes') tabId = 'themes';
      else if (tabContent.id === 'content-industry') tabId = 'industry';
      else if (tabContent.id === 'content-etf') tabId = 'etf';
      else if (tabContent.id === 'content-ep') tabId = 'ep';
      else return;

      tabContent.querySelectorAll('.tn-link').forEach(l => l.classList.remove('active-ticker'));
      link.classList.add('active-ticker');

      openChart(tabId, sym, name);
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
    areaEl.innerHTML = `<div id="${containerId}" style="width:100%;height:100%"></div>`;

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
        "container_id": containerId,
        "hotlist": false,
        "details": false,
        "calendar": false,
        "hide_volume": true,
        "studies": [
          "STD;MA%Ribbon",
          "STD;Volume"
        ]
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

    activeCharts[tabId] = sym;
  }

  // Expose globally for inline onclick handlers to work (e.g. from themes tab)
  window.openChart = openChart;

  // ── ARROW KEY NAVIGATION ────────────────────────────────
  let navIndices = { macro: -1, themes: -1, industry: -1, etf: -1, ep: -1 };

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
      openChart(tabId, sym, name);
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
          const timeStr = ev.time || '00:00';
          const [hours, mins] = timeStr.split(':');

          // Convert event time from US Eastern to local timezone
          const isoStr = `${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}T${hours.padStart(2, '0')}:${mins.padStart(2, '0')}:00`;
          // Step 1: Find ET's UTC offset for this date
          // Format a known UTC instant in both ET and local, compare the difference
          const probe = new Date(`${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}T12:00:00Z`);
          const etParts = new Intl.DateTimeFormat('en-US', { timeZone: 'America/New_York', hour: 'numeric', minute: 'numeric', hour12: false, year: 'numeric', month: 'numeric', day: 'numeric' }).formatToParts(probe);
          const etH = parseInt(etParts.find(p => p.type === 'hour').value);
          const etM = parseInt(etParts.find(p => p.type === 'minute').value);
          // ET shows (etH:etM) when UTC is 12:00, so ET offset = (etH*60+etM) - 720 minutes
          const etOffsetMins = (etH * 60 + etM) - 720;
          // Step 2: event is at isoStr in ET, so UTC = isoStr - etOffset
          const eventUtcMs = new Date(isoStr + 'Z').getTime() - (etOffsetMins * 60000);
          const eventDate = new Date(eventUtcMs);

          const localDateStr = eventDate.toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' });
          const localTimeStr = ev.time ? eventDate.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', hour12: false }) : '';
          const displayDate = (localDateStr !== lastLocalDate) ? localDateStr : '';
          lastLocalDate = localDateStr;
          const dtDisplay = displayDate ? (displayDate + ' ' + localTimeStr) : localTimeStr;

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
                tr.onclick = () => openChart('macro', item.tv, item.name);
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

  // ── THEME DATA ────────────────────────────────────────
  function loadThemeData() {
    fetch(withCacheBust(THEME_DATA_URL))
      .then(r => r.json())
      .then(data => {
        renderThemes(data);
      })
      .catch(err => {
        console.warn('Theme data not available:', err);
        document.getElementById('themes-container').innerHTML =
          '<div class="no-data">Theme data not available.<br>Run the daily workflow to generate data.</div>';
      });
  }

  function renderThemes(data) {
    const container = document.getElementById('themes-container');

    if (!data.themes || data.themes.length === 0) {
      container.innerHTML = '<div class="no-data">No themes found for this date.</div>';
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
                    <span class="tn-link${t.day_pattern ? ' day-pattern' : ''}" data-sym="${escAttr(t.ticker)}" data-nm="${escAttr(theme.name + ' · ' + t.ticker)}">
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

  // ── INDUSTRY ETF DATA ─────────────────────────────────
  function loadIndustryETFData() {
    fetch(withCacheBust(INDUSTRY_ETF_URL))
      .then(r => {
        if (!r.ok) throw new Error('Not found');
        return r.json();
      })
      .then(data => {
        industryData = data;
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
      tbody.innerHTML = '<tr><td colspan="6" class="no-data">No industry ETF data available.</td></tr>';
      return;
    }

    let html = '';
    industryData.forEach(row => {
      html += `
        <tr>
          <td class="l">
            <span class="tn-link${row.day_pattern ? ' day-pattern' : ''}" data-sym="${escAttr(row.display_ticker || row.ticker)}" data-nm="${escAttr(row.name)}">${escHtml(row.ticker)}</span>
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
    fetch(withCacheBust(ETF_FALLBACK_URL))
      .then(r => {
        if (!r.ok) throw new Error('Not found');
        return r.json();
      })
      .then(data => {
        etfData = data;
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
      tbody.innerHTML = '<tr><td colspan="6" class="no-data">No ETF data available.</td></tr>';
      return;
    }

    let html = '';
    etfData.forEach(row => {
      html += `
        <tr>
          <td class="l">
            <span class="tn-link${row.day_pattern ? ' day-pattern' : ''}" data-sym="${escAttr(row.ticker)}" data-nm="${escAttr(row.name)}">${escHtml(row.ticker)}</span>
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
        else if (tab === 'ep') sortAndRenderEP();
        else sortAndRenderETF();
      });
    });
  }

  // ── EP SCANNER DATA ──────────────────────────────────────
  const EP_SCAN_URL = 'data/ep_scan.json';

  function loadEPScannerData() {
    fetch(withCacheBust(EP_SCAN_URL))
      .then(r => {
        if (!r.ok) throw new Error('HTTP ' + r.status);
        return r.json();
      })
      .then(data => {
        epData = data.tickers || [];
        sortAndRenderEP();

        // Update refresh badge
        const infoEl = document.getElementById('ep-refresh-info');
        if (infoEl && data.timestamp) {
          const dt = new Date(data.timestamp);
          infoEl.textContent = 'Updated: ' + dt.toLocaleDateString() + ' ' +
            dt.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
        }
      })
      .catch(() => {
        document.getElementById('ep-body').innerHTML =
          '<tr><td colspan="6" class="no-data">EP scan data not available.</td></tr>';
        const infoEl = document.getElementById('ep-refresh-info');
        if (infoEl) infoEl.textContent = 'No data';
      });
  }

  function sortAndRenderEP() {
    const s = sortState.ep;
    epData.sort((a, b) => {
      const av = a[s.column] ?? (s.dir === 'asc' ? Infinity : -Infinity);
      const bv = b[s.column] ?? (s.dir === 'asc' ? Infinity : -Infinity);
      return s.dir === 'desc' ? bv - av : av - bv;
    });
    renderEPTable();
  }

  function renderEPTable() {
    const tbody = document.getElementById('ep-body');
    if (!epData.length) {
      tbody.innerHTML = '<tr><td colspan="6" class="no-data">No EP scan results for today.</td></tr>';
      return;
    }

    let html = '';
    epData.forEach(row => {
      const floatClass = (row.float != null && row.float < 150) ? 'ep-float-green' : 'neu';
      const shortClass = epShortClass(row.short);
      const dist52wClass = epDist52wClass(row.dist_52w_high);
      const atrClass = epAtrClass(row.atr_multiple);

      const floatStr = row.float != null ? row.float.toFixed(1) + 'M' : '—';
      const shortStr = row.short != null ? row.short.toFixed(1) + '%' : '—';
      const dist52wStr = row.dist_52w_high != null
        ? (row.dist_52w_high > 0 ? '+' : '') + row.dist_52w_high.toFixed(1) + '%'
        : '—';
      const atrStr = row.atr_multiple != null ? row.atr_multiple.toFixed(1) + '×' : '—';
      const ahChgStr = row.ah_chg_pct != null
        ? (row.ah_chg_pct > 0 ? '+' : '') + row.ah_chg_pct.toFixed(2) + '%'
        : '—';
      const ahChgClass = pctClass(row.ah_chg_pct);

      html += `
        <tr>
          <td class="l">
            <span class="tn-link" data-sym="${escAttr(row.ticker)}" data-nm="${escAttr(row.ticker + ' · EP Scan')}">${escHtml(row.ticker)}</span>
          </td>
          <td class="${floatClass}">${floatStr}</td>
          <td class="${shortClass}">${shortStr}</td>
          <td class="${dist52wClass}">${dist52wStr}</td>
          <td class="${atrClass}">${atrStr}</td>
          <td class="${ahChgClass}">${ahChgStr}</td>
        </tr>
      `;
    });
    tbody.innerHTML = html;
  }

  // EP color helpers
  function epShortClass(val) {
    if (val == null) return 'neu';
    if (val > 20) return 'up';
    if (val > 10) return 'short-blue';
    return 'neu';
  }

  function epDist52wClass(val) {
    // val is % distance: (ah_price - 52w_high) / 52w_high * 100
    // green if better than -10% (i.e., val > -10)
    if (val == null) return 'neu';
    return val > -10 ? 'up' : 'neu';
  }

  function epAtrClass(val) {
    // ATR multiple: green <5, blue <7, normal <9, red >=10
    if (val == null) return 'neu';
    if (val < 5) return 'up';
    if (val < 7) return 'short-blue';
    if (val < 9) return 'neu';
    return 'dn';
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
