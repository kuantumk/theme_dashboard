// ═══════════════════════════════════════════════════════
// BID/ASK TAPE PRESSURE — client
// Renders from a single state payload the server rewrites every poll. All
// filtering happens here against the full payload, so the controls are instant
// and never trigger a refetch.
// ═══════════════════════════════════════════════════════

(function () {
  'use strict';

  const STATE_URL = 'state.json';

  const CADENCE_URL = 'cadence';

  let market = 'equity';
  let state = null;
  let timer = null;
  let cadence = null;      // seconds; mirrors the server's live value
  let pickedTab = false;   // user has chosen a tab, so stop auto-selecting

  const els = {
    strong: document.getElementById('strong-body'),
    weak: document.getElementById('weak-body'),
    feed: document.getElementById('feed-pill'),
    coverage: document.getElementById('coverage-pill'),
    polls: document.getElementById('polls-pill'),
    scan: document.getElementById('scan-pill'),
    buyLabel: document.getElementById('buy-label'),
    sellLabel: document.getElementById('sell-label'),
    fill: document.getElementById('pressure-fill'),
    minHits: document.getElementById('min-hits'),
    minHitsVal: document.getElementById('min-hits-val'),
    hideUncertain: document.getElementById('hide-uncertain'),
    hideDivergent: document.getElementById('hide-divergent'),
    cadence: document.getElementById('cadence'),
    minDollarVol: document.getElementById('min-dollar-vol'),
    minDollarVolVal: document.getElementById('min-dollar-vol-val'),
    minVolume: document.getElementById('min-volume'),
    minVolumeVal: document.getElementById('min-volume-val'),
    market: document.getElementById('market-pill'),
    quotes: document.getElementById('quotes-pill'),
  };

  // Liquidity spans orders of magnitude, so a linear slider would spend most of
  // its travel in a range nobody filters on. Each slider step is a tenth of a
  // decade above $1K/1K shares, which makes round values land on integer
  // positions: $1M is exactly position 30, 1M shares exactly position 30.
  const LOG_BASE = 1e3;

  function sliderToValue(pos) {
    return LOG_BASE * Math.pow(10, pos / 10);
  }

  function compact(n) {
    const abs = Math.abs(n);
    if (abs >= 1e9) return (n / 1e9).toFixed(abs >= 1e10 ? 0 : 1) + 'B';
    if (abs >= 1e6) return (n / 1e6).toFixed(abs >= 1e7 ? 0 : 1) + 'M';
    if (abs >= 1e3) return (n / 1e3).toFixed(abs >= 1e4 ? 0 : 1) + 'K';
    return String(Math.round(n));
  }

  function liquidityFloors() {
    return {
      dollar: sliderToValue(parseInt(els.minDollarVol.value, 10)),
      volume: sliderToValue(parseInt(els.minVolume.value, 10)),
    };
  }

  function syncLiquidityLabels() {
    const f = liquidityFloors();
    els.minDollarVolVal.textContent = '$' + compact(f.dollar);
    els.minVolumeVal.textContent = compact(f.volume);
  }

  function tickerCount(view) {
    if (!view || !view.columns) return 0;
    return ['strong', 'weak'].reduce((total, side) =>
      total + (view.columns[side] || []).reduce((n, g) => n + (g.members || []).length, 0), 0);
  }

  // Equity is empty outside US market hours, so landing there shows nothing and
  // looks broken. Switch to whichever tab actually has data until the user picks
  // one, then respect their choice.
  function autoSelectTab() {
    if (pickedTab || !state) return;
    if (tickerCount(state[market])) { pickedTab = true; return; }
    const other = market === 'equity' ? 'crypto' : 'equity';
    if (tickerCount(state[other])) {
      market = other;
      document.querySelectorAll('.tab-btn').forEach(b =>
        b.classList.toggle('active', b.dataset.market === market));
    }
  }

  // Feed-sourced strings (industry labels, symbols) come from an external
  // vendor, unlike the repo-controlled taxonomy. Escape before they touch markup.
  function esc(value) {
    return String(value == null ? '' : value)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }

  function filters() {
    const liq = liquidityFloors();
    return {
      minHits: parseInt(els.minHits.value, 10) || 1,
      hideUncertain: els.hideUncertain.checked,
      hideDivergent: els.hideDivergent.checked,
      minDollarVol: liq.dollar,
      minVolume: liq.volume,
    };
  }

  function keepMember(m, f) {
    if (m.total_hits < f.minHits) return false;
    if (f.hideDivergent && m.divergent) return false;
    // "Low confidence" means most of this ticker's observations fired the
    // quote-drift override, where the quote and tick rules disagreed.
    if (f.hideUncertain && m.total_hits > 0 && m.uncertain / m.total_hits > 0.5) return false;
    // Liquidity floors. A ticker missing the metric is dropped rather than let
    // through: these filters exist to guarantee tradeable names, so an unknown
    // must not pass as if it qualified.
    if (!(typeof m.dollar_vol === 'number' && m.dollar_vol >= f.minDollarVol)) return false;
    if (!(typeof m.volume === 'number' && m.volume >= f.minVolume)) return false;
    return true;
  }

  // An empty column has several possible causes and they are not interchangeable.
  // The default text blames the user's thresholds; when the real cause is that no
  // quotes are arriving, saying so is the difference between a two-minute fix and
  // a lost session. US equity bid/ask comes from the quote socket, because the
  // screener API carries no such field for that market.
  function emptyReason(view) {
    if (!view) return 'Waiting for the first scans…';
    if (view.error) return `Feed error: ${esc(view.error)}. Nothing can be classified until it clears.`;
    const q = view.quotes;
    if (q) {
      if (!q.connected) {
        return `Quote stream disconnected${q.error ? ` (${esc(q.error)})` : ''} — reconnecting. `
          + 'US equity bid/ask comes from the quote socket, so nothing classifies while it is down.';
      }
      if (q.subscribed > 0 && !q.merged) {
        return 'Quote stream connected but no symbol is quoting yet. '
          + 'If this persists, the session cookie in .env has probably expired.';
      }
    }
    const stats = view.stats || {};
    if (stats.polls < 2) return 'Waiting for the first scans…';
    if (!stats.classified) {
      const top = Object.entries(stats.rejections || {}).sort((a, b) => b[1] - a[1])[0];
      return top
        ? `No observation has classified yet — every one was rejected as "${esc(top[0])}".`
        : 'No observation has classified yet.';
    }
    return 'No tickers above the current thresholds yet.';
  }

  function renderColumn(groups, container, side, emptyMsg) {
    const f = filters();
    const kept = [];
    (groups || []).forEach(group => {
      const members = (group.members || []).filter(m => keepMember(m, f));
      if (members.length) {
        const score = members.reduce((sum, m) => sum + m.margin, 0);
        kept.push({ name: group.name, origin: group.origin, score, members });
      }
    });
    // Re-sort after filtering so the displayed score, not the unfiltered one,
    // decides the order. Group totals always equal the sum of visible members.
    kept.sort((a, b) => (side === 'strong' ? b.score - a.score : a.score - b.score));

    if (!kept.length) {
      container.innerHTML = `<div class="empty">${emptyMsg || 'No tickers above the current thresholds yet.'}</div>`;
      return;
    }

    container.innerHTML = kept.map(group => {
      const chips = group.members.map(m => {
        const badge = m.badge
          ? `<span class="badge ${m.badge.direction === 'low' ? 'low' : ''}">${esc(m.badge.label)}</span>`
          : '';
        const warn = m.divergent ? '<span class="warn" title="count and volume signals disagree">!</span>' : '';
        const hits = side === 'strong'
          ? `<b>${m.ask_hits}</b>/${m.bid_hits}`
          : `${m.ask_hits}/<b>${m.bid_hits}</b>`;
        return `<span class="chip ${m.divergent ? 'divergent' : ''}" title="${esc(m.symbol)} — ${m.ask_hits} ask-side, ${m.bid_hits} bid-side, imbalance ${m.imbalance}">
            <span class="sym">${esc(m.symbol)}</span>
            <span class="hits">${hits}</span>${badge}${warn}
          </span>`;
      }).join('');
      const nameClass = group.origin === 'industry' ? 'group-name industry' : 'group-name';
      return `<div class="group">
          <div class="group-head">
            <span class="${nameClass}">${esc(group.name)}</span>
            <span class="group-score">${group.score > 0 ? '+' : ''}${group.score}</span>
          </div>
          <div class="chips">${chips}</div>
        </div>`;
    }).join('');
  }

  function renderPressure(view) {
    const buy = (view && view.ask_side) || 0;
    const sell = (view && view.bid_side) || 0;
    const total = buy + sell;
    const pct = total ? (buy / total) * 100 : 50;
    els.fill.style.width = pct.toFixed(1) + '%';
    els.buyLabel.textContent = `Buying ${total ? pct.toFixed(0) : '—'}% (${buy})`;
    els.sellLabel.textContent = `(${sell}) ${total ? (100 - pct).toFixed(0) : '—'}% Selling`;
  }

  function renderStatus(view) {
    if (!view) return;
    // Market state and feed state are different questions: a real-time
    // entitlement on a closed market is still a closed market.
    const status = view.market_status || 'unknown';
    const open = status === 'market open' || status === '24/7';
    els.market.textContent = status;
    els.market.className = 'pill ' + (open ? 'live' : 'delayed');

    els.feed.textContent = view.error ? `feed error: ${esc(view.error)}`
      : (view.delayed ? 'delayed feed' : 'real-time feed');
    els.feed.className = 'pill ' + (view.error ? 'error' : (view.delayed ? 'delayed' : 'live'));
    // Coverage is classification quality (share of actual trades classified),
    // not trade frequency — most symbols do not print every interval.
    const cov = view.stats && typeof view.stats.coverage === 'number'
      ? (view.stats.coverage * 100).toFixed(0) + '%' : '—';
    const rate = view.stats && typeof view.stats.trade_rate === 'number'
      ? (view.stats.trade_rate * 100).toFixed(0) + '%' : '—';
    els.coverage.textContent = `classified ${cov} · traded ${rate}`;
    els.coverage.title = 'share of actual trades classified · share of scans where a trade printed';
    els.polls.textContent = `${(view.stats && view.stats.polls) || 0} scans`;
    els.scan.textContent = view.scanned_at || '—';

    // Only equities carry a quote socket; the crypto screener serves bid/ask
    // directly, so the pill would be meaningless on that tab.
    const q = view.quotes;
    els.quotes.hidden = !q;
    if (q) {
      const healthy = q.connected && q.merged > 0;
      els.quotes.textContent = q.connected
        ? `quotes ${q.merged}/${q.subscribed}`
        : `quotes offline${q.error ? ` (${q.error})` : ''}`;
      els.quotes.title = 'symbols with a live two-sided quote / symbols subscribed';
      els.quotes.className = 'pill ' + (healthy ? 'live' : 'error');
    }
  }

  function render() {
    if (!state) return;
    autoSelectTab();
    const view = state[market];
    if (!view) return;
    renderStatus(view);
    renderPressure(view);
    // Preserve scroll across the poll-cadence re-render: this is the first
    // surface in the project that re-renders while being actively read.
    const y = window.scrollY;
    const reason = emptyReason(view);
    renderColumn(view.columns && view.columns.strong, els.strong, 'strong', reason);
    renderColumn(view.columns && view.columns.weak, els.weak, 'weak', reason);
    window.scrollTo(0, y);
  }

  function refresh() {
    fetch(STATE_URL + '?t=' + Date.now())
      .then(r => (r.ok ? r.json() : Promise.reject(new Error('HTTP ' + r.status))))
      .then(data => {
        state = data;
        // Follow the server's live cadence: it is authoritative (it clamps to
        // configured bounds) and another tab may have changed it.
        if (state.poll_seconds && state.poll_seconds !== cadence) {
          cadence = state.poll_seconds;
          els.cadence.value = String(cadence);
          schedule();
        }
        render();
      })
      .catch(() => {
        els.feed.textContent = 'server unreachable';
        els.feed.className = 'pill error';
      });
  }

  function schedule() {
    if (timer) clearInterval(timer);
    timer = setInterval(refresh, (cadence || 10) * 1000);
  }

  function setCadence(seconds) {
    fetch(CADENCE_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ seconds: seconds }),
    })
      .then(r => (r.ok ? r.json() : Promise.reject(new Error('HTTP ' + r.status))))
      .then(data => {
        // The server clamps to configured bounds, so echo back what it applied
        // rather than what was asked for.
        cadence = data.poll_seconds;
        els.cadence.value = String(cadence);
        schedule();
      })
      .catch(() => { els.cadence.value = String(cadence || 10); });
  }

  document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      market = btn.dataset.market;
      pickedTab = true;
      render();
    });
  });

  els.cadence.addEventListener('change', () => setCadence(parseInt(els.cadence.value, 10)));

  els.minHits.addEventListener('input', () => {
    els.minHitsVal.textContent = els.minHits.value;
    render();
  });
  els.hideUncertain.addEventListener('change', render);
  els.hideDivergent.addEventListener('change', render);

  [els.minDollarVol, els.minVolume].forEach(slider => {
    slider.addEventListener('input', () => { syncLiquidityLabels(); render(); });
  });
  syncLiquidityLabels();

  refresh();
  setTimeout(schedule, 1000);
})();
