// ═══════════════════════════════════════════════════════
// BID/ASK TAPE PRESSURE — client
// Renders from a single state payload the server rewrites every poll. All
// filtering happens here against the full payload, so the controls are instant
// and never trigger a refetch.
// ═══════════════════════════════════════════════════════

(function () {
  'use strict';

  const STATE_URL = 'state.json';

  let market = 'crypto';
  let state = null;
  let timer = null;

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
  };

  // Feed-sourced strings (industry labels, symbols) come from an external
  // vendor, unlike the repo-controlled taxonomy. Escape before they touch markup.
  function esc(value) {
    return String(value == null ? '' : value)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }

  function filters() {
    return {
      minHits: parseInt(els.minHits.value, 10) || 1,
      hideUncertain: els.hideUncertain.checked,
      hideDivergent: els.hideDivergent.checked,
    };
  }

  function keepMember(m, f) {
    if (m.total_hits < f.minHits) return false;
    if (f.hideDivergent && m.divergent) return false;
    // "Low confidence" means most of this ticker's observations fired the
    // quote-drift override, where the quote and tick rules disagreed.
    if (f.hideUncertain && m.total_hits > 0 && m.uncertain / m.total_hits > 0.5) return false;
    return true;
  }

  function renderColumn(groups, container, side) {
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
      container.innerHTML = '<div class="empty">No tickers above the current thresholds yet.</div>';
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
    els.feed.textContent = view.error ? `feed error: ${esc(view.error)}`
      : (view.delayed ? 'delayed feed' : 'live');
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
  }

  function render() {
    if (!state) return;
    const view = state[market];
    if (!view) return;
    renderStatus(view);
    renderPressure(view);
    // Preserve scroll across the poll-cadence re-render: this is the first
    // surface in the project that re-renders while being actively read.
    const y = window.scrollY;
    renderColumn(view.columns && view.columns.strong, els.strong, 'strong');
    renderColumn(view.columns && view.columns.weak, els.weak, 'weak');
    window.scrollTo(0, y);
  }

  function refresh() {
    fetch(STATE_URL + '?t=' + Date.now())
      .then(r => (r.ok ? r.json() : Promise.reject(new Error('HTTP ' + r.status))))
      .then(data => { state = data; render(); })
      .catch(() => {
        els.feed.textContent = 'server unreachable';
        els.feed.className = 'pill error';
      });
  }

  function schedule() {
    if (timer) clearInterval(timer);
    const seconds = (state && state.poll_seconds) || 10;
    timer = setInterval(refresh, seconds * 1000);
  }

  document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      market = btn.dataset.market;
      render();
    });
  });

  els.minHits.addEventListener('input', () => {
    els.minHitsVal.textContent = els.minHits.value;
    render();
  });
  els.hideUncertain.addEventListener('change', render);
  els.hideDivergent.addEventListener('change', render);

  refresh();
  setTimeout(schedule, 1000);
})();
