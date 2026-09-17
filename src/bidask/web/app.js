// ═══════════════════════════════════════════════════════
// TAPE PRESSURE — client
// Renders from a single state payload the server rewrites every poll. All
// filtering happens here against the full payload, so the controls are instant
// and never trigger a refetch.
//
// The board is stateless. Each poll asks two questions of every row — where its
// price sits against a session-appropriate reference, and whether it is trading
// on unusual volume for this time of day — and nothing on this page remembers
// an earlier poll. There are no hit counts, no trailing window and no
// accumulator, so every surface that reported one is gone rather than left
// reading a field the payload no longer carries.
// ═══════════════════════════════════════════════════════

(function () {
  'use strict';

  const STATE_URL = 'state.json';

  const CADENCE_URL = 'cadence';

  // Mirrors `RVOL_FIELD` and `SIDES_FIELD` in `src/bidask/grouping.py`. Named
  // rather than inlined so both joins to the payload are greppable from here.
  const RVOL_FIELD = 'rvol_at_time';
  const SIDES_FIELD = 'sides';

  // KTD5's scoring constants, mirroring `bidask.group_rvol_cap` and
  // `grouping.TOP_MEMBERS`. The cap is not optional: one row read 2090.8x in
  // the measured session, and an uncapped mean hands its theme a score three
  // orders of magnitude above every rival, so the ranking stops responding to
  // anything else. The payload carries neither value, so these are a copy —
  // `tests/test_bidask_column_meta_markup.py` pins them against the Python.
  const RVOL_CAP = 5.0;
  const TOP_MEMBERS = 3;

  // Which change field answers each reference, per session state. Mirrors
  // `REFERENCE_FIELDS` in `src/bidask/session_state.py` and the single pair in
  // `src/bidask/crypto_state.py`. Keyed by state because one reference name has
  // two fields behind it: `prev close` is answered by `change` during the
  // session and by `premarket_change` before the bell, and reading the wrong
  // one prints yesterday's move beside this morning's.
  const MOVE_FIELDS = {
    market: { 'open': 'change_from_open', 'prev close': 'change' },
    pre_market: { 'prev close': 'premarket_change' },
    post_market: { 'session close': 'postmarket_change' },
    crypto: { '24h ago': 'change_pct' },
  };

  // The session state that selected the floor, named for a reader. Distinct
  // from the market-status pill, which reports whether the market is trading:
  // a real-time entitlement on a closed market is still a closed market, and
  // the floor follows the state rather than the entitlement.
  const STATE_LABELS = {
    market: 'regular session',
    pre_market: 'pre-market',
    post_market: 'after hours',
    closed: 'closed',
    crypto: '24/7',
  };

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
    floor: document.getElementById('floor-pill'),
    reference: document.getElementById('reference-pill'),
    scan: document.getElementById('scan-pill'),
    cadence: document.getElementById('cadence'),
    minDollarVol: document.getElementById('min-dollar-vol'),
    minDollarVolVal: document.getElementById('min-dollar-vol-val'),
    minVolume: document.getElementById('min-volume'),
    minVolumeVal: document.getElementById('min-volume-val'),
    market: document.getElementById('market-pill'),
    strongMeta: document.getElementById('strong-meta'),
    weakMeta: document.getElementById('weak-meta'),
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

  function num(value) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }

  // Relative volume, as a trader reads it off a chart. Kept to one decimal
  // below 10x, none above, and compacted past 100x so the 2090x tail does not
  // widen every chip in its column.
  function fmtRvol(value) {
    const v = num(value);
    if (v === null || v <= 0) return '—';
    if (v >= 100) return compact(v) + 'x';
    return v.toFixed(v < 10 ? 1 : 0) + 'x';
  }

  function fmtMove(value) {
    const v = num(value);
    if (v === null) return '';
    return (v > 0 ? '+' : '') + v.toFixed(1) + '%';
  }

  function stateLabel(value) {
    const key = String(value == null ? '' : value);
    return STATE_LABELS[key] || (key ? key.replace(/_/g, ' ') : 'unknown state');
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
    return { minDollarVol: liq.dollar, minVolume: liq.volume };
  }

  // The server's relative-volume gate decides what reaches a column. These two
  // sliders are the reader's own view cut on top of it, and nothing else
  // survives: the old hit, confidence and divergence controls read counters the
  // stateless board never produces, so they sat on screen responding to nothing.
  function keepMember(m, f) {
    // A ticker missing the metric is dropped rather than let through: these
    // filters exist to guarantee tradeable names, so an unknown must not pass
    // as if it qualified.
    if (!(typeof m.dollar_vol === 'number' && m.dollar_vol >= f.minDollarVol)) return false;
    if (!(typeof m.volume === 'number' && m.volume >= f.minVolume)) return false;
    return true;
  }

  // ── scoring ───────────────────────────────────────────
  // KTD5, recomputed after the sliders. The server scores the whole group; once
  // a slider removes a member, that number describes a roster no longer on
  // screen and the sort would order the board by rows the reader cannot see.

  function memberValue(m) {
    const v = num(m && m[RVOL_FIELD]);
    return v !== null && v > 0 ? v : 0;
  }

  // Mean of the top three, each capped BEFORE the mean. Capping the mean
  // instead is a different operation: it lets one extreme member carry two
  // quiet ones to the ceiling.
  function intensity(members) {
    const values = members.map(m => Math.min(memberValue(m), RVOL_CAP))
      .sort((a, b) => b - a)
      .slice(0, TOP_MEMBERS);
    if (!values.length) return 0;
    return values.reduce((sum, v) => sum + v, 0) / values.length;
  }

  // The breadth half is recovered from the published score rather than
  // recomputed, because the payload carries neither the group's pre-gate roster
  // nor the coefficient — see the report accompanying this change. The residual
  // is exact for an unfiltered group, so a board with the sliders wide open
  // shows precisely the numbers the server ranked on. Once members are filtered
  // out the residual is scaled by the share that survived, which is an
  // approximation of a term bounded by its own coefficient (0.5) against an
  // intensity half spanning 0 to 5.
  function groupScore(all, visible, published) {
    const full = intensity(all);
    const score = num(published);
    const breadth = score === null ? 0 : Math.max(0, score - full);
    const share = all.length ? visible.length / all.length : 0;
    return intensity(visible) + breadth * share;
  }

  // ── honesty ───────────────────────────────────────────

  // An empty column has several possible causes and they are not
  // interchangeable. Relative volume is the ONLY admission path, so a board
  // with no readings is not a board with nothing to show — and the default
  // "nothing qualified" text blames a quiet market for a broken source. That
  // ambiguity is the failure this redesign was written around, so each cause
  // gets its own sentence and the order below is the order of confidence.
  function emptyReason(view) {
    if (!view) return 'Waiting for the first scan…';
    if (view.error) {
      return `Feed error: ${esc(view.error)}. Nothing can be scored until it clears.`;
    }
    // The screener answered and our own liquidity floors then removed every
    // row. Tested before the relative-volume branches on purpose: with no
    // universe there is nothing to score, so those branches would blame the
    // volume source for a field change upstream of it. A 100% drop is upstream
    // breakage, not a quiet market — `Value.Traded` cost a full session because
    // nothing said so.
    if (view.matched > 0 && !view.universe) {
      return `The screener matched ${view.matched} rows and every one failed the local `
        + 'liquidity floors. A 100% drop is almost always an upstream field change, not a '
        + 'quiet market — check that the columns the filters read are still served.';
    }
    const r = view.rvol || {};
    const status = String(r.status || '');
    // Warm-up first: it is the one cause that clears itself, and reporting it
    // as a broken source sends the reader after a vendor that is working.
    if (status.indexOf('pending') === 0 && !r.tickers) {
      return 'Relative-volume baselines are still warming up. Relative volume is the only way '
        + 'onto this board, so the columns stay empty until the download finishes — this is a '
        + 'warm-up in progress, not a quiet market.';
    }
    if (status.indexOf('failed') === 0) {
      return `Relative-volume baselines failed to build (${esc(status)}). Relative volume is the `
        + 'only way onto this board, so the columns stay empty until a rebuild succeeds.';
    }
    if (r.unavailable) {
      return `Relative-volume source unavailable — ${esc(r.reason || 'no row could be scored')}. `
        + 'A source that answered nothing is not a market that did nothing.';
    }
    if (r.floor == null) {
      return `The board is shut — the session state reads ${esc(stateLabel(r.session_state))}, `
        + 'which carries no volume floor, so no ticker can be admitted. Nothing here is a '
        + 'reading about the market.';
    }
    if (!r.polled) return 'Waiting for the first scan…';
    return `No ticker cleared the ${fmtRvol(r.floor)} volume floor for ${esc(stateLabel(r.session_state))}. `
      + `${r.scored} of ${r.polled} rows carried a usable relative volume, so the source is `
      + 'working and the market is quiet.';
  }

  // The server caps each column at a fixed number of rows, so most of the board
  // never reaches the page — measured 2026-08-14, the strong column rendered 13
  // of 124 themes and 111 of 367 in-play tickers. Saying nothing made a theme
  // that was genuinely bid look identical to one nobody was tracking. The
  // server publishes only the totals; the shown half is counted here, after the
  // sliders above, because a count sent from the server would be pre-slider and
  // would disagree with what is on screen.
  function renderColumnMeta(el, kept, meta) {
    if (!el) return;
    if (!meta || !meta.groups_total) {
      // Reset the class too. The tabs share one element, so a crypto view left
      // the amber "hiding" state on an empty label after an equity render.
      el.textContent = '';
      el.className = 'column-meta';
      el.title = '';
      return;
    }
    const groups = kept.length;
    const tickers = kept.reduce((n, g) => n + g.members.length, 0);
    const hiding = groups < meta.groups_total || tickers < meta.tickers_total;
    el.textContent = hiding
      ? `${groups}/${meta.groups_total} themes · ${tickers}/${meta.tickers_total} tickers`
      : `${groups} themes · ${tickers} tickers`;
    el.className = 'column-meta' + (hiding ? ' hiding' : '');
    el.title = hiding
      ? 'Shown / admitted this poll. The column is capped, and themes rank by the MEAN '
        + 'relative volume of their top three members plus a breadth bonus — so a small theme '
        + 'whose members are all running hot can outrank a large quiet one. Loosen the sliders '
        + 'to see more.'
      : 'Every theme that cleared the volume floor is on screen.';
  }

  // R5: name the reference that put this ticker on this side. A tint cannot
  // carry a name, and with two references live in the regular session the
  // answer is not inferable from the column — a stock above today's open and
  // below yesterday's close appears in BOTH columns, and the label is the only
  // thing that says which comparison each appearance came from.
  function referenceMarks(m, side, sessionState) {
    const sides = (m && m[SIDES_FIELD]) || {};
    const references = sides[side] || [];
    const table = MOVE_FIELDS[sessionState] || {};
    return references.map(reference => ({
      reference: reference,
      // A reference whose change field is missing renders as a bare name. An
      // absent move is never filled in.
      move: fmtMove(m[table[reference]]),
    }));
  }

  function renderChip(m, side, sessionState) {
    const badge = m.badge
      ? `<span class="badge ${m.badge.direction === 'low' ? 'low' : ''}">${esc(m.badge.label)}</span>`
      : '';
    const marks = referenceMarks(m, side, sessionState);
    const markup = marks.map(mark =>
      `<span class="mark"><span class="ref">${esc(mark.reference)}</span>`
      + (mark.move ? `<span class="move">${esc(mark.move)}</span>` : '')
      + '</span>').join('');
    const spoken = marks.map(mark =>
      `${side === 'strong' ? 'above' : 'below'} ${mark.reference}`
      + (mark.move ? ` ${mark.move}` : '')).join(', ');
    const title = `${m.symbol} — ${fmtRvol(m[RVOL_FIELD])} its usual volume by this time of day`
      + (spoken ? `; ${spoken}` : '');
    return `<span class="chip" title="${esc(title)}">
        <span class="sym">${esc(m.symbol)}</span>
        <span class="rvol">${esc(fmtRvol(m[RVOL_FIELD]))}</span>
        ${markup}${badge}
      </span>`;
  }

  function renderColumn(groups, container, side, emptyMsg, meta, metaEl, sessionState) {
    const f = filters();
    const kept = [];
    (groups || []).forEach(group => {
      const all = group.members || [];
      const members = all.filter(m => keepMember(m, f));
      if (members.length) {
        kept.push({
          name: group.name,
          origin: group.origin,
          score: groupScore(all, members, group.score),
          members: members,
        });
      }
    });
    // Both columns descend. The score is a capped relative volume plus a term
    // that only adds, so it is never negative — an ascending weak column would
    // lead with the quietest theme and spend its whole budget before reaching
    // the ones being distributed. Ties break on name, as the server's do, so
    // the order is stable across polls.
    kept.sort((a, b) => (b.score - a.score) || a.name.localeCompare(b.name));
    renderColumnMeta(metaEl, kept, meta);

    if (!kept.length) {
      // The server admitted rows and the reader's own sliders then removed
      // them. Saying "no ticker cleared the volume floor" there would blame the
      // feed for a control on this page.
      const admitted = (groups || []).some(g => (g.members || []).length);
      const msg = admitted
        ? 'Every ticker on this side is below the liquidity sliders above. The volume floor '
          + 'admitted them; this cut is yours.'
        : (emptyMsg || 'Nothing on this side yet.');
      container.innerHTML = `<div class="empty">${msg}</div>`;
      return;
    }

    container.innerHTML = kept.map(group => {
      const chips = group.members.map(m => renderChip(m, side, sessionState)).join('');
      const nameClass = group.origin === 'industry' ? 'group-name industry' : 'group-name';
      return `<div class="group">
          <div class="group-head" title="Mean relative volume of the top ${TOP_MEMBERS} members, each capped at ${RVOL_CAP}x, plus a bonus for the share of the theme that qualified.">
            <span class="${nameClass}">${esc(group.name)}</span>
            <span class="group-score">${group.score.toFixed(2)}</span>
          </div>
          <div class="chips">${chips}</div>
        </div>`;
    }).join('');
  }

  function renderStatus(view) {
    if (!view) return;
    // Market state and feed state are different questions: a real-time
    // entitlement on a closed market is still a closed market.
    const status = view.market_status || 'unknown';
    const open = status === 'market open' || status === '24/7';
    els.market.textContent = status;
    els.market.className = 'pill ' + (open ? 'live' : 'delayed');

    // An absent reading must not assert a stale vendor. `feed` is read off the
    // rows, so a response that carried none leaves it empty — and calling that
    // "delayed feed" is what accused a streaming vendor during the outage.
    // `delayed` stays true either way, so the styling and the never-claim-live
    // fail-safe are untouched; only the wording stops overclaiming.
    els.feed.textContent = view.error ? `feed error: ${esc(view.error)}`
      : (view.feed ? (view.delayed ? 'delayed feed' : 'real-time feed')
                   : 'feed unknown');
    els.feed.className = 'pill ' + (view.error ? 'error' : (view.delayed ? 'delayed' : 'live'));

    const r = view.rvol || {};

    // Coverage: rows carrying a usable relative-volume reading over rows
    // polled. Rendered EVERY poll rather than only when a column empties.
    // Partial nulls are routine on this feed, and relative volume is the only
    // admission path — without this pill a board that quietly shrank to a
    // third of the market reads as a calm one.
    const polled = num(r.polled) || 0;
    const scored = num(r.scored) || 0;
    els.coverage.textContent = polled ? `rvol ${scored}/${polled}` : 'rvol —';
    els.coverage.title = 'Rows with a usable Relative Volume at Time reading, over rows polled. '
      + 'Relative volume is the only way onto the board, so this is how much of the market it '
      + 'can judge at all.';
    els.coverage.className = 'pill'
      + (!polled ? '' : (!scored ? ' error' : (scored * 2 < polled ? ' delayed' : ' live')));

    // The floor in force and the state that selected it. The regular session
    // steps 0.7 → 1.0 → 1.2 through its first hour, so a block of tickers can
    // leave the board with the market unchanged and nothing else on screen to
    // explain it.
    els.floor.textContent = r.floor == null
      ? `no floor · ${stateLabel(r.session_state)}`
      : `floor ${fmtRvol(r.floor)} · ${stateLabel(r.session_state)}`;
    els.floor.title = 'The Relative Volume at Time a ticker must clear to reach a column, and '
      + 'the session state that selected it. Regular-session floors step 0.7 at the open, 1.0 at '
      + '15 minutes and 1.2 from 30 minutes on; pre-market is 3.0, after hours 1.5, crypto a '
      + 'flat 1.2. A state with no floor admits nothing.';
    els.floor.className = 'pill' + (r.floor == null ? ' delayed' : '');

    // R15. The crypto tab measures price against its own 24-hour reference on a
    // UTC clock; unlabelled, its number reads as the equity session measure
    // taken at a strange hour. The two tabs share a ratio whose anchor differs
    // by five hours, so the anchor is named on both.
    els.reference.textContent = (r.reference ? `vs ${r.reference}` : 'refs per ticker')
      + ` · vol from ${r.anchor || '—'}`;
    els.reference.title = r.reference
      ? 'This market has no open and no close, so every side is measured against the price 24 '
        + 'hours ago, and the volume ratio counts from UTC midnight. Neither is the equity '
        + 'session measure.'
      : 'Each ticker carries the reference that placed it on its side — open, prev close or '
        + 'session close. The volume ratio counts from the 04:00 ET extended-session anchor.';

    els.scan.textContent = view.scanned_at || '—';
  }

  function render() {
    if (!state) return;
    autoSelectTab();
    const view = state[market];
    if (!view) return;
    renderStatus(view);
    // Preserve scroll across the poll-cadence re-render: this is the first
    // surface in the project that re-renders while being actively read.
    const y = window.scrollY;
    const reason = emptyReason(view);
    const cols = view.columns || {};
    const cut = cols.truncated || {};
    // One state for the whole response, as the server resolves it. Reading it
    // per row would let two tickers in one poll be labelled against different
    // references.
    const sessionState = (view.rvol && view.rvol.session_state) || '';
    renderColumn(cols.strong, els.strong, 'strong', reason, cut.strong, els.strongMeta, sessionState);
    renderColumn(cols.weak, els.weak, 'weak', reason, cut.weak, els.weakMeta, sessionState);
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

  [els.minDollarVol, els.minVolume].forEach(slider => {
    slider.addEventListener('input', () => { syncLiquidityLabels(); render(); });
  });
  syncLiquidityLabels();

  refresh();
  setTimeout(schedule, 1000);
})();
