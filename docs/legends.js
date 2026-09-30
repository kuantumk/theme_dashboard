// ═══════════════════════════════════════════════════════
// MARKET MONITOR — Legends tab
// ═══════════════════════════════════════════════════════
//
// One card per colour scheme on the dashboard: what the colour means, the rule
// that paints it, and a picture of a ticker that meets the rule.
//
// Nothing here restates a threshold. Every rule and every sample is read from
// `window.MM_COLORS`, which docs/app.js builds from the same tables its render
// sites call, and every sample is painted with the real CSS class. So:
//   - a retuned threshold in COLOR_BANDS moves the tab and this legend together;
//   - a retuned CSS colour repaints the samples here with no edit;
//   - a retuned server rule (PATTERN_RULES) redraws the candle checks, and a
//     check that stops passing shows a red ✗ on its own figure.
//
// ⛔ A NEW colour class needs a card here. tests/test_dashboard_legends.py
// fails when a class that paints a hue ships in style.css + app.js without
// being named in this file.

(function () {
  'use strict';

  // ── SMALL HELPERS ──────────────────────────────────────
  function esc(s) {
    return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;')
      .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }

  function num(v, d) {
    const r = Number(v.toFixed(d));
    return (Object.is(r, -0) ? 0 : r).toFixed(d);
  }

  function signed(v, d) {
    return (v > 0 ? '+' : '') + num(v, d);
  }

  function tabLabel(id) {
    const btn = document.getElementById('tab-' + id);
    return btn ? btn.textContent.trim() : id;
  }

  function whereTags(ids) {
    return '<footer class="lg-where"><span class="lg-where-lbl">Used on</span>' +
      ids.map(id => `<span class="lg-where-tag">${esc(tabLabel(id))}</span>`).join('') +
      '</footer>';
  }

  /* A two-column key: every sample sits in one fixed-width column, so the
     explanations start at one x down the list and across cards. */
  function keyList(rows) {
    return '<ul class="lg-keys">' + rows.map(([key, text]) =>
      `<li><span class="lg-key">${key}</span><span class="lg-key-text">${text}</span></li>`).join('') + '</ul>';
  }

  const OP_TEXT = { '>=': '≥', '>': '>', '<=': '≤', '<': '<' };

  // ── BAND TABLES → LEGEND ROWS ─────────────────────────
  // A band table is read top-down, first match wins. For the legend each row
  // needs (a) a plain condition and (b) a sample value that the REAL pickBand
  // sends to that row — so the sample is painted by the live rule, not by a
  // copy of it.

  function bandIndex(table, v) {
    const C = window.MM_COLORS;
    for (let i = 0; i < table.bands.length; i++) {
      const b = table.bands[i];
      if (C.pickBand({ bands: [b], otherwise: false, missing: false }, v) !== false) return i;
    }
    return -1;
  }

  /* The condition a row applies, with the earlier rows it sits behind folded
     in, e.g. "≥ 10 and < 20" for the second of two falling bands. */
  function bandCondition(table, i, fmt) {
    const b = table.bands[i];
    const upward = b.op === '>=' || b.op === '>';
    let text = `${OP_TEXT[b.op]} ${fmt(b.at)}`;
    for (let j = i - 1; j >= 0; j--) {
      const p = table.bands[j];
      const pUp = p.op === '>=' || p.op === '>';
      if (pUp !== upward) continue;
      const cap = upward
        ? (p.op === '>=' ? '<' : '≤')
        : (p.op === '<' ? '≥' : '>');
      text += ` and ${cap} ${fmt(p.at)}`;
      break;
    }
    return text;
  }

  function sampleValues(table) {
    const ths = table.bands.map(b => b.at).filter(t => typeof t === 'number');
    const sorted = [...new Set(ths)].sort((a, b) => a - b);
    const lo = sorted[0], hi = sorted[sorted.length - 1];
    const scale = Math.max(hi - lo, Math.abs(hi), Math.abs(lo), 4);
    const cands = new Set();
    const eps = scale * 0.05;
    sorted.forEach((t, k) => {
      [t - eps, t, t + eps].forEach(v => cands.add(v));
      if (k + 1 < sorted.length) cands.add((t + sorted[k + 1]) / 2);
    });
    cands.add(lo - scale * 0.25);
    cands.add(hi + scale * 0.25);
    let vals = [...cands].map(v => Math.round(v * 100) / 100);
    if (table.range) vals = vals.filter(v => v >= table.range[0] && v <= table.range[1]);
    return vals.sort((a, b) => a - b);
  }

  /* Rows: one per band, then "otherwise", then "no data" when it differs. */
  function bandRows(table, fmt) {
    const vals = sampleValues(table);
    const pick = (idx) => {
      const hits = vals.filter(v => bandIndex(table, v) === idx);
      if (!hits.length) return null;
      const inner = hits.filter(v => !table.bands.some(b => b.at === v));
      const pool = inner.length ? inner : hits;
      return pool[Math.floor(pool.length / 2)];
    };
    const rows = table.bands.map((b, i) => ({
      value: pick(i), out: b.out, cond: bandCondition(table, i, fmt),
    }));
    rows.push({ value: pick(-1), out: table.otherwise, cond: 'any other value' });
    if (table.missing !== table.otherwise) {
      rows.push({ value: null, out: table.missing, cond: 'no data' });
    }
    return rows;
  }

  /* A two-column table: the sample painted with the live class | the rule. */
  function bandTable(table, fmt, opts) {
    const o = Object.assign({ tag: 'td', swatch: false }, opts || {});
    const rows = bandRows(table, fmt).map(r => {
      let sample;
      if (o.swatch) {
        sample = `<span class="lg-swatch" style="background:${esc(r.out)}"></span>`;
      } else {
        const text = r.value == null ? '—' : fmt(r.value);
        // With no sample value the row's own output is the class; with one,
        // the live rule decides, so a drifted row paints what the tab paints.
        const cls = r.value == null ? r.out : window.MM_COLORS.pickBand(table, r.value);
        sample = `<span class="lg-num ${esc(cls)}">${esc(text)}</span>`;
      }
      return `<tr><td class="lg-bt-sample">${sample}</td><td class="lg-bt-cond">${esc(r.cond)}</td></tr>`;
    }).join('');
    return `<table class="lg-band-table"><tbody>${rows}</tbody></table>`;
  }

  // ── CANDLE MATH (mirrors the server's definitions) ────
  function ramp(a, b, n) {
    return Array.from({ length: n }, (_, i) => a + (b - a) * i / (n - 1));
  }

  function ema(xs, n) {
    const a = 2 / (n + 1);
    let e = xs[0];
    return xs.map(x => (e = a * x + (1 - a) * e));
  }

  function sma(xs, n) {
    return xs.map((_, i) => {
      const w = xs.slice(Math.max(0, i - n + 1), i + 1);
      return w.reduce((s, x) => s + x, 0) / w.length;
    });
  }

  /* Every figure is a real series: a hidden run of closes seeds the averages,
     then the drawn candles follow. The checks below read these numbers, so a
     picture that stops meeting its rule says so on screen. */
  function analyse(scn) {
    const [a, b, n] = scn.prefix;
    const pre = ramp(a, b, n);
    const candles = scn.candles.map(([o, h, l, c]) => ({ o, h, l, c }));
    const closes = pre.concat(candles.map(k => k.c));
    const m = candles.length;
    const tail = xs => xs.slice(-m);
    const e10 = tail(ema(closes, 10));
    const e20 = tail(ema(closes, 20));
    const s50 = tail(sma(closes, 50));
    const adr = candles.reduce((s, k) => s + (k.h / k.l - 1), 0) / m * 100;
    const atr = candles.reduce((s, k, i) => {
      const pc = i ? candles[i - 1].c : k.o;
      return s + Math.max(k.h - k.l, Math.abs(k.h - pc), Math.abs(k.l - pc));
    }, 0) / m;
    const R = window.MM_COLORS.PATTERN_RULES;
    const look = R.coilHighLookback;
    const hiPool = pre.slice(-(Math.max(0, look - m))).concat(candles.map(k => k.h));
    const hi50 = Math.max(...hiPool);
    return { candles, e10, e20, s50, adr, atr, hi50 };
  }

  function dayPatternChecks(s) {
    const R = window.MM_COLORS.PATTERN_RULES;
    const k = s.candles[s.candles.length - 1];
    const p = s.candles[s.candles.length - 2];
    const body = Math.abs(k.c - k.o) / k.c * 100;
    const bodyCap = R.tightDayBodyAdr * s.adr;
    const tight = body < bodyCap;
    const candleIn = k.h <= p.h && k.l >= p.l;
    const bodyIn = Math.max(k.o, k.c) <= Math.max(p.o, p.c) && Math.min(k.o, k.c) >= Math.min(p.o, p.c);
    const inside = candleIn || bodyIn;
    const d10 = Math.abs(k.c - s.e10[s.e10.length - 1]);
    const d20 = Math.abs(k.c - s.e20[s.e20.length - 1]);
    const near = Math.min(d10, d20) < R.closeToMaAtr * s.atr;
    const nearMa = d10 <= d20 ? 'EMA10' : 'EMA20';
    return {
      pass: (tight || inside) && near,
      rows: [
        [tight, `Tight day: body ${num(body, 2)}% vs limit ${R.tightDayBodyAdr} × ADR ${num(s.adr, 2)}% = ${num(bodyCap, 2)}%`],
        [inside, candleIn
          ? `Inside day: high ${num(k.h, 1)} ≤ ${num(p.h, 1)} and low ${num(k.l, 1)} ≥ ${num(p.l, 1)}`
          : bodyIn ? 'Inside day: the body sits inside the prior body'
            : 'Inside day: the bar breaks the prior range and the prior body'],
        [near, `Near an average: ${num(Math.min(d10, d20), 2)} from ${nearMa} vs limit ${R.closeToMaAtr} × ATR ${num(s.atr, 2)} = ${num(R.closeToMaAtr * s.atr, 2)}`],
      ],
    };
  }

  function coilChecks(s) {
    const R = window.MM_COLORS.PATTERN_RULES;
    const last = s.candles.slice(-R.coilWindow).map(k => k.c);
    const mean = last.reduce((a, b) => a + b, 0) / last.length;
    const band = (Math.max(...last) - Math.min(...last)) / mean * 100 / s.adr;
    const c = last[last.length - 1];
    const loc = c / s.hi50;
    return {
      pass: band <= R.coilAdrFraction && loc >= R.coilHighFrac,
      rows: [
        [band <= R.coilAdrFraction, `Last ${R.coilWindow} closes span ${num(band, 2)} ADR (limit ${R.coilAdrFraction})`],
        [loc >= R.coilHighFrac, `Close is ${num(loc, 2)} of the ${R.coilHighLookback}-day high (floor ${R.coilHighFrac})`],
      ],
    };
  }

  function stackChecks(s, split) {
    const e10 = s.e10[s.e10.length - 1], e20 = s.e20[s.e20.length - 1], s50 = s.s50[s.s50.length - 1];
    const a = e10 > e20;
    const b = split ? !(e20 > s50) : e20 > s50;
    return {
      pass: a && b,
      rows: [
        [a, `EMA10 ${num(e10, 1)} above EMA20 ${num(e20, 1)}`],
        [b, split
          ? `EMA20 ${num(e20, 1)} not above SMA50 ${num(s50, 1)}`
          : `EMA20 ${num(e20, 1)} above SMA50 ${num(s50, 1)}`],
      ],
    };
  }

  function checkList(res, verdict) {
    const rows = res.rows.map(([ok, text]) =>
      `<li class="${ok ? 'lg-ok' : 'lg-no'}"><span class="lg-mark">${ok ? '✓' : '✗'}</span>${esc(text)}</li>`).join('');
    const v = verdict
      ? `<div class="lg-verdict ${res.pass === verdict.expect ? 'lg-ok' : 'lg-no'}">${esc(res.pass ? verdict.yes : verdict.no)}</div>`
      : '';
    return `<ul class="lg-checks">${rows}</ul>${v}`;
  }

  // ── SVG CANDLE CHART ──────────────────────────────────
  const CANDLE_UP = '#26a69a';   // TradingView dark-theme candle colours, so the
  const CANDLE_DN = '#ef5350';   // figures read like the chart beside every tab

  function candleSvg(s, opts) {
    const o = Object.assign({ lines: ['e10', 'e20', 's50'], box: null, hi: false, marks: {} }, opts || {});
    const MA = window.MM_COLORS.CHART_MA_COLORS;
    const LINE = {
      e10: { vals: s.e10, color: MA.ema, dash: '', label: 'EMA10' },
      e20: { vals: s.e20, color: MA.ema, dash: '4 3', label: 'EMA20' },
      s50: { vals: s.s50, color: MA.sma, dash: '', label: 'SMA50' },
    };
    const W = 330, H = 170, L = 6, R = 64, T = 16, B = 14;
    const n = s.candles.length;
    const R_ = window.MM_COLORS.PATTERN_RULES;
    let ys = s.candles.flatMap(k => [k.h, k.l]);
    o.lines.forEach(key => { ys = ys.concat(LINE[key].vals); });
    if (o.hi) ys.push(s.hi50);
    let lo = Math.min(...ys), hi = Math.max(...ys);
    const pad = (hi - lo) * 0.08;
    lo -= pad; hi += pad;
    const slot = (W - L - R) / n;
    const x = i => L + slot * (i + 0.5);
    const y = v => T + (hi - v) / (hi - lo) * (H - T - B);
    const parts = [];
    if (o.box) {
      const from = n - o.box.count;
      const cl = s.candles.slice(from).map(k => k.c);
      const y1 = y(Math.max(...cl)), y2 = y(Math.min(...cl));
      parts.push(`<rect x="${x(from) - slot / 2}" y="${y1 - 2}" width="${slot * o.box.count}" height="${Math.max(4, y2 - y1 + 4)}" style="fill:var(--coil-dim);stroke:var(--coil)" rx="2"/>`);
      parts.push(`<text x="${x(from) - slot / 2}" y="${Math.max(4, y2 - y1 + 4) + y1 + 8}" class="lg-svg-note" style="fill:var(--coil)">${esc(o.box.label)}</text>`);
    }
    if (o.hi) {
      const yh = y(s.hi50);
      parts.push(`<line x1="${L}" x2="${W - R}" y1="${yh}" y2="${yh}" style="stroke:var(--text3)" stroke-dasharray="3 3"/>`);
      parts.push(`<text x="${W - R + 4}" y="${yh + 3}" class="lg-svg-label" style="fill:var(--text2)">${R_.coilHighLookback}d high</text>`);
    }
    const labels = [];
    o.lines.forEach(key => {
      const ln = LINE[key];
      const pts = ln.vals.map((v, i) => `${x(i).toFixed(1)},${y(v).toFixed(1)}`).join(' ');
      parts.push(`<polyline points="${pts}" fill="none" style="stroke:${ln.color}" stroke-width="1.4"${ln.dash ? ` stroke-dasharray="${ln.dash}"` : ''}/>`);
      labels.push({ y: y(ln.vals[ln.vals.length - 1]), text: ln.label, color: ln.color });
    });
    s.candles.forEach((k, i) => {
      const up = k.c >= k.o;
      const col = up ? CANDLE_UP : CANDLE_DN;
      const bw = slot * 0.56;
      const top = y(Math.max(k.o, k.c)), bot = y(Math.min(k.o, k.c));
      parts.push(`<line x1="${x(i)}" x2="${x(i)}" y1="${y(k.h)}" y2="${y(k.l)}" style="stroke:${col}"/>`);
      parts.push(`<rect x="${x(i) - bw / 2}" y="${top}" width="${bw}" height="${Math.max(1.2, bot - top)}" style="fill:${col}"/>`);
      if (o.marks[i]) {
        parts.push(`<text x="${x(i)}" y="${y(k.h) - 4}" text-anchor="middle" class="lg-svg-note" style="fill:var(--text2)">${esc(o.marks[i])}</text>`);
      }
    });
    // Right-edge labels, nudged apart so close averages stay readable.
    labels.sort((a, b) => a.y - b.y);
    for (let i = 1; i < labels.length; i++) {
      if (labels[i].y - labels[i - 1].y < 10) labels[i].y = labels[i - 1].y + 10;
    }
    labels.forEach(lb => parts.push(
      `<text x="${W - R + 4}" y="${lb.y + 3}" class="lg-svg-label" style="fill:${lb.color}">${lb.text}</text>`));
    return `<svg class="lg-candles" viewBox="0 0 ${W} ${H}" role="img" aria-label="Candlestick example">${parts.join('')}</svg>`;
  }

  function figure(svg, checks, caption) {
    return `<figure class="lg-fig">${caption ? `<figcaption>${esc(caption)}</figcaption>` : ''}${svg}${checks ? `<div class="lg-fig-checks">${checks}</div>` : ''}</figure>`;
  }

  // ── SCENARIOS ─────────────────────────────────────────
  // [open, high, low, close] per bar; `prefix` is [first, last, count] of the
  // hidden closes that seed the averages. Tuned so each meets its rule at the
  // shipped thresholds — if a threshold moves, the check list says whether the
  // picture still qualifies.
  const BASE_RUN = [
    [95.0, 97.6, 94.4, 97.2], [97.2, 99.8, 96.6, 99.4], [99.4, 101.5, 98.2, 98.6],
    [98.6, 100.2, 97.1, 99.8], [99.8, 102.4, 99.2, 101.9], [101.9, 103.0, 100.1, 100.6],
    [100.6, 101.4, 98.9, 99.5],
  ];
  const SCN = {
    inside: { prefix: [80, 95, 50], candles: BASE_RUN.concat([[99.5, 102.6, 98.4, 101.8], [101.6, 102.2, 99.0, 99.6]]) },
    tight: { prefix: [80, 95, 50], candles: BASE_RUN.concat([[99.5, 101.2, 98.8, 100.2], [99.4, 101.6, 98.5, 99.7]]) },
    extended: { prefix: [80, 95, 50], candles: BASE_RUN.concat([[99.5, 102.6, 98.4, 101.8], [102.0, 108.8, 101.6, 108.3]]) },
    coil: {
      prefix: [78, 92, 50], candles: [
        [92.0, 95.2, 91.4, 94.8], [94.8, 98.6, 94.2, 98.1], [98.1, 102.3, 97.6, 101.7],
        [101.7, 104.9, 100.8, 102.2], [102.2, 103.1, 99.6, 100.3], [100.3, 101.8, 99.0, 101.2],
        [101.2, 102.3, 100.2, 101.5], [101.5, 102.1, 100.6, 101.1], [101.1, 102.0, 100.4, 101.4],
      ],
    },
    stacked: {
      prefix: [72, 96, 50], candles: [
        [96.0, 98.4, 95.4, 97.9], [97.9, 99.6, 96.8, 99.2], [99.2, 101.9, 98.7, 101.4],
        [101.4, 102.2, 99.8, 100.4], [100.4, 103.1, 100.0, 102.7], [102.7, 105.0, 102.1, 104.6],
        [104.6, 106.1, 103.2, 103.8], [103.8, 106.9, 103.4, 106.4], [106.4, 108.8, 105.8, 108.1],
      ],
    },
    split: {
      prefix: [118, 96, 50], candles: [
        [94.0, 95.0, 92.6, 93.2], [93.2, 94.6, 92.4, 94.1], [94.1, 96.3, 93.8, 95.9],
        [95.9, 98.0, 95.4, 97.6], [97.6, 99.4, 97.0, 98.9], [98.9, 100.9, 98.3, 100.4],
        [100.4, 102.1, 99.6, 101.6], [101.6, 103.4, 101.0, 102.8], [102.8, 104.3, 102.1, 103.6],
        [103.6, 105.2, 103.0, 104.7],
      ],
    },
  };

  // ── OTHER FIGURES ─────────────────────────────────────
  function floatBarSvg() {
    const floor = window.MM_COLORS.PATTERN_RULES.shortFloorPct;
    const shorted = Math.min(95, floor + 8);
    const W = 330, x0 = 8, x1 = W - 8, w = x1 - x0;
    const at = p => x0 + w * p / 100;
    return `<svg class="lg-candles" viewBox="0 0 ${W} 74" role="img" aria-label="Short interest as a share of float">
      <text x="${x0}" y="14" class="lg-svg-label" style="fill:var(--text2)">Float (shares free to trade) = 100%</text>
      <rect x="${x0}" y="22" width="${w}" height="22" style="fill:var(--bg4);stroke:var(--border2)"/>
      <rect x="${x0}" y="22" width="${at(shorted) - x0}" height="22" style="fill:var(--hl-short-dim);stroke:var(--hl-short)"/>
      <text x="${at(shorted) + 6}" y="37" class="lg-svg-label" style="fill:var(--hl-short)">${shorted}% sold short</text>
      <line x1="${at(floor)}" x2="${at(floor)}" y1="16" y2="50" style="stroke:var(--amber)" stroke-dasharray="3 2"/>
      <text x="${at(floor)}" y="62" text-anchor="middle" class="lg-svg-label" style="fill:var(--amber)">${floor}% floor</text>
    </svg>`;
  }

  function nasiSvg() {
    const N = window.MM_COLORS.NASI;
    const W = 330, n = 60;
    const sum = Array.from({ length: n }, (_, i) => 40 + 22 * Math.sin(i / 8) + i * 0.3);
    const ma = sum.map((_, i) => sum.slice(Math.max(0, i - 9), i + 1).reduce((a, b) => a + b, 0) / Math.min(10, i + 1));
    const rsi = Array.from({ length: n }, (_, i) => 50 + 44 * Math.sin(i / 8 + 0.9));
    const x = i => 6 + (W - 40) * i / (n - 1);
    const yS = v => 8 + (80 - v) * 0.8;
    const yR = v => 78 + (100 - v) * 0.4;
    const path = (arr, f) => arr.map((v, i) => `${i ? 'L' : 'M'}${x(i).toFixed(1)},${f(v).toFixed(1)}`).join('');
    const marks = rsi.map((v, i) => {
      if (v <= N.oversold) return `<circle cx="${x(i)}" cy="${yR(v)}" r="2" style="fill:var(--green)"/>`;
      if (v >= N.overbought) return `<circle cx="${x(i)}" cy="${yR(v)}" r="2" style="fill:var(--red)"/>`;
      return '';
    }).join('');
    const cx = x(40);
    return `<svg class="lg-candles" viewBox="0 0 ${W} 124" role="img" aria-label="NASI chart colour key">
      <line x1="6" x2="${W - 34}" y1="${yS(40)}" y2="${yS(40)}" style="stroke:var(--border2)" stroke-dasharray="3 3"/>
      <path d="${path(ma, yS)}" fill="none" style="stroke:var(--accent)" stroke-dasharray="4 3"/>
      <path d="${path(sum, yS)}" fill="none" style="stroke:var(--text)" stroke-width="1.4"/>
      <rect x="6" y="${yR(N.oversold)}" width="${W - 40}" height="${yR(0) - yR(N.oversold)}" style="fill:var(--amber)" opacity="0.16"/>
      <line x1="6" x2="${W - 34}" y1="${yR(N.oversold)}" y2="${yR(N.oversold)}" style="stroke:var(--amber)"/>
      <line x1="6" x2="${W - 34}" y1="${yR(N.overbought)}" y2="${yR(N.overbought)}" style="stroke:var(--amber)"/>
      <path d="${path(rsi, yR)}" fill="none" style="stroke:var(--text2)" stroke-width="1.2"/>
      ${marks}
      <line x1="${cx}" x2="${cx}" y1="6" y2="118" style="stroke:var(--yellow)" opacity="0.75"/>
      <text x="${W - 30}" y="${yR(N.oversold) + 3}" class="lg-svg-label" style="fill:var(--amber)">${N.oversold}</text>
      <text x="${W - 30}" y="${yR(N.overbought) + 3}" class="lg-svg-label" style="fill:var(--amber)">${N.overbought}</text>
      <text x="${W - 30}" y="30" class="lg-svg-label" style="fill:var(--text2)">NASI</text>
      <text x="${W - 30}" y="104" class="lg-svg-label" style="fill:var(--text2)">RSI</text>
    </svg>`;
  }

  function ringsSvg() {
    const V = window.MM_COLORS.VIZ_COLORS;
    return `<svg class="lg-candles" viewBox="0 0 330 96" role="img" aria-label="Network node key">
      <polygon points="42,20 62,31 62,53 42,64 22,53 22,31" style="fill:${V.l1Fill};stroke:${V.l1Accent}" stroke-width="2.5" opacity="0.9"/>
      <text x="42" y="80" text-anchor="middle" class="lg-svg-label" style="fill:${V.l1Accent}">L1 hub</text>
      <circle cx="108" cy="42" r="17" style="fill:hsl(35,88%,53%);stroke:${V.l1Accent}" stroke-width="4"/>
      <text x="108" y="80" text-anchor="middle" class="lg-svg-label" style="fill:var(--text2)">#1 theme</text>
      <circle cx="170" cy="42" r="10" style="fill:${window.MM_COLORS.rsFill(95)};stroke:${V.bridge}" stroke-width="2"/>
      <text x="170" y="80" text-anchor="middle" class="lg-svg-label" style="fill:var(--text2)">bridge</text>
      <circle cx="226" cy="42" r="10" style="fill:${window.MM_COLORS.rsFill(95)};stroke:${V.selected}" stroke-width="3"/>
      <text x="226" y="80" text-anchor="middle" class="lg-svg-label" style="fill:${V.selected}">selected</text>
      <circle cx="286" cy="42" r="9" style="fill:${window.MM_COLORS.rsFill(85)}"/>
      <circle cx="286" cy="42" r="15" fill="none" style="stroke:var(--green)" stroke-width="2"/>
      <text x="286" y="80" text-anchor="middle" class="lg-svg-label" style="fill:var(--green)">tight</text>
    </svg>`;
  }

  function edgesSvg() {
    const V = window.MM_COLORS.VIZ_COLORS;
    const row = (y, color, dash, width, opacity, label) =>
      `<line x1="10" x2="120" y1="${y}" y2="${y}" style="stroke:${color}" stroke-width="${width}" opacity="${opacity}"${dash ? ` stroke-dasharray="${dash}"` : ''}/>
       <text x="132" y="${y + 3}" class="lg-svg-label" style="fill:var(--text2)">${label}</text>`;
    return `<svg class="lg-candles" viewBox="0 0 330 74" role="img" aria-label="Network edge key">
      ${row(14, V.edge, '', 2.5, 0.55, 'theme to member')}
      ${row(36, V.leaderEdge, '', 3, 0.85, 'theme to its leader')}
      ${row(58, V.l1Accent, '6 4', 2, 0.35, 'theme to its L1 hub')}
    </svg>`;
  }

  // ── SAMPLE MARKUP ─────────────────────────────────────
  function chip(cls, text) {
    return `<span class="tn-link radar-chip ${cls}">${esc(text)}</span>`;
  }

  function link(cls, text) {
    return `<span class="tn-link${cls ? ' ' + cls : ''}">${esc(text)}</span>`;
  }

  function miniRow(linkCls, rowCls, ticker, cells) {
    return `<tr${rowCls ? ` class="${rowCls}"` : ''}><td class="l">${link(linkCls, ticker)}</td>${cells.map(c => `<td>${c}</td>`).join('')}</tr>`;
  }

  function fmtMoney(v) {
    return v >= 1e9 ? '$' + num(v / 1e9, 1) + 'B' : '$' + Math.round(v / 1e6) + 'M';
  }

  function minsToEt(m) {
    const h = Math.floor(m / 60), mm = m % 60;
    const h12 = ((h + 11) % 12) + 1;
    return `${h12}:${String(mm).padStart(2, '0')} ${h < 12 ? 'AM' : 'PM'}`;
  }

  // ── CARDS ─────────────────────────────────────────────
  function card(c) {
    return `
      <article class="lg-card${c.wide ? ' lg-wide' : ''}" id="lg-${c.id}">
        <header class="lg-card-hdr">
          <div class="lg-sample">${c.sample}</div>
          <h3>${esc(c.title)}</h3>
        </header>
        <div class="lg-body">${c.body}</div>
        ${c.fig || ''}
        ${c.where && c.where.length ? whereTags(c.where) : ''}
      </article>`;
  }

  const LIST_TABS = ['themes', 'vars', 'momentum', 'volume', 'si', 'parabolic', 'industry', 'etf'];
  const CUTOFF_TABS = ['themes', 'vars', 'momentum', 'volume', 'parabolic'];

  function sections() {
    const C = window.MM_COLORS;
    const R = C.PATTERN_RULES;
    const B = C.COLOR_BANDS;
    const cut = C.cutoffs();
    const f0 = v => num(v, 0);
    const f1 = v => num(v, 1);
    const f2 = v => num(v, 2);
    const pct1 = v => num(v, 1) + '%';
    const spct = v => signed(v, 1) + '%';

    const dp = (key, cap, expect) => {
      const s = analyse(SCN[key]);
      const res = dayPatternChecks(s);
      const n = s.candles.length;
      const marks = key === 'inside' ? { [n - 2]: 'prior', [n - 1]: 'inside' }
        : key === 'tight' ? { [n - 1]: 'tight' } : { [n - 1]: 'wide' };
      return figure(candleSvg(s, { lines: ['e10', 'e20'], marks }),
        checkList(res, { expect, yes: 'Result: ticker turns green', no: 'Result: ticker stays default' }), cap);
    };

    const tierIntro = Object.keys(C.HL_TIER_TIP).map((tier, i) => {
      const cls = tier === 'coil' ? 'coiled' : C.HL_TIER_CLASS[tier];
      return `<li><span class="lg-rung">${i + 1}</span><span class="lg-key">${link(cls, 'TICK')}</span><span class="lg-key-text">${esc(C.HL_TIER_TIP[tier])}</span></li>`;
    }).join('');

    const coil = analyse(SCN.coil);
    const stacked = analyse(SCN.stacked);
    const split = analyse(SCN.split);

    const VIZ_TABS = ['themeviz', 'momentumviz', 'volumeviz', 'varsviz'];

    return [
      {
        id: 'ticker-text', title: 'Ticker symbol — text colour',
        intro: 'The text colour of a ticker symbol marks the day pattern, the selection, and the V/A cutoffs.',
        cards: [
          {
            id: 'green', title: 'Green text — entry-ready day pattern',
            wide: true,
            sample: link('day-pattern-green', 'NVDA'),
            where: LIST_TABS,
            body: `<p>The latest bar is a tight day or an inside day, and it closes near a short-term average. All of the conditions are true on the latest bar:</p>
              <ul class="lg-rules">
                <li><b>Tight day:</b> |close − open| ÷ close &lt; ${R.tightDayBodyAdr} × ADR%.</li>
                <li><b>Inside day:</b> high ≤ prior high and low ≥ prior low, <i>or</i> the body sits inside the prior body.</li>
                <li><b>Near an average:</b> |close − EMA10| or |close − EMA20| &lt; ${R.closeToMaAtr} × ATR14.</li>
              </ul>
              <p class="lg-formula">(tight day OR inside day) AND near an average</p>
              <p>The server computes this rule. The EP tab does not use it. The Viz tabs show the same signal as a pulsing green ring.</p>`,
            fig: `<div class="lg-figs">${dp('inside', 'Inside day on EMA10', true)}${dp('tight', 'Tight day on EMA10', true)}${dp('extended', 'Counter-example: wide bar far above EMA10', false)}</div>`,
          },
          {
            id: 'selected', title: 'Yellow text, solid underline — selected ticker',
            sample: link('active-ticker', 'AMD'),
            where: LIST_TABS.concat(['macro', 'ep']),
            body: `<p>This ticker is on the chart now. Click a ticker or use the ↑/↓ keys to select one. In a table, the row also gets a yellow tint and outline.</p>
              <table class="lg-mini"><tbody>
                ${miniRow('', '', 'NVDA', ['91'])}
                ${miniRow('active-ticker', 'nav-active', 'AMD', ['88'])}
                ${miniRow('', '', 'AVGO', ['84'])}
              </tbody></table>
              <p>Selection always wins over the tints below. A selected chip on the Themes tab gets a yellow outline.</p>
              <div class="lg-chips">${chip('chip-screened active-ticker', 'AMD')}${chip('chip-quiet coiled active-ticker', 'MU')}</div>`,
          },
          {
            id: 'hover', title: 'Blue text on hover — clickable ticker',
            sample: `<span class="tn-link lg-live">hover me</span>`,
            where: LIST_TABS.concat(['macro', 'ep']),
            body: '<p>The pointer is on a ticker. Click it to open its chart. The dotted underline marks every clickable ticker.</p>',
          },
          {
            id: 'dimmed', title: 'Greyed out — below a V/A cutoff',
            sample: `<span class="lg-dim-demo">${chip('filtered-out', 'LOW')}</span>`,
            where: CUTOFF_TABS,
            body: `<p>The ticker fails a cutoff in the <b>V</b> or <b>A</b> dropdown:</p>
              <ul class="lg-rules">
                <li><b>V:</b> 20-day average dollar volume below the cutoff (now ${esc(fmtMoney(cut.vol))}).</li>
                <li><b>A:</b> 20-day ADR% below the cutoff (now ${esc(num(cut.adr * 100, 1))}%).</li>
              </ul>
              <p>Dimming changes no score, rank or order. A dimmed ticker stays clickable, but the ↑/↓ keys skip it. A ticker with no data for a metric is not dimmed.</p>
              <table class="lg-mini"><tbody>
                ${miniRow('', '', 'CRWD', ['$1.2B', '3.1%'])}
                ${miniRow('', 'filtered-out', 'TINY', ['$8M', '2.1%'])}
              </tbody></table>
              <div class="lg-chips">${chip('chip-screened', 'CRWD')}${chip('chip-quiet filtered-out', 'TINY')}</div>`,
          },
        ],
      },
      {
        id: 'ticker-tint', title: 'Ticker symbol — background tint',
        intro: 'A tint behind the symbol names one rung of a ladder. Each ticker gets one tint at most: the first rung it meets, from the top.',
        cards: [
          {
            id: 'ladder', title: 'The ladder — one tint per ticker',
            sample: link('coiled day-pattern-green', 'TICK'),
            where: LIST_TABS.concat(['ep']),
            body: `<ol class="lg-ladder">${tierIntro}</ol>
              <p>The order is a display choice, not a ranking claim. A tab skips a rung when it has no data for that rung, and the ladder continues to the next one. So green does not mean "not crowded".</p>
              <p>The tint is a background, so it combines with green text. The sample above is a coiled ticker with the green day pattern.</p>
              <p>The short rung uses today's short interest only. Past sessions in the date dropdown never show it.</p>`,
          },
          {
            id: 'coil', title: 'Violet tint — coiled (tight base)',
            sample: link('coiled', 'CRWD'),
            where: LIST_TABS.concat(['ep']),
            body: `<p>The stock rests in a tight base near its high. Both conditions are true:</p>
              <ul class="lg-rules">
                <li>The last ${R.coilWindow} closes sit in a band no wider than ${R.coilAdrFraction} × ADR.</li>
                <li>The close is at least ${R.coilHighFrac} × the ${R.coilHighLookback}-day high.</li>
              </ul>
              <p>This is rung 1 of the ladder. On the Themes tab it also drives the COIL badges and the COILED strip.</p>`,
            fig: figure(candleSvg(coil, { lines: ['e10', 'e20'], box: { count: R.coilWindow, label: `last ${R.coilWindow} closes` }, hi: true }),
              checkList(coilChecks(coil), { expect: true, yes: 'Result: violet tint', no: 'Result: no coil tint' }),
              'Tight closes under the recent high'),
          },
          {
            id: 'short', title: 'Cyan-blue tint — crowded short',
            sample: link('hl-short', 'BYND'),
            where: LIST_TABS.concat(['ep']),
            body: `<p>Short interest is ${R.shortFloorPct}% of float or more. The Short% number in the same row turns green at the same level.</p>`,
            fig: figure(floatBarSvg(), '', `Short interest at or above ${R.shortFloorPct}% of float`),
          },
          {
            id: 'ma-up', title: 'Green tint — averages stacked',
            sample: link('hl-ma-up', 'PLTR'),
            where: LIST_TABS.concat(['ep']),
            body: '<p>EMA10 is above EMA20, and EMA20 is above SMA50. The SMA50 needs a full 50 sessions of data.</p>',
            fig: figure(candleSvg(stacked), checkList(stackChecks(stacked, false), { expect: true, yes: 'Result: green tint', no: 'Result: no green tint' }), 'EMA10 > EMA20 > SMA50'),
          },
          {
            id: 'ma-split', title: 'Orange tint — split stack',
            sample: link('hl-ma-split', 'SNOW'),
            where: LIST_TABS.concat(['ep']),
            body: '<p>EMA10 is above EMA20, and EMA20 is not above SMA50. This rung states where the averages sit. It does not state a direction.</p>',
            fig: figure(candleSvg(split), checkList(stackChecks(split, true), { expect: true, yes: 'Result: orange tint', no: 'Result: no orange tint' }), 'EMA10 > EMA20, EMA20 ≤ SMA50'),
          },
        ],
      },
      {
        id: 'themes-tab', title: 'Themes tab — chips, badges and the coil strip',
        intro: 'Each chip is one ticker in a leaf theme. The chip border and weight tell you if the ticker passed a screener today.',
        cards: [
          {
            id: 'chips', title: 'Blue outline vs. dimmed chip',
            sample: `<span class="lg-chips">${chip('chip-screened', 'CRWD')}${chip('chip-quiet', 'CHKP')}</span>`,
            where: ['themes'],
            body: `${keyList([
                [chip('chip-screened', 'CRWD'), 'Blue outline, bold: the ticker passed at least one screener today.'],
                [chip('chip-quiet', 'CHKP'), 'Dimmed: the ticker is in the theme but passed no screener today. Hover it to read it at full strength.'],
              ])}
              <p>Tints and green text combine with both styles:</p>
              <div class="lg-chips">${chip('chip-screened day-pattern-green', 'PANW')}${chip('chip-quiet coiled', 'FTNT')}${chip('chip-screened hl-short', 'S')}${chip('chip-quiet hl-ma-up', 'ZS')}${chip('chip-quiet hl-ma-split', 'OKTA')}</div>`,
          },
          {
            id: 'coil-marks', title: 'Violet coil markers',
            sample: '<span class="coil-badge">COIL 4</span>',
            where: ['themes'],
            body: `${keyList([
                ['<span class="coil-badge">COIL 4</span>', 'On an L1 header: the count of members in a tight base.'],
                ['<span class="coil-badge">◉ 2</span>', 'On a leaf: the count of members in a tight base.'],
              ])}
              <p>The COILED strip above the board names the themes with the largest share of coiled members, at any rank. Click a pin to jump to its theme. The theme block flashes a violet outline.</p>
              <div class="coil-strip"><span class="coil-strip-label">COILED</span><div class="coil-pins"><button type="button" class="coil-pin" tabindex="-1">Cybersecurity <span class="coil-pin-n">4/31</span><span class="coil-pin-pct">13%</span></button></div></div>
              <div class="coil-strip coil-strip-empty"><span class="coil-strip-label">COILED</span><span class="radar-n">no theme qualified today</span></div>
              <div class="radar-controls"><button type="button" class="coil-sort-btn on" tabindex="-1">◉ Coiled first</button><span class="radar-n">violet = sort by coil share is on (order only)</span></div>
              <div class="theme-block coil-jump lg-jump-demo">Theme block after a pin jump</div>`,
          },
          {
            id: 'radar-nums', title: 'Amber rank, blue boosted score',
            sample: '<span class="radar-rank">#3</span>',
            where: ['themes', 'vars', 'momentum', 'volume', 'si'],
            body: `<div class="radar-leaf-hdr lg-inline"><span class="radar-rank">#3</span><span class="radar-leaf-name">Cybersecurity / Endpoint</span><span class="radar-n">N=6</span><span class="radar-scores">0.812<span class="radar-arrow">→</span><span class="radar-boosted">1.104</span></span></div>
              ${keyList([
                ['<span class="theme-rank">#1</span>', 'Amber numbers are ranks.'],
                ['<span class="radar-scores">0.81<span class="radar-arrow">→</span><span class="radar-boosted">1.10</span></span>',
                  'Themes tab: grey is the raw score, blue is the score after the L1 sibling boost.'],
              ])}`,
          },
        ],
      },
      {
        id: 'headers', title: 'Section headers',
        intro: 'Theme tabs group tickers under L1 sections.',
        cards: [
          {
            id: 'l1-band', title: 'Indigo band — an L1 section starts',
            sample: '<span class="lg-l1-swatch"></span>',
            where: ['themes', 'vars', 'momentum', 'volume', 'si'],
            body: `<div class="theme-header"><span class="theme-rank">#2</span><span class="theme-name">Cybersecurity</span><span class="theme-score">top-5 VARS 4.10</span></div>
              <p>The band and the blue left rail mark the start of a new L1 section in a long list.</p>`,
          },
          {
            id: 'hot', title: 'Amber band + HOT — a hot L1',
            sample: '<span class="hot-badge">HOT</span>',
            where: ['vars', 'si'],
            body: `<div class="theme-header l1-hot"><span class="theme-rank">#1</span><span class="theme-name">Semiconductors<span class="hot-badge">HOT</span></span><span class="theme-score">avg RS 81.2%</span></div>
              <ul class="lg-rules">
                <li><b>${esc(tabLabel('vars'))}:</b> average member RS ≥ ${R.varsHotRs} with at least ${R.varsHotMinMembers} members.</li>
                <li><b>${esc(tabLabel('si'))}:</b> the L1 ranks #${R.siHotRadarRank} or better on the Themes board. A strong theme with a broken, heavily shorted member is the squeeze setup.</li>
              </ul>
              <p>HOT never changes the order.</p>`,
          },
        ],
      },
      {
        id: 'numbers', title: 'Table numbers',
        intro: 'Green is good for the setup and red is a warning, unless a card says otherwise. Each row below shows a sample value painted by the live rule.',
        cards: [
          {
            id: 'rs', title: 'RS% — relative strength percentile',
            sample: '<span class="lg-num up">91</span>',
            where: ['momentum', 'volume', 'vars'],
            body: bandTable(B.rs, f0),
          },
          {
            id: 'vars', title: 'VARS — volatility-adjusted relative strength',
            sample: '<span class="lg-num up">6.40</span>',
            where: ['volume', 'vars', 'industry', 'etf'],
            body: `${bandTable(B.vars, f2)}
              <p>On the ${esc(tabLabel('vars'))} tab an arrow follows the number:</p>
              ${keyList([
                ['<span class="lg-num">6.40<span class="accel accel-up">▲</span></span>', 'VARS is above its 20-day EMA (strength is building).'],
                ['<span class="lg-num">6.40<span class="accel accel-dn">▼</span></span>', 'VARS is at or below its 20-day EMA.'],
              ])}`,
          },
          {
            id: 'short-col', title: 'Short% / SI% — short interest as % of float',
            sample: '<span class="lg-num up">24.0</span>',
            where: ['momentum', 'volume', 'vars', 'parabolic', 'si'],
            body: `${bandTable(B.short, f1)}<p>Green starts at the same ${R.shortFloorPct}% floor as the cyan-blue tint.</p>`,
          },
          {
            id: 'inst', title: 'Inst% — change in institutional ownership',
            sample: '<span class="lg-num up">+3.2</span>',
            where: ['momentum', 'volume', 'vars', 'si', 'parabolic'],
            body: bandTable(B.inst, v => signed(v, 1)),
          },
          {
            id: 'pct', title: 'Signed % columns',
            sample: '<span class="lg-num up">+4.1%</span>',
            where: ['momentum', 'volume', 'vars', 'industry', 'etf', 'ep'],
            body: `${bandTable(B.pct, spct)}<p>Applies to EPS%, Sales%, Intra%, Daily%, Monthly%, AH Chg% and PM Chg%.</p>`,
          },
          {
            id: 'atrmul', title: 'ATRMul — stretch above the 50-day SMA',
            sample: '<span class="lg-num dn">16.2x</span>',
            where: ['parabolic'],
            body: `${bandTable(B.parabolicAtr, v => num(v, 1) + 'x')}<p>Red is the most stretched. On this short-watch tab, red is the signal, not a warning.</p>`,
          },
          {
            id: 'si-dd', title: 'Drawdown columns — always red',
            sample: '<span class="lg-num dn">-38.4</span>',
            where: ['si'],
            body: '<p>60D DD and 15D Drop are always red. Every row on the SI tab has already sold off, so the colour marks the column, not a level.</p>',
          },
          {
            id: 'ep', title: 'EP scanner columns',
            sample: '<span class="lg-num rvol-high">3.4x</span>',
            where: ['ep'],
            wide: true,
            body: `<div class="lg-subgrid"><div class="lg-sub"><h4>Float(M)</h4>${bandTable(B.epFloat, v => num(v, 0) + 'M')}</div>
              <div class="lg-sub"><h4>Short%</h4>${bandTable(B.epShort, pct1)}</div>
              <div class="lg-sub"><h4>52W Hi%</h4>${bandTable(B.epDist52w, spct)}</div>
              <div class="lg-sub"><h4>ATR× — distance from the 50-day SMA</h4>${bandTable(B.epAtr, v => num(v, 1) + '×')}</div>
              <div class="lg-sub"><h4>RVol — relative volume at this time of day</h4>${bandTable(B.epRvol, v => num(v, 1) + 'x')}</div></div>
              <p>AH Chg% and PM Chg% use the signed % rule.</p>`,
          },
        ],
      },
      {
        id: 'overview', title: 'Overview tab',
        intro: 'The breadth tiles tint by contrarian meaning: washed-out breadth is a bullish setup, so a low reading is green.',
        cards: [
          {
            id: 'macro', title: 'Price and % change',
            sample: '<span class="lg-num pos">+1.2%</span>',
            where: ['macro'],
            body: `${bandTable(B.macroChange, spct)}<p>The price cell takes the colour of the 1-day change.</p>`,
          },
          {
            id: 'breadth', title: 'Breadth and sentiment tiles',
            sample: '<span class="lg-num up">14.2%</span>',
            where: ['macro'],
            wide: true,
            body: `<div class="lg-subgrid"><div class="lg-sub"><h4>CNN Fear &amp; Greed</h4>${bandTable(Object.assign({ range: [0, 100] }, B.fearGreed), f1)}</div>
              <div class="lg-sub"><h4>NCFD</h4>${bandTable(Object.assign({ range: [0, 100] }, B.ncfd), pct1)}</div>
              <div class="lg-sub"><h4>MMFI</h4>${bandTable(Object.assign({ range: [0, 100] }, B.mmfi), pct1)}</div>
              <div class="lg-sub"><h4>MMTW</h4>${bandTable(Object.assign({ range: [0, 100] }, B.mmtw), pct1)}</div>
              <div class="lg-sub"><h4>MMTH</h4>${bandTable(Object.assign({ range: [0, 100] }, B.mmth), pct1)}</div></div>
              <p>The small history numbers under a tile use the rule of that tile. NAAIM and AAII are never tinted: no threshold for them is calibrated here.</p>`,
          },
          {
            id: 'nasi', title: 'NASI — McClellan Summation and its RSI',
            sample: '<span class="nasi-stat-val oversold">9.9</span>',
            where: ['macro'],
            body: `<div class="lg-sub"><h4>OSC (header)</h4>${bandTable(B.nasiOsc, v => signed(v, 2))}</div>
              <div class="lg-sub"><h4>RSI (header)</h4>${bandTable(Object.assign({ range: [0, 100] }, B.nasiRsi), f1).replace(/lg-num/g, 'lg-num nasi-stat-val')}</div>
              <ul class="lg-rules">
                <li>White line: the summation index. Blue dashed line: its 10-day average. Grey dashed line: zero.</li>
                <li>Grey line: RSI(14). Amber rails at ${C.NASI.oversold} and ${C.NASI.overbought}; amber fill below ${C.NASI.oversold}.</li>
                <li>Green dots: sessions at or below ${C.NASI.oversold}. Red dots: sessions at or above ${C.NASI.overbought}.</li>
                <li>Yellow line and <span class="nasi-ro-date">date</span>: the session under the pointer.</li>
              </ul>`,
            fig: figure(nasiSvg(), '', ''),
          },
          {
            id: 'market', title: 'Market status badge',
            sample: '<span class="market-status open"><span class="dot"></span>LIVE</span>',
            where: ['Header'],
            body: `<div class="lg-wide-keys">${keyList(C.MARKET_SESSIONS.map(w => [
                `<span class="market-status ${esc(w.cls)}"><span class="dot"></span><span>${esc(w.text)}</span></span>`,
                `${minsToEt(w.from)}–${minsToEt(w.to)} ET, weekdays`,
              ]).concat([[
                `<span class="market-status ${esc(C.MARKET_CLOSED.cls)}"><span class="dot"></span><span>${esc(C.MARKET_CLOSED.text)}</span></span>`,
                'All other times and weekends',
              ]]))}</div>
              <p>The badge in the header uses your clock. It does not know about market holidays.</p>`,
          },
        ],
      },
      {
        id: 'viz', title: 'Network viz tabs',
        intro: 'The four Viz tabs draw themes and their tickers as a network. They use node colour, rings and glows, not tints.',
        cards: [
          {
            id: 'viz-theme', title: 'Theme node colour — theme strength',
            sample: `<span class="lg-swatch" style="background:${esc(C.themeFill(80, 1))}"></span>`,
            where: VIZ_TABS,
            body: `${themeStrengthTable(C)}
              <p>A paler node is a less actionable theme: saturation falls as fewer members are leaders or tight. The node grows with strength.</p>
              <p>A theme appears only when it is hot: average RS ≥ ${C.VIZ_HOT.rs} with ${C.VIZ_HOT.breadth}+ tickers, or on VARS Viz and Volume Viz, average VARS ≥ ${C.VIZ_HOT.vars}.</p>`,
          },
          {
            id: 'viz-rs', title: 'Ticker node colour — RS',
            sample: `<span class="lg-swatch lg-round" style="background:${esc(C.rsFill(95))}"></span>`,
            where: ['themeviz', 'momentumviz'],
            body: `${bandTable(Object.assign({ range: [0, 100] }, B.vizRs), f0, { swatch: true })}<p>A bigger node has a higher RS.</p>`,
          },
          {
            id: 'viz-vars', title: 'Ticker node colour — VARS',
            sample: `<span class="lg-swatch lg-round" style="background:${esc(C.varsFill(7))}"></span>`,
            where: ['varsviz', 'volumeviz'],
            body: bandTable(B.vizVars, f1, { swatch: true }),
          },
          {
            id: 'viz-rings', title: 'Rings, glows and hubs',
            sample: '<span class="lg-pulse-demo"><span class="tight-pulse-ring"></span></span>',
            where: VIZ_TABS,
            body: `${keyList([
                ['<span class="lg-ring lg-tight"></span>', 'Pulsing green ring: the green day pattern (see the first card).'],
                ['<span class="lg-ring lg-bridge"></span>', 'White ring: a bridge ticker, tagged into two or more themes.'],
                ['<span class="lg-leader-dot"></span>', 'Glow: the leader of its theme. Its edge to the theme is lighter.'],
                [`<span class="lg-ring" style="border:2px solid ${esc(C.VIZ_COLORS.selected)}"></span>`, 'Yellow ring and glow: the selected ticker.'],
                [`<svg class="lg-hex" viewBox="0 0 12 12"><polygon points="6,0.5 11,3.2 11,8.8 6,11.5 1,8.8 1,3.2" style="fill:${esc(C.VIZ_COLORS.l1Fill)};stroke:${esc(C.VIZ_COLORS.l1Accent)}"/></svg>`,
                  'Pale-yellow hexagon: an L1 hub. The #1 theme gets a pale-yellow ring and glow.'],
              ])}
              <p>Hover a node for its tooltip. Tags there repeat the rings:</p>
              <div class="themeviz-tooltip lg-static-tip"><div class="tip-title">NVDA <span class="tip-tag tag-leader">LEADER</span> <span class="tip-tag tag-bridge">BRIDGE</span> <span class="tip-tag tag-tight">TIGHT</span></div><div class="tip-sub">RS 96.0%</div></div>`,
            fig: figure(ringsSvg(), '', ''),
          },
          {
            id: 'viz-edges', title: 'Edges',
            sample: `<span class="lg-edge-demo" style="border-color:${esc(C.VIZ_COLORS.l1Accent)}"></span>`,
            where: VIZ_TABS,
            body: '<p>Lines join each theme to its members and to its L1 hub. A thicker member line is a stronger member.</p>',
            fig: figure(edgesSvg(), '', ''),
          },
        ],
      },
      {
        id: 'chart', title: 'Chart',
        intro: 'Every tab opens the same TradingView daily chart.',
        cards: [
          {
            id: 'chart-ma', title: 'Moving-average lines',
            sample: `<span class="lg-line" style="background:${esc(C.CHART_MA_COLORS.ema)}"></span><span class="lg-line" style="background:${esc(C.CHART_MA_COLORS.sma)}"></span>`,
            where: ['Every chart'],
            body: `${keyList([
                [`<span class="lg-line" style="background:${esc(C.CHART_MA_COLORS.ema)}"></span>`, 'Green: EMA10 and EMA20.'],
                [`<span class="lg-line" style="background:${esc(C.CHART_MA_COLORS.sma)}"></span>`, 'Gold: SMA50 and SMA200.'],
              ])}
              <p>The free chart allows one colour per average type, so the two EMAs share a colour and the two SMAs share a colour. The faster average of each pair sits closer to price. The volume pane is below the price pane.</p>`,
          },
        ],
      },
      {
        id: 'chrome', title: 'Controls and other markers',
        intro: 'These colours mark controls and state, not tickers.',
        cards: [
          {
            id: 'va', title: 'Amber V and A dropdowns — cutoffs are on',
            sample: '<span class="tt-filter-select lg-static">$50M</span>',
            where: CUTOFF_TABS,
            body: '<p>The V/A cutoffs are always on, so the dropdowns are always amber. Choose a value to make the cutoff tighter or looser. See "Greyed out" above.</p>',
          },
          {
            id: 'dates', title: 'Blue date button — the session on screen',
            sample: '<span class="tt-date-btn active lg-static">09-29<span class="tt-weekday">Tue</span></span>',
            where: LIST_TABS.concat(['ep', 'themeviz', 'momentumviz', 'volumeviz', 'varsviz']),
            body: `<div class="lg-inline"><span class="tt-date-btn active lg-static">09-29<span class="tt-weekday">Tue</span></span><span class="tt-date-select active lg-static">09-26 Fri</span><span class="tt-date-btn lg-static">09-25<span class="tt-weekday">Thu</span></span></div>
              <p>The blue button or dropdown is the session you see. Choose another date to go back in time, up to 180 days.</p>`,
          },
          {
            id: 'stale', title: 'Amber banner — old short-interest data',
            sample: '<span class="lg-num" style="color:var(--amber)">!</span>',
            where: ['si'],
            body: '<div class="si-stale-note">Short interest dated 2026-09-26, not this session (2026-09-29). Prices below are current.</div><p>The short-interest list did not update today. The tab still shows the last list, with this warning.</p>',
          },
          {
            id: 'blue-misc', title: 'Other blue markers',
            sample: '<span class="card-lbl lg-static"><span class="badge">sorted by VARS</span></span>',
            where: ['macro', 'industry', 'etf', 'ep', 'parabolic', 'themes'],
            body: `<div class="lg-wide-keys">${keyList([
                ['<span class="card-lbl lg-static"><span class="badge">sorted by VARS</span></span>', 'Blue badge: how a table is sorted, or which session an EP table covers.'],
                ['<span class="event-datetime">Sep 30 08:30</span>', 'Blue time: a macro event in the MACRO EVENTS list.'],
                ['<span class="ep-news-item lg-static"><a href="#" tabindex="-1" onclick="return false">News headline</a></span>', 'Blue on hover: an EP news link.'],
                ['<button type="button" class="radar-more lg-static" tabindex="-1">+5 more</button>', 'Blue on hover: show the hidden chips of a leaf.'],
                ['<span class="resize-handle lg-handle-demo"></span><span class="resize-handle dragging lg-handle-demo"></span>', 'Grey at rest, blue on hover or drag: the handle between the list and the chart.'],
              ])}</div>`,
          },
        ],
      },
    ];
  }

  function themeStrengthTable(C) {
    const t = C.COLOR_BANDS.vizThemeStrength;
    const rows = t.bands.map((b, i) => {
      const cond = bandCondition(t, i, v => num(v, 0));
      return [b.out.label, b.at, cond];
    });
    rows.push([t.otherwise.label, -Infinity, 'any other value']);
    return `<table class="lg-band-table"><tbody>${rows.map(([label, at, cond]) => {
      const v = Number.isFinite(at) ? at : 0;
      return `<tr><td class="lg-bt-sample"><span class="lg-swatch" style="background:${esc(C.themeFill(v, 1))}"></span><span class="lg-swatch lg-pale" style="background:${esc(C.themeFill(v, 0))}"></span></td><td class="lg-bt-cond">${esc(label)}: strength ${esc(cond)}</td></tr>`;
    }).join('')}</tbody></table>`;
  }

  // ── RENDER ────────────────────────────────────────────
  function render() {
    const page = document.getElementById('legend-page');
    if (!page || !window.MM_COLORS) return;
    const secs = sections();
    const toc = secs.map(s => `<a href="#lgs-${s.id}" class="lg-toc-link">${esc(s.title)}</a>`).join('');
    const body = secs.map(s => `
      <section class="lg-section" id="lgs-${s.id}">
        <h2>${esc(s.title)}</h2>
        ${s.intro ? `<p class="lg-intro">${esc(s.intro)}</p>` : ''}
        <div class="lg-grid">${s.cards.map(card).join('')}</div>
      </section>`).join('');
    page.innerHTML = `
      <div class="lg-head">
        <h1>Colour legend</h1>
        <nav class="lg-toc">${toc}</nav>
      </div>
      ${body}`;
  }

  /* The Viz tabs carry their own short key under the network. It is drawn
     from the same bands as the nodes, so it cannot drift from them. */
  function renderVizLegends() {
    const C = window.MM_COLORS;
    if (!C) return;
    const strength = C.COLOR_BANDS.vizThemeStrength;
    const themeItems = strength.bands.map(b =>
      `<span class="lg-item"><span class="lg-dot" style="background:${esc(C.themeFill(b.at, 1))}"></span>${esc(b.out.label)} (≥ ${b.at})</span>`)
      .concat(`<span class="lg-item"><span class="lg-dot" style="background:${esc(C.themeFill(0, 1))}"></span>${esc(strength.otherwise.label)}</span>`)
      .join('');
    const tickerItems = (table, name) => {
      const rows = bandRows(table, v => num(v, 0));
      return rows.filter(r => r.cond !== 'no data').map(r => {
        const cond = r.cond === 'any other value' ? `${name} lower` : `${name} ${r.cond}`;
        return `<span class="lg-item"><span class="lg-dot" style="background:${esc(r.out)}"></span>${esc(cond)}</span>`;
      }).join('');
    };
    const rings = '<span class="lg-item"><span class="lg-ring lg-tight"></span>Tight / inside day on MA</span>' +
      '<span class="lg-item"><span class="lg-ring lg-bridge"></span>Bridge (multi-theme)</span>' +
      '<span class="lg-item"><span class="lg-leader-dot"></span>Theme leader (glow)</span>';
    const sep = '<span class="lg-sep">|</span>';
    document.querySelectorAll('[data-viz-legend]').forEach(el => {
      const kind = el.dataset.vizLegend;
      const ticks = kind === 'vars' ? tickerItems(C.COLOR_BANDS.vizVars, 'VARS') : tickerItems(C.COLOR_BANDS.vizRs, 'RS');
      el.innerHTML = themeItems + sep + ticks + sep + rings +
        '<a class="lg-item lg-more" href="#" data-open-legends>All colours →</a>';
    });
  }

  function openLegends(anchor) {
    const btn = document.getElementById('tab-legends');
    if (btn) btn.click();
    if (anchor) {
      const el = document.getElementById(anchor);
      if (el) el.scrollIntoView({ block: 'start' });
    }
  }

  document.addEventListener('DOMContentLoaded', () => {
    renderVizLegends();
    render();
    const page = document.getElementById('legend-page');
    if (page) {
      // Samples are real `.tn-link` spans so they wear the real styles. Keep
      // their clicks away from the document-level ticker handlers, which would
      // otherwise move the sample selection and clear the sample row tint.
      page.addEventListener('click', (e) => {
        const a = e.target.closest('.lg-toc-link');
        if (a) {
          e.preventDefault();
          const el = document.getElementById(a.getAttribute('href').slice(1));
          if (el) el.scrollIntoView({ block: 'start' });
          return;
        }
        if (e.target.closest('.tn-link, a, button')) e.stopPropagation();
      });
    }
    // Re-render on every visit: the dimmed-ticker card quotes the live V/A
    // cutoffs, which the user may have changed on another tab.
    const btn = document.getElementById('tab-legends');
    if (btn) btn.addEventListener('click', render);
    document.addEventListener('click', (e) => {
      const a = e.target.closest('[data-open-legends]');
      if (!a) return;
      e.preventDefault();
      openLegends('lgs-viz');
    });
  });
})();
