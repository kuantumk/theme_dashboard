const assert = require('node:assert/strict');
const { readFileSync } = require('node:fs');
const { test } = require('node:test');
const vm = require('node:vm');
const path = require('node:path');

const source = readFileSync(path.join(__dirname, '../src/bidask/web/app.js'), 'utf8').replace(/\r\n/g, '\n');
function page() {
  const elements = new Map();
  const requests = [];
  const get = id => {
    if (!elements.has(id)) elements.set(id, { value: '0', textContent: '', innerHTML: '',
      addEventListener() {}, classList: { add() {}, remove() {}, toggle() {} } });
    return elements.get(id);
  };
  const context = vm.createContext({
    document: { getElementById: get, querySelectorAll: () => [] },
    window: { scrollY: 0, scrollTo() {} },
    setInterval() {}, clearInterval() {}, setTimeout() {},
    fetch: () => new Promise((resolve, reject) => requests.push({ resolve, reject })),
  });
  // Expose functions inside the production closure without reimplementing them.
  vm.runInContext(source.replace('  refresh();\n  setTimeout(schedule, 1000);',
    '  globalThis.app = { refresh, renderColumn, groupScore, getState: () => state };'), context);
  return { app: context.app, get, requests };
}
const scoring = { rvol_cap: 5, top_members: 3, breadth_coef: .5,
  breadth_min_members: 2, max_rows_per_column: 60, max_rows_per_group: 12 };
const member = (symbol, ratio, volume=10000) => ({ symbol, rvol_at_time: ratio,
  dollar_vol: volume * 100, volume });

function render(p, groups, settings=scoring) {
  const target = p.get('strong-body');
  p.app.renderColumn(groups, target, 'strong', '', { groups_total: groups.length,
    tickers_total: groups.reduce((n, g) => n + g.members.length, 0) },
    p.get('strong-meta'), 'market', settings);
  return target.innerHTML;
}

test('59-row prefix does not inflate the last group score or reorder it', () => {
  const p = page();
  const groups = Array.from({length:59}, (_, i) => ({ name: `Leading ${i}`,
    score: 3.5, roster: 1, members: [member(`L${i}`, 3.5)] }));
  groups.push({ name: 'Tail', score: 2.8333, roster: 3,
    members: [member('A',5), member('B',1), member('C',1)] });
  const html = render(p, groups);
  assert.equal((html.match(/class="chip"/g) || []).length, 60);
  assert.ok(html.indexOf('Tail') > html.indexOf('Leading 58'));
  assert.ok(html.includes('2.83</span>'));
  assert.ok(!html.includes('5.00</span>'));
});

test('filters recover previously hidden groups and remove singleton breadth', () => {
  const p = page();
  p.get('min-volume').value = '10'; // 10,000 shares
  const groups = [
    { name:'Removed', roster:1, score:5, members:[member('A',5,1000)] },
    { name:'Survivor', roster:2, score:2.5, members:[member('B',2,20000),member('C',2,1000)] },
  ];
  const html = render(p, groups, { ...scoring, max_rows_per_column:1 });
  assert.ok(!html.includes('Removed'));
  assert.ok(html.includes('Survivor'));
  assert.ok(html.includes('2.00</span>'));
});

test('filtered scores consume nondefault cap, coefficient, and membership minimum', () => {
  const p = page();
  const group = { members:[member('A',10), member('B',1), member('C',1)], roster:4, score:3 };
  assert.equal(p.app.groupScore(group, group.members.slice(0,2),
    {...scoring, rvol_cap:2, breadth_coef:.8, breadth_min_members:2}), 1.9);
  assert.equal(p.app.groupScore(group, group.members.slice(0,2),
    {...scoring, rvol_cap:2, breadth_coef:.8, breadth_min_members:3}), 1.5);
});

const response = marker => ({ ok:true, json:async () => ({ marker }) });
test('a late older response cannot overwrite an already displayed newer response', async () => {
  const p = page();
  const first = p.app.refresh();
  const second = p.app.refresh();
  p.requests[1].resolve(response('new'));
  await second;
  p.requests[0].resolve(response('old'));
  await first;
  assert.equal(p.app.getState().marker, 'new');
});

test('a late older failure cannot replace a newer healthy status', async () => {
  const p = page();
  const first = p.app.refresh();
  const second = p.app.refresh();
  p.requests[1].resolve(response('new'));
  await second;
  p.requests[0].reject(new Error('old failure'));
  await first;
  assert.notEqual(p.get('feed-pill').textContent, 'server unreachable');
});

test('responses still advance while a newer poll is pending on a slow connection', async () => {
  const p = page();
  const first = p.app.refresh();
  const second = p.app.refresh();
  p.requests[0].resolve(response('first'));
  await first;
  assert.equal(p.app.getState().marker, 'first');
  const third = p.app.refresh();
  p.requests[1].resolve(response('second'));
  await second;
  assert.equal(p.app.getState().marker, 'second');
  p.requests[2].resolve(response('third'));
  await third;
  assert.equal(p.app.getState().marker, 'third');
});
