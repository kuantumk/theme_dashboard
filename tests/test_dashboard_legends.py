"""Legends tab: the guards that keep it in step with the rest of the dashboard.

The Legends tab (docs/legends.js) describes every colour on the dashboard. It
stays current by construction, not by discipline:

1. **Thresholds live once.** The render sites in docs/app.js call `pickBand` on
   the `COLOR_BANDS` tables, and the legend reads the same tables through
   `window.MM_COLORS`. A render site that inlines a threshold again would leave
   the legend describing the old rule, so an inline threshold fails here.

2. **Colours live once.** Legend samples are painted with the real CSS
   classes, so a retuned colour needs no legend edit. But a NEW colour class
   needs a new card, and nothing on screen says one is missing. So every class
   that paints a hue (or dims) in style.css and is emitted by the page must be
   named in legends.js.

3. **Server rules are mirrored, and the mirror is pinned.** The day-pattern,
   tight, short and HOT rules are decided in Python and config. The browser
   copy in `PATTERN_RULES` is compared here against those sources, so a
   retuned server rule fails this module until the legend follows it.
"""

import re
import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / 'docs'
CSS = (DOCS / 'style.css').read_text(encoding='utf-8')
APP = (DOCS / 'app.js').read_text(encoding='utf-8')
HTML = (DOCS / 'index.html').read_text(encoding='utf-8')
LEGENDS = (DOCS / 'legends.js').read_text(encoding='utf-8')
CONFIG = yaml.safe_load((ROOT / 'config' / 'workflow_config.yaml').read_text(encoding='utf-8'))
INDICATORS = (ROOT / 'src' / 'indicators' / 'create_technical_indicators.py').read_text(encoding='utf-8')
EXPORT = (ROOT / 'src' / 'reporting' / 'export_dashboard_data.py').read_text(encoding='utf-8')

# Tokens whose value is a hue. Anything painted from these is a signal a reader
# has to decode, so it needs a legend entry. The greys (--text*, --bg*,
# --border*, --white) are structure, not signal.
HUED_VARS = {
    'green', 'gdim', 'red', 'rdim', 'amber', 'adim2', 'accent', 'accent2',
    'adim', 'yellow', 'ydim', 'tight', 'tight-dim', 'hl-short', 'hl-short-dim',
    'hl-ma-up', 'hl-ma-up-dim', 'hl-ma-split', 'hl-ma-split-dim', 'l1-bg',
    'l1-bg-hover', 'l1-edge', 'l1-hot-bg', 'l1-hot-bg-hover',
}
COLOUR_PROPS = ('color', 'background', 'border', 'outline', 'box-shadow', 'fill', 'stroke')

# Classes that paint a hue or dim, but are decoration rather than a meaning.
# Each entry needs a reason; an empty reason is a legend card waiting to be
# written.
NOT_A_SIGNAL = {
    'placeholder-icon': 'the faint emoji in an empty chart pane',
}


def _strip_js_comments(source):
    without_blocks = re.sub(r'/\*.*?\*/', '', source, flags=re.S)
    return re.sub(r'(?<![:/\\])//[^\n]*', '', without_blocks)


def _strip_css_comments(source):
    return re.sub(r'/\*.*?\*/', '', source, flags=re.S)


def _word(name):
    return re.compile(r'(?<![\w-])' + re.escape(name) + r'(?![\w-])')


def _is_grey(value):
    """True when a literal colour has equal channels (or is black/white)."""
    m = re.search(r'#([0-9a-fA-F]{6}|[0-9a-fA-F]{3})\b', value)
    if m:
        h = m.group(1)
        if len(h) == 3:
            h = ''.join(c * 2 for c in h)
        r, g, b = (int(h[i:i + 2], 16) for i in (0, 2, 4))
        return r == g == b
    m = re.search(r'rgba?\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)', value)
    if m:
        r, g, b = (int(x) for x in m.groups())
        return r == g == b
    m = re.search(r'hsla?\(\s*[\d.]+\s*,\s*([\d.]+)%', value)
    if m:
        return float(m.group(1)) == 0
    return True


def _paints_a_signal(body):
    for decl in body.split(';'):
        if ':' not in decl:
            continue
        prop, value = (x.strip() for x in decl.split(':', 1))
        if prop == 'opacity':
            try:
                if float(value) < 1:
                    return True
            except ValueError:
                pass
            continue
        if not prop.startswith(COLOUR_PROPS):
            continue
        for token in re.findall(r'var\(--([\w-]+)\)', value):
            if token in HUED_VARS:
                return True
        if re.search(r'#[0-9a-fA-F]{3,6}\b|rgba?\(|hsla?\(', value) and not _is_grey(value):
            return True
    return False


def _signal_selectors():
    """Every (selector, classes) whose rule paints a hue or dims."""
    css = _strip_css_comments(CSS)
    out = []
    for m in re.finditer(r'([^{}]+)\{([^{}]*)\}', css):
        if not _paints_a_signal(m.group(2)):
            continue
        for sel in m.group(1).split(','):
            classes = re.findall(r'\.([A-Za-z][\w-]*)', sel)
            if classes:
                out.append((sel.strip(), classes))
    return out


def _page_source():
    """Markup and script that ship to the other tabs — not the legend itself."""
    html = re.sub(r'<!--.*?-->', '', HTML, flags=re.S)
    return _strip_js_comments(APP) + '\n' + html


def _js_object(name):
    m = re.search(r'const ' + re.escape(name) + r' = \{(.*?)(?:\n  \};|\};\n)', APP, re.S)
    assert m is not None, f'{name} is gone from docs/app.js'
    return m.group(1)


def _js_object_or_array(name):
    m = re.search(r'const ' + re.escape(name) + r' = \[(.*?)\n  \];', APP, re.S)
    assert m is not None, f'{name} is gone from docs/app.js'
    return m.group(1)


class LegendsTabPlacementTests(unittest.TestCase):
    def test_the_tab_sits_immediately_right_of_si(self):
        tabs = re.findall(r'<button class="tab-btn[^"]*" data-tab="(\w+)"', HTML)
        self.assertIn('legends', tabs)
        self.assertEqual('si', tabs[tabs.index('legends') - 1],
                         'the Legends tab must sit directly to the right of SI')

    def test_the_tab_has_its_content_block(self):
        self.assertIn('id="content-legends"', HTML)
        self.assertIn('id="legend-page"', HTML)

    def test_legends_js_loads_after_app_js(self):
        """legends.js reads window.MM_COLORS, which app.js defines."""
        app = HTML.find('src="app.js')
        leg = HTML.find('src="legends.js')
        self.assertGreater(app, -1)
        self.assertGreater(leg, app, 'legends.js must load after app.js')

    def test_app_js_exports_the_registry(self):
        m = re.search(r'window\.MM_COLORS = Object\.freeze\(\{(.*?)\}\);', APP, re.S)
        self.assertIsNotNone(m, 'app.js no longer exports window.MM_COLORS')
        for key in ('COLOR_BANDS', 'pickBand', 'PATTERN_RULES', 'HL_TIER_CLASS',
                    'HL_TIER_TIP', 'VIZ_COLORS', 'CHART_MA_COLORS', 'MARKET_SESSIONS',
                    'rsFill', 'varsFill', 'themeFill'):
            self.assertIn(key, m.group(1), f'MM_COLORS no longer carries {key}')


class LegendCoverageTests(unittest.TestCase):
    def test_every_signal_class_on_the_page_has_a_legend_entry(self):
        """Rule 2. A class that paints a hue and ships to a tab needs a card.

        Proven by mutation: adding `.tn-link.hl-new { background: var(--amber); }`
        and emitting `hl-new` from a render site fails this test until
        legends.js names the class.
        """
        page = _page_source()
        # The legend draws every COLOR_BANDS table and every market session
        # from the registry (pinned below), so their outputs count as named.
        registry = (_js_object('COLOR_BANDS') + _js_object_or_array('MARKET_SESSIONS')
                    + _js_object('MARKET_CLOSED'))
        vocabulary = LEGENDS + '\n' + registry
        missing = set()
        for sel, classes in _signal_selectors():
            if any(c.startswith(('lg-', 'legend-')) for c in classes):
                continue   # the legend's own layout rules
            if any(c in NOT_A_SIGNAL for c in classes):
                continue
            if not all(_word(c).search(page) for c in classes):
                continue   # dead CSS: no tab emits it
            for c in classes:
                if not _word(c).search(vocabulary):
                    missing.add(f'{c}  (from `{sel}`)')
        self.assertFalse(
            missing,
            'these colour classes reach a tab but the Legends tab never shows '
            'them — add a card to docs/legends.js:\n  ' + '\n  '.join(sorted(missing)))

    def test_every_exemption_still_exists(self):
        """A stale exemption would hide a future class of the same name."""
        for cls in NOT_A_SIGNAL:
            self.assertTrue(_word(cls).search(CSS), f'{cls} is gone; drop the exemption')

    def test_every_band_table_is_drawn_by_the_legend(self):
        tables = re.findall(r'^    (\w+): \{$', _js_object('COLOR_BANDS'), re.M)
        self.assertGreater(len(tables), 10)
        for name in tables:
            self.assertRegex(
                LEGENDS, r'COLOR_BANDS\.' + name + r'\b|\bB\.' + name + r'\b',
                f'COLOR_BANDS.{name} colours a tab but the legend never draws it')

    def test_every_nasi_chart_colour_is_in_the_legend_figure(self):
        """renderNasiChart paints with literal tokens the NASI tests pin, so it
        cannot read them from the registry. Bind them to the legend instead."""
        start = APP.index('\n  function renderNasiChart(')
        end = APP.index('\n  function ', start + 10)
        chart = APP[start:end]
        m = re.search(r'function nasiSvg\(\)(.*?)\n  \}\n', LEGENDS, re.S)
        self.assertIsNotNone(m, 'nasiSvg is gone from legends.js')
        for token in sorted(set(re.findall(r"var\(--[\w-]+\)", chart))):
            self.assertIn(token, m.group(1),
                          f'renderNasiChart paints {token}; the legend figure does not')


class SingleSourceTests(unittest.TestCase):
    """Rule 1. A threshold inlined at a render site is invisible to the legend."""

    COLOUR_OUTPUTS = (
        'up', 'dn', 'pos', 'neg', 'neu', 'short-blue', 'short-white', 'rvol-high',
        'rvol-medium', 'rvol-low', 'ep-float-green', 'oversold', 'watch', 'overbought',
    )

    def _code_outside_registry(self):
        start = APP.index('  // ── COLOR SCHEME REGISTRY')
        end = APP.index('window.MM_COLORS = Object.freeze(')
        return _strip_js_comments(APP[:start] + APP[end:])

    def test_no_render_site_inlines_a_colour_threshold(self):
        code = self._code_outside_registry()
        outs = '|'.join(re.escape(o) for o in self.COLOUR_OUTPUTS)
        hits = re.findall(
            r"[<>]=?\s*-?\d+(?:\.\d+)?\s*\)?\s*\?\s*'(?:" + outs + r")'", code)
        self.assertEqual([], hits,
                         'a render site picks a colour from an inline threshold; '
                         'add the rule to COLOR_BANDS and call pickBand')

    def test_no_fill_function_returns_a_literal_colour(self):
        code = self._code_outside_registry()
        self.assertNotRegex(code, r"return\s+'#[0-9a-fA-F]{6}'",
                            'a node fill returns a literal; use COLOR_BANDS.viz*')

    def test_the_cytoscape_style_reads_every_colour_from_the_registry(self):
        start = APP.index('      style: [')
        end = APP.index('      wheelSensitivity', start)
        self.assertNotRegex(APP[start:end], r"'#[0-9a-fA-F]{3,6}'",
                            'a raw hex in the network style; add it to VIZ_COLORS')

    def test_the_chart_ma_colours_come_from_the_registry(self):
        self.assertIn('"moving average exponential.ma.color": CHART_MA_COLORS.ema', APP)
        self.assertIn('"moving average.ma.color": CHART_MA_COLORS.sma', APP)

    def test_every_banded_column_calls_pickband(self):
        code = self._code_outside_registry()
        expected = {
            'rs': 3, 'vars': 4, 'inst': 5, 'short': 5, 'pct': 1,
            'parabolicAtr': 1, 'epFloat': 1, 'epShort': 1, 'epDist52w': 1,
            'epAtr': 1, 'epRvol': 1, 'macroChange': 2, 'fearGreed': 1,
            'nasiOsc': 1, 'nasiRsi': 1, 'vizRs': 1, 'vizVars': 1,
            'vizThemeStrength': 1,
        }
        for table, n in expected.items():
            found = len(re.findall(r'pickBand\(COLOR_BANDS\.' + table + r'\b', code))
            self.assertGreaterEqual(found, n, f'COLOR_BANDS.{table}: {found} call sites, expected {n}')
        # The four breadth tiles index the table by key.
        self.assertGreaterEqual(code.count('pickBand(COLOR_BANDS[key], val)'), 2)

    def test_the_viz_keys_are_generated_not_hand_written(self):
        """The hand-written keys had drifted: a blue swatch for a cyan node,
        "score >= 100" for a band that starts at 65, and RS swatches on the
        Volume Viz tab, whose nodes are coloured by VARS."""
        self.assertEqual(4, len(re.findall(r'data-viz-legend="(?:rs|vars)"', HTML)))
        for stale in ('lg-rs90', 'lg-vars6', 'lg-blazing'):
            self.assertNotIn(stale, HTML)
            self.assertNotIn(stale, CSS)
        self.assertIn('data-viz-legend="vars"></div>', HTML.split('volume-network')[1][:1500])


class ServerRuleMirrorTests(unittest.TestCase):
    """Rule 3. PATTERN_RULES must match the Python and config it describes."""

    def _rule(self, key):
        m = re.search(r'\b' + key + r':\s*([\w.]+)', _js_object('PATTERN_RULES'))
        self.assertIsNotNone(m, f'PATTERN_RULES.{key} is gone')
        return m.group(1)

    def test_day_pattern_multipliers_match_the_indicator_pipeline(self):
        tight = re.search(r"daily\['tight_day'\] = .*?<\s*([\d.]+)\s*\*\s*daily\['adr_pct'\]", INDICATORS)
        self.assertIsNotNone(tight, 'tight_day moved; re-point this test')
        self.assertEqual(float(tight.group(1)), float(self._rule('tightDayBodyAdr')))
        near = re.findall(r"< ([\d.]+) \* daily\['atr14'\]", INDICATORS)
        self.assertEqual(2, len(near), 'close_to_ma moved; re-point this test')
        self.assertEqual({float(self._rule('closeToMaAtr'))}, {float(x) for x in near})

    def test_tight_rule_matches_the_tight_range_config(self):
        from src.indicators.create_technical_indicators import tight_range_config

        t = tight_range_config()
        pairs = {
            'tightMinWindow': 'min_window',
            'tightMaxWindow': 'max_window',
            'tightRatioMax': 'ratio_max',
            'tightPctileMax': 'pctile_max',
            'tightPctileLookback': 'pctile_lookback',
            'tightMaHoldAdr': 'ma_hold_adr',
            'tightEmaRolloverAdr': 'ema_rollover_adr',
            'tightSupportAdr': 'support_adr',
        }
        for js_key, cfg_key in pairs.items():
            self.assertEqual(float(t[cfg_key]), float(self._rule(js_key)),
                             f'PATTERN_RULES.{js_key} differs from tight_range.{cfg_key}')

    def test_short_floor_matches_the_ladder(self):
        from src.indicators.create_technical_indicators import HIGHLIGHT_SHORT_FLOOR

        self.assertEqual('SHORT_CROWDED_PCT', self._rule('shortFloorPct'))
        m = re.search(r'const SHORT_CROWDED_PCT = ([\d.]+);', APP)
        self.assertIsNotNone(m)
        self.assertEqual(HIGHLIGHT_SHORT_FLOOR, float(m.group(1)))

    def test_hot_rules_match_config_and_export(self):
        self.assertEqual(float(CONFIG['vars_tab']['hot_rs_threshold']),
                         float(self._rule('varsHotRs')))
        self.assertEqual(int(CONFIG['si_tab']['hot_radar_rank']),
                         int(self._rule('siHotRadarRank')))
        m = re.search(r"avg_rs >= hot_rs and len\(members\) >= (\d+)", EXPORT)
        self.assertIsNotNone(m, 'the VARS HOT member minimum moved; re-point this test')
        self.assertEqual(int(m.group(1)), int(self._rule('varsHotMinMembers')))


if __name__ == '__main__':
    unittest.main()
