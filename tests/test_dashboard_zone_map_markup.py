"""Zone map: the browser mirror, the renderer and the markup.

The server decides every zone (export_dashboard_data.build_zone_map). The
browser mirrors the limits in ZONE_RULES only to draw rails, the box and the
Legends tab, so the mirror is pinned to the config here.
"""

import re
import unittest
from pathlib import Path

import yaml

from src.reporting import export_dashboard_data as ex

ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT / 'docs' / 'app.js').read_text(encoding='utf-8')
HTML = (ROOT / 'docs' / 'index.html').read_text(encoding='utf-8')
CONFIG = yaml.safe_load((ROOT / 'config' / 'workflow_config.yaml').read_text(encoding='utf-8'))

#: Every Viz tab draws a zone map.
ZONE_TABS = ('themeviz', 'varsviz', 'momentumviz', 'volumeviz')


def _zone_section():
    start = APP.index('  // ── ZONE MAP')
    end = APP.index('  // ── END ZONE MAP', start)
    return APP[start:end]


def _fn(name):
    """One top-level function, up to the next function or section banner."""
    start = APP.index(f'\n  function {name}(')
    ends = [i for i in (APP.find('\n  function ', start + 10), APP.find('\n  // ──', start + 10))
            if i != -1]
    return APP[start:min(ends)]


class ZoneRulesMirrorTests(unittest.TestCase):
    def _rules(self):
        m = re.search(r'const ZONE_RULES = \{(.*?)\n  \};', APP, re.S)
        self.assertIsNotNone(m, 'ZONE_RULES is gone from docs/app.js')
        return m.group(1)

    def test_every_limit_matches_the_config(self):
        rules, cfg = self._rules(), CONFIG['zone_map']
        for js, py in (('bandLow', 'band_low'), ('bandHigh', 'band_high'),
                       ('weeklyLow', 'weekly_low'), ('weeklyHigh', 'weekly_high')):
            m = re.search(r'\b' + js + r':\s*(-?[\d.]+)', rules)
            self.assertIsNotNone(m, js)
            self.assertEqual(float(cfg[py]), float(m.group(1)), js)
        for js, py in (('xRange', 'x_range'), ('yRange', 'y_range')):
            m = re.search(r'\b' + js + r':\s*\[\s*(-?[\d.]+),\s*(-?[\d.]+)\s*\]', rules)
            self.assertIsNotNone(m, js)
            self.assertEqual([float(v) for v in cfg[py]], [float(m.group(1)), float(m.group(2))], js)

    def test_the_states_match_the_server_in_order(self):
        m = re.search(r'const ZONE_STATES = \[(.*?)\n  \];', APP, re.S)
        self.assertIsNotNone(m, 'ZONE_STATES is gone from docs/app.js')
        self.assertEqual(list(ex.ZONE_STATES), re.findall(r"\bid: '(\w+)'", m.group(1)))

    def test_the_config_matches_the_server_defaults(self):
        for key, default in ex.ZONE_MAP_DEFAULTS.items():
            self.assertEqual(default, CONFIG['zone_map'][key], key)


class ZoneRendererTests(unittest.TestCase):
    def test_the_section_paints_with_classes_never_literals(self):
        self.assertNotRegex(_zone_section(), r"#[0-9a-fA-F]{3,6}\b",
                            'a raw colour in the zone map; paint it with a CSS class')

    def test_an_absent_block_says_when_data_arrives(self):
        self.assertIn('Zone map data arrives with the next daily run.', _zone_section())

    def test_momentum_prints_rs_and_the_rest_print_vars(self):
        modes = re.search(r'const ZONE_MODES = \{(.*?)\n  \};', APP, re.S).group(1)
        self.assertRegex(modes, r"momentum:\s*\{[^}]*metric:\s*'rs'")
        for mode in ('themes', 'vars', 'volume'):
            self.assertRegex(modes, mode + r":\s*\{[^}]*metric:\s*'vars'")

    def test_the_row_number_reads_the_shared_bands(self):
        body = _fn('zoneNum')
        self.assertIn('pickBand(COLOR_BANDS.rs', body)
        self.assertIn('pickBand(COLOR_BANDS.vars', body)

    def test_list_tickers_are_real_links_with_the_tier_tint(self):
        body = _fn('zoneTickerLink')
        for token in ('tn-link', 'hlClass(', 'hlTitle(', 'day-pattern-green', 'data-sym='):
            self.assertIn(token, body)

    def test_arrow_keys_skip_the_zone_map_lists(self):
        """Review finding: `.viz-left` is a `.left-panel`, so arrow keys walked
        the states and clusters lists — each ticker came up twice, and the dot
        ring stayed on the old name. Arrow keys on the map are out of scope."""
        body = _fn('getTickerLinksForTab')
        self.assertIn("closest('.zone-map')", body)

    def test_the_footer_counts_only_themes_that_hold_a_remaining_name(self):
        """Review finding: the footer counted every hidden leaf, including
        leaves whose names already sit in a shown row."""
        body = _fn('zoneClustersCard')
        self.assertIn('remainingSet.has(', body)
        self.assertNotIn('const rest = leaves.length - shown.length', body)

    def test_fixed_text_reserves_its_space_before_any_label(self):
        """Seen in the browser: the buy-area tag drew on top of a ticker label.
        The tag and both rail labels must be in `placed` before the label pass."""
        body = _fn('zoneScatterSvg')
        reserve = body.index('placed.push(...fixedText)')
        first_label = body.index('placed.push(box)')
        self.assertLess(reserve, first_label)
        for text in ('Buy area · ', 'weekly EMA20', 'weekly extended'):
            self.assertIn(text, body)

    def test_the_map_carries_no_title_or_caption(self):
        """The user asked for both to go: the "Where is each name?" title and
        the "N names: members of ..." caption had no use."""
        for text in ('Where is each name?', 'zm-caption', 'Left off:'):
            self.assertNotIn(text, _zone_section(), text)

    def test_text_keeps_its_size_when_the_divider_moves(self):
        """With a fixed viewBox the SVG scaled its text with the panel. The
        viewBox now equals the pixel width, and every resize path redraws."""
        draw = _fn('drawZoneScatter')
        self.assertIn("setAttribute('width', g.w)", draw)
        self.assertIn("setAttribute('viewBox', `0 0 ${g.w} ${g.h}`)", draw)
        self.assertNotIn('const ZM = {', APP)
        self.assertIn('redrawZoneMapsSoon();', _fn('initResizablePanels'))
        self.assertIn('redrawZoneMapsNow();', _fn('initResizablePanels'))
        self.assertIn('redrawZoneMapsNow();', _fn('initTabs'))
        self.assertIn("addEventListener('resize', redrawZoneMapsSoon)", _fn('initZoneMaps'))

    def test_every_name_gets_a_label(self):
        """No label is skipped: a crowded name moves out on a leader line, and
        the last fallback still places it beside its dot."""
        body = _fn('zoneScatterSvg')
        self.assertNotIn('if (hit) return;', body)
        self.assertNotIn('q.box\n        ? ', body)
        self.assertIn('zm-leader', body)
        self.assertIn('zmLabelSpots(', body)

    def test_a_redraw_keeps_the_selection(self):
        self.assertIn('el._zmSel = sym;', _fn('selectZoneTicker'))
        self.assertIn('el._zmSel', _fn('drawZoneScatter'))

    def test_an_off_scale_name_is_clamped_and_titled_with_its_real_value(self):
        body = _fn('zoneScatterSvg')
        self.assertIn('zmClamp(', body)
        self.assertIn('zoneTriangle(', body)
        self.assertIn('t.d_daily', body)
        self.assertIn('<title>', body)


class ZoneMarkupTests(unittest.TestCase):
    def test_each_zone_tab_has_its_container_and_key(self):
        for tab in ZONE_TABS:
            block = HTML.split(f'id="content-{tab}"')[1].split('class="tab-content"')[0]
            self.assertIn(f'id="{tab}-zone"', block, tab)
            self.assertIn('data-viz-legend="zone"', block, tab)
            self.assertNotIn('theme-network', block, tab)

    def test_a_list_click_on_a_zone_tab_opens_the_chart(self):
        """initTickerClicks opens the chart for any tab with a chart pane."""
        self.assertIn("tabId + '-chart-area'", _fn('initTickerClicks'))
        for tab in ZONE_TABS:
            self.assertIn(f'id="{tab}-chart-area"', HTML, tab)

    def test_no_network_code_remains(self):
        LEGENDS = (ROOT / 'docs' / 'legends.js').read_text(encoding='utf-8')
        CSS = (ROOT / 'docs' / 'style.css').read_text(encoding='utf-8')
        for token in ('cytoscape', 'renderNetwork', 'radarVizSnap', 'varsVizSnap',
                      'VIZ_COLORS', 'VIZ_HOT', 'vizRs', 'vizVars', 'vizThemeStrength',
                      'theme-network', 'tight-pulse-ring'):
            for name, text in (('app.js', APP), ('index.html', HTML),
                               ('legends.js', LEGENDS), ('style.css', CSS)):
                self.assertNotIn(token, text, f'{token} still in {name}')


if __name__ == '__main__':
    unittest.main()
