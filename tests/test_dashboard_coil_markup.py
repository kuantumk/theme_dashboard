"""Coil marker markup: the joins across app.js and style.css that nothing on
screen would reveal if they broke.

Three rules are pinned here because each one fails silently:

1. The chip marker sets NO layout-affecting property. Chip width drives how
   radar chip rows wrap, and `syncRadarClamps` measures that wrapping to derive
   the "+N more" count — a width change would move that count whenever a chip
   coiled. This is the same rule `.radar-chip.filtered-out` carries.

2. The coil rules sit AFTER the screened/quiet rules, and the cutoff dim wins
   over the coil's opacity restore. `.radar-chip.coiled.chip-quiet` raises
   opacity so the tint survives the unscreened state (the majority state for a
   coiling stock, whose low RS is what keeps it out of the screeners); an armed
   V/A cutoff must still push the chip back, and that needs equal specificity
   rather than source order alone.

3. The sort ranks by coil SHARE with a minimum count, never by raw count. L1
   member counts run 2 to 214 on live data, so a raw count ranks by roster size
   and buries the small densely-coiled theme the control exists to surface.
"""

import re
import unittest
from pathlib import Path

DOCS = Path(__file__).resolve().parents[1] / 'docs'
CSS = (DOCS / 'style.css').read_text(encoding='utf-8')
APP = (DOCS / 'app.js').read_text(encoding='utf-8')

LAYOUT_PROPS = (
    'font-weight', 'font-size', 'padding', 'margin', 'border-width',
    'letter-spacing', 'display', 'width', 'line-height',
)


def _rule_body(selector):
    """Return the declaration block for an exact selector, or None."""
    m = re.search(re.escape(selector) + r'\s*\{([^}]*)\}', CSS)
    return m.group(1) if m else None


class CoilChipStyleTests(unittest.TestCase):
    def test_coil_chip_rule_exists_and_sets_a_background(self):
        body = _rule_body('.radar-chip.coiled')
        self.assertIsNotNone(body, '.radar-chip.coiled rule is missing')
        self.assertIn('background', body)

    def test_coil_chip_sets_no_layout_affecting_property(self):
        body = _rule_body('.radar-chip.coiled')
        for prop in LAYOUT_PROPS:
            self.assertNotIn(
                prop, body,
                f'{prop} in .radar-chip.coiled changes chip width, which '
                f'syncRadarClamps measures for the "+N more" count')

    def test_coil_rules_come_after_the_screened_and_quiet_rules(self):
        # Match the rule, not a comment that names the selector.
        def rule_at(selector):
            m = re.search(re.escape(selector) + r'\s*\{', CSS)
            self.assertIsNotNone(m, f'{selector} rule is missing')
            return m.start()

        coil = rule_at('.radar-chip.coiled')
        self.assertGreater(coil, rule_at('.radar-chip.chip-screened'))
        self.assertGreater(coil, rule_at('.radar-chip.chip-quiet'))

    def test_unscreened_coiled_chip_keeps_most_of_its_opacity(self):
        # Without this the tint renders at 55% on the majority of coiled chips.
        body = _rule_body('.radar-chip.coiled.chip-quiet')
        self.assertIsNotNone(body)
        m = re.search(r'opacity:\s*([\d.]+)', body)
        self.assertIsNotNone(m)
        self.assertGreater(float(m.group(1)), 0.55)

    def test_an_armed_cutoff_still_dims_a_coiled_chip(self):
        # Equal specificity to the restore above, and later in source order.
        body = _rule_body('.radar-chip.coiled.filtered-out')
        self.assertIsNotNone(
            body, 'without this, the 3-class opacity restore outranks '
                  '.radar-chip.filtered-out and a dimmed coiled chip stays bright')
        m = re.search(r'opacity:\s*([\d.]+)', body)
        self.assertIsNotNone(m)
        self.assertLessEqual(float(m.group(1)), 0.4)
        self.assertGreater(
            CSS.index('.radar-chip.coiled.filtered-out'),
            CSS.index('.radar-chip.coiled.chip-quiet'))

    def test_coil_colour_is_not_a_colour_another_state_owns(self):
        m = re.search(r'--coil:\s*([^;]+);', CSS)
        self.assertIsNotNone(m)
        coil = m.group(1).strip().lower()
        for token in ('--green', '--amber', '--yellow', '--accent2'):
            other = re.search(re.escape(token) + r':\s*([^;]+);', CSS)
            if other:
                self.assertNotEqual(coil, other.group(1).strip().lower())


class CoilBadgeTests(unittest.TestCase):
    def test_badge_has_its_own_class_not_the_dim_metadata_styling(self):
        self.assertIsNotNone(_rule_body('.coil-badge'))
        self.assertIn('coil-badge', APP)

    def test_badge_renders_only_when_the_count_is_present_and_non_zero(self):
        # `${leaf.n_coiled ? ... : ''}` — an absent field must render nothing,
        # which is distinct from a theme that has zero coiled members.
        self.assertIn('leaf.n_coiled ?', APP)
        self.assertIn('grp.n_coiled ?', APP)


class CoilStripTests(unittest.TestCase):
    """The pinned strip is the feature's answer to the original miss.

    A marker inside a rank-27 block is unreachable by a reader who stops at
    rank 10, so the strip names the leading coiled themes ABOVE the board at
    any rank. These pin the two properties that would silently undo that:
    ranking the pins by raw count (which just lists the biggest rosters), and
    letting the jump flash resize the block it lands on.
    """

    def test_strip_renders_above_the_theme_blocks(self):
        strip = APP.index('coil-strip')
        blocks = APP.index('class="theme-block" data-l1=')
        self.assertLess(strip, blocks)
        self.assertIsNotNone(_rule_body('.coil-strip'))

    def test_pins_rank_by_share_not_raw_count(self):
        # Reuses byCoil, which divides by the member count. A separate
        # raw-count sort here would reintroduce the size bias the sort avoids.
        seg = APP[APP.index('const pinned = l1s'):][:400]
        self.assertIn('.sort(byCoil)', seg)
        self.assertIn('COIL_SORT_MIN', seg)
        self.assertIn('COIL_PINNED_MAX', seg)

    def test_pin_count_is_small_enough_to_glance_at(self):
        m = re.search(r'const COIL_PINNED_MAX = (\d+)', APP)
        self.assertIsNotNone(m)
        self.assertLessEqual(int(m.group(1)), 8)

    def test_empty_state_names_its_own_cause(self):
        # A blank strip must say no theme qualified, not look broken.
        self.assertIn('coil-strip-empty', APP)
        self.assertIn('no theme has', APP)
        self.assertIsNotNone(_rule_body('.coil-strip-empty'))

    def test_jump_flash_cannot_resize_the_block(self):
        # outline is drawn outside the box model; border/padding would shift
        # the block and, inside it, re-wrap the chip rows syncRadarClamps
        # measured for the "+N more" count.
        body = _rule_body('.theme-block.coil-jump')
        self.assertIsNotNone(body)
        self.assertIn('outline', body)
        for prop in ('border:', 'border-width', 'padding', 'margin', 'width'):
            self.assertNotIn(prop, body)

    def test_pin_targets_a_block_that_carries_the_matching_attribute(self):
        # The pin reads data-l1 and queries .theme-block[data-l1=...]; if the
        # block ever stops emitting it, every pin silently does nothing.
        self.assertIn('data-l1="${escAttr(grp.name)}"', APP)
        self.assertIn('.theme-block[data-l1=', APP)
        self.assertIn('CSS.escape(pin.dataset.l1)', APP)


class CoilSortTests(unittest.TestCase):
    def test_sort_uses_share_with_a_minimum_count(self):
        self.assertIn('COIL_SORT_MIN', APP)
        m = re.search(r'const COIL_SORT_MIN = (\d+)', APP)
        self.assertIsNotNone(m)
        self.assertGreaterEqual(int(m.group(1)), 2)
        # The key divides by the member count; a raw-count sort would not.
        key = re.search(r'function coilKey\(entry\)\s*\{(.*?)\n  \}', APP, re.S)
        self.assertIsNotNone(key)
        self.assertIn('/ total', key.group(1))

    def test_sort_is_off_by_default(self):
        self.assertIn('let radarCoilSort = false', APP)

    def test_sort_reorders_without_touching_scores_or_visibility(self):
        # It sorts copies and re-renders from the same snapshot; it must not
        # recompute a score or set a filter class.
        body = re.search(r'function toggleCoilSort\(\)\s*\{(.*?)\n  \}', APP, re.S)
        self.assertIsNotNone(body)
        for forbidden in ('composite', 'boosted =', 'filtered-out', 'tickerFilters'):
            self.assertNotIn(forbidden, body.group(1))
        self.assertIn('slice().sort(byCoil)', APP)

    def test_filters_and_clamps_rerun_after_a_reorder(self):
        # Re-ordering changes which chips land in which row, so the "+N more"
        # count must be recomputed rather than carried across the re-render.
        # Assert ORDER, not proximity: a character window breaks the moment
        # anything is inserted between the handler and these calls, which
        # pins the spelling rather than the behaviour.
        wire = APP.index('const coilBtn = container.querySelector')
        filters = APP.index('applyTickerFilters()', wire)
        clamps = APP.index('syncRadarClamps(container)', wire)
        render_end = APP.index('// ── RADAR CHIP CLAMPING', wire)
        self.assertLess(filters, render_end)
        self.assertLess(clamps, render_end)
        self.assertLess(filters, clamps)


if __name__ == '__main__':
    unittest.main()
