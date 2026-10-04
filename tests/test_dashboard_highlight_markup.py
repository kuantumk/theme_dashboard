"""Highlight-tier markup: the joins across app.js and style.css that nothing on
screen would reveal if they broke.

The server picks one rung per ticker — a tight base, a crowded short, stacked
averages, or a stack below the 50-day — and publishes it as `highlight`. The
browser turns that one value into a background tint and a tooltip. Five rules
are pinned here, each one silent on failure:

1. **A tier rule paints a BACKGROUND and nothing else.** Colour belongs to the
   green day-pattern flag, yellow to selection, blue to the screened outline and
   hover. `.tn-link.day-pattern-green` and `.tn-link.hl-ma-up` are both
   two-class selectors and the tier rule is the later one, so a `color` there
   would win and blank the day-pattern marker on exactly the stocks that carry
   both. The same reasoning forbids a width, because chip width drives radar
   chip-row wrapping and `syncRadarClamps` measures that for the "+N more"
   count.

2. **Every rung is written twice, once for the chip and once for the plain
   span.** `renderThemes` is the only renderer that adds the `radar-chip` class,
   so a chip-only rule ships the tint invisible on the other eight list tabs.
   The tight rung needs its second rule for that exact reason: it reached the
   eight table tabs with no plain-span rule to draw it.

3. **The opacity ladder holds in both directions.** A tier restores opacity
   under `chip-quiet`, because a basing stock's low RS is what keeps it out of
   the screeners and the unscreened dim would swallow an already faint tint. An
   armed V/A cutoff must still win, and that needs equal specificity rather
   than source order alone. So must selection: the tier opacity rules are three
   classes against the plain selection rule's two.

4. **The tight rung reaches the table tabs through the tier field, and the
   Themes chip through the radar's own boolean.** Those are two different
   inputs for one class, and only one of them exists per tab.

5. **Every tint is named in words.** The tiers are otherwise colour-only, and
   green and orange are the two closest hues on the board. The split-stack
   wording states where the averages sit and asserts no direction — one reading
   covers a stock reclaiming its short-term averages and a stock that has just
   lost its 50-day.
"""

import re
import unittest
from pathlib import Path

DOCS = Path(__file__).resolve().parents[1] / 'docs'
CSS = (DOCS / 'style.css').read_text(encoding='utf-8')
APP = (DOCS / 'app.js').read_text(encoding='utf-8')

# The same list the tight markup test pins, for the same reason.
LAYOUT_PROPS = (
    'font-weight', 'font-size', 'padding', 'margin', 'border-width',
    'letter-spacing', 'display', 'width', 'line-height',
)

# The three rungs the tier field alone carries. Tight is the fourth and is
# spelled `tight-range`, because renaming it would break the tight markup test and
# the invariants that module pins, for no gain.
NEW_TIERS = ('hl-short', 'hl-ma-up', 'hl-ma-split')

# The nine list tabs of R12, in render order, and the function that draws each.
RENDER_SITES = (
    'renderThemes',          # Themes — radar chips
    'renderMomentum',
    'renderVolume',
    'renderVARS',
    'renderSI',
    'renderParabolicTable',
    'renderIndustryTable',   # Industry ETFs
    'renderETFTable',        # Lev ETFs
    'epRow',                 # shared by both EP tables
)


def _strip_js_comments(source):
    """Drop `//` and `/* */` comments so a mention cannot pass for a call.

    Proven by mutation: deleting the real `${hlClass(t.highlight)}` from a render
    site and leaving a stale comment that names `hlClass(...)` satisfied the
    per-site check while the tab rendered no tint at all. A comment is the one
    thing that looks like the call and does nothing.
    """
    without_blocks = re.sub(r'/\*.*?\*/', '', source, flags=re.S)
    return re.sub(r'(?<![:/])//[^\n]*', '', without_blocks)


def _rule_body(selector):
    """Return the declaration block for an exact selector, or None."""
    m = re.search(re.escape(selector) + r'\s*\{([^}]*)\}', CSS)
    return m.group(1) if m else None


def _rule_start(selector):
    """Character offset of a RULE with this selector — not of a comment that
    merely names it."""
    m = re.search(re.escape(selector) + r'\s*\{', CSS)
    return None if m is None else m.start()


def _group(*selectors):
    """(offset, declaration block) of a comma-separated group whose head
    contains every one of these selectors, or (None, None). Written so a group
    may be reordered or re-split without the test pinning its spelling."""
    for m in re.finditer(r'(?:^|\})\s*((?:[^{}]*?))\{([^}]*)\}', CSS, re.M):
        head = m.group(1)
        if all(sel in head for sel in selectors):
            return m.start(1), m.group(2)
    return None, None


def _group_body(*selectors):
    return _group(*selectors)[1]


def _fn_body(name):
    """Source of one top-level function in app.js, up to the next top-level
    function or section banner."""
    start = re.search(r'\n  function ' + re.escape(name) + r'\s*\(', APP)
    assert start is not None, f'{name} is gone from docs/app.js'
    marks = [m.start() for m in re.finditer(r'\n  function \w+\s*\(', APP)]
    marks += [m.start() for m in re.finditer('\n  // ──', APP)]
    end = min((x for x in sorted(marks) if x > start.start()), default=len(APP))
    return APP[start.start():end]


class HighlightTintStyleTests(unittest.TestCase):
    def test_every_rung_is_drawn_on_the_chip_and_on_the_plain_span(self):
        """Rule 2. One renderer applies `radar-chip`; eight draw a bare span."""
        for tier in NEW_TIERS:
            for base in ('.radar-chip', '.tn-link'):
                body = _rule_body(f'{base}.{tier}')
                self.assertIsNotNone(
                    body, f'{base}.{tier} rule is missing — the tint would not '
                          f'render on the tabs that use {base}')
                self.assertIn('background', body)

    def test_the_tight_tint_reaches_the_table_tabs(self):
        """The one rule without which KD1 ships invisible.

        The stylesheet's only tight background was `.radar-chip.tight-range`, and
        `renderThemes` is the sole place that class is applied. So the eight
        table tabs received the `tight-range` class with nothing to paint it.
        """
        body = _rule_body('.tn-link.tight-range')
        self.assertIsNotNone(
            body, 'without this the tight tint renders on the Themes chip only, '
                  'and the ladder\'s top rung is invisible on eight of nine tabs')
        self.assertIn('--tight-dim', body)

    def test_no_tier_rule_sets_a_layout_affecting_property(self):
        for tier in NEW_TIERS + ('tight-range',):
            for base in ('.radar-chip', '.tn-link'):
                body = _rule_body(f'{base}.{tier}')
                if body is None:
                    continue
                for prop in LAYOUT_PROPS:
                    self.assertNotIn(
                        prop, body,
                        f'{prop} in {base}.{tier} changes the element\'s width; '
                        f'syncRadarClamps measures chip wrapping for the '
                        f'"+N more" count')

    def test_no_tier_rule_sets_a_colour(self):
        """Rule 1, and it matters more on the plain span than on the chip.

        `.tn-link.day-pattern-green` sets `color` at two classes and sits
        earlier in the file. A tier rule is also two classes and sits later, so
        a `color` there wins on source order and the green day-pattern marker
        vanishes on every ticker that carries both signals.
        """
        for tier in NEW_TIERS + ('tight-range',):
            for base in ('.radar-chip', '.tn-link'):
                body = _rule_body(f'{base}.{tier}')
                if body is None:
                    continue
                self.assertNotIn(
                    'color:', body.replace('background-color', ''),
                    f'{base}.{tier} sets a colour; the green day-pattern text '
                    f'and the yellow selection own that channel')

    def test_a_plain_span_tier_rule_paints_only_a_background(self):
        """`.tn-link.active-ticker` is two classes and sits ~1,350 lines earlier.

        A tier rule ties its specificity and wins on source order, so anything
        it sets beyond `background` is taken from the selected ticker. The
        selection there is a colour plus a solid underline; a border declaration
        in a tier rule would silently flatten it.
        """
        for tier in NEW_TIERS + ('tight-range',):
            body = _rule_body(f'.tn-link.{tier}')
            if body is None:
                continue
            props = [d.split(':')[0].strip() for d in body.split(';') if ':' in d]
            self.assertEqual(
                ['background'], props,
                f'.tn-link.{tier} sets {props}; only `background` is safe '
                f'against .tn-link.active-ticker, which sits earlier at equal '
                f'specificity')

    def test_tier_rules_come_after_the_screened_and_quiet_rules(self):
        screened = _rule_start('.radar-chip.chip-screened')
        quiet = _rule_start('.radar-chip.chip-quiet')
        self.assertIsNotNone(screened)
        self.assertIsNotNone(quiet)
        for tier in NEW_TIERS:
            start = _rule_start(f'.radar-chip.{tier}')
            self.assertIsNotNone(start, f'.radar-chip.{tier} rule is missing')
            self.assertGreater(
                start, screened,
                f'.radar-chip.{tier} must follow chip-screened, which sets the '
                f'whole border-color at equal specificity')
            self.assertGreater(start, quiet)

    def test_an_unscreened_tinted_chip_keeps_most_of_its_opacity(self):
        """Rule 3. The unscreened dim is the common state for these rungs.

        A basing stock's low RS is what keeps it out of the screeners, and the
        tints are faint to begin with, so 0.55 would swallow them.
        """
        body = _group_body(*[f'.radar-chip.{t}.chip-quiet' for t in NEW_TIERS])
        self.assertIsNotNone(
            body, 'no chip-quiet opacity restore for the tier rungs')
        m = re.search(r'opacity:\s*([\d.]+)', body)
        self.assertIsNotNone(m)
        self.assertGreater(float(m.group(1)), 0.55)

    def test_an_armed_cutoff_still_dims_a_tinted_chip(self):
        """The cutoff is an explicit request to push the chip back, and it
        outranks our wish to advertise a rung.

        It needs equal specificity to the restore above — three classes — plus
        a later position. `.radar-chip.filtered-out` alone is two classes and
        loses.
        """
        dim_at, body = _group(*[f'.radar-chip.{t}.filtered-out' for t in NEW_TIERS])
        self.assertIsNotNone(
            body, 'without this the 3-class opacity restore outranks '
                  '.radar-chip.filtered-out and a dimmed tinted chip stays bright')
        m = re.search(r'opacity:\s*([\d.]+)', body)
        self.assertIsNotNone(m)
        self.assertLessEqual(float(m.group(1)), 0.4)
        restore_at, _ = _group(*[f'.radar-chip.{t}.chip-quiet' for t in NEW_TIERS])
        self.assertGreater(dim_at, restore_at,
                           'the dim must follow the restore it overrides')

    def test_selection_survives_on_a_tinted_chip(self):
        """AE5. A selected, dimmed, tinted chip reads as selected.

        `.radar-chip.active-ticker` is two classes. The tier opacity rules are
        three, and the dim pair is effectively four once `filtered-out` joins,
        so both outrank plain selection regardless of order. Without an override
        at matching specificity a selected chip in a tier renders at 0.85
        unscreened and 0.4 under an armed cutoff, where the same chip untinted
        renders at 1.
        """
        selectors = []
        for tier in NEW_TIERS:
            selectors.append(f'.radar-chip.{tier}.active-ticker')
            selectors.append(f'.radar-chip.{tier}.filtered-out.active-ticker')
        sel_at, body = _group(*selectors)
        self.assertIsNotNone(
            body, 'no equal-specificity override — a selected chip in a tier '
                  'loses its selection to the tier opacity rules')
        self.assertIn('opacity: 1', body)
        self.assertIn('--yellow', body)
        dim_at, _ = _group(*[f'.radar-chip.{t}.filtered-out' for t in NEW_TIERS])
        self.assertGreater(
            sel_at, dim_at,
            'the selection override must follow the rules it beats')

    def test_no_painted_tint_borrows_the_selection_yellow(self):
        """The base hues are documentation; the `*-dim` values are what paint.

        Checking the base tokens alone leaves the real hole open: setting
        `--hl-ma-split-dim` to the selection yellow at 0.16 alpha passed both the
        base-token check and the distinctness check, because neither compares a
        painted value against `--yellow`. A tint that reads as selection is the
        one collision this whole colour scheme exists to avoid.
        """
        yellow = re.search(r'--yellow:\s*#?([0-9a-fA-F]{6})\s*;', CSS)
        self.assertIsNotNone(yellow, '--yellow is gone')
        r, g, b = (int(yellow.group(1)[i:i + 2], 16) for i in (0, 2, 4))
        for token in ('tight-dim',) + tuple(f'{t}-dim' for t in NEW_TIERS):
            m = re.search(r'--' + re.escape(token) + r':\s*([^;]+);', CSS)
            self.assertIsNotNone(m, f'--{token} token is missing')
            channels = re.findall(r'\d+', m.group(1))
            self.assertGreaterEqual(
                len(channels), 3, f'--{token} is not an rgb/rgba value')
            self.assertNotEqual(
                (r, g, b), tuple(int(c) for c in channels[:3]),
                f'--{token} paints the selection yellow')

    def test_no_tier_borrows_the_selection_yellow(self):
        """R11. Yellow marks the selected ticker on every tab, so no rung may
        take it — the two would be indistinguishable on a dense panel."""
        yellow = re.search(r'--yellow:\s*([^;]+);', CSS).group(1).strip().lower()
        for tier in NEW_TIERS:
            m = re.search(r'--' + re.escape(tier) + r':\s*([^;]+);', CSS)
            self.assertIsNotNone(m, f'--{tier} token is missing')
            self.assertNotEqual(yellow, m.group(1).strip().lower())

    def test_the_span_reserves_its_tint_box_without_occupying_width(self):
        """The padding and the negative margin must stay equal and opposite.

        A background on a bare inline span hugs the glyphs, so `.tn-link` pads
        itself; the negative margin gives the width back. Keep only the padding
        and eight tables widen, which invalidates the six panel widths
        `tests/test_dashboard_panel_layout.py` pins — and that module compares CSS
        literals rather than measuring a rendered table, so it would stay green
        while every one of those widths was wrong. Nothing else pins this pair.
        """
        body = _rule_body('.tn-link')
        self.assertIsNotNone(body, '.tn-link rule is missing')
        pad = re.search(r'padding:\s*0\s+(\d+)px', body)
        margin = re.search(r'margin:\s*0\s+-(\d+)px', body)
        self.assertIsNotNone(pad, '.tn-link lost the padding its tint needs')
        self.assertIsNotNone(
            margin, '.tn-link has padding with no negative margin to cancel it')
        self.assertEqual(
            pad.group(1), margin.group(1),
            'the padding and negative margin must cancel exactly, or the eight '
            'table tabs widen and the pinned panel widths are all wrong')

    def test_each_rung_paints_its_own_tint(self):
        """Four rungs sharing a tint would collapse the ladder into one signal.

        The BASE hues may legitimately repeat — `--hl-ma-up` is the `--green`
        value on purpose, so the tint composes with the green day-pattern text
        rather than fighting it. The painted `*-dim` values are what must differ.
        """
        painted = {}
        for token in ('tight-dim', 'hl-short-dim', 'hl-ma-up-dim', 'hl-ma-split-dim'):
            m = re.search(r'--' + re.escape(token) + r':\s*([^;]+);', CSS)
            self.assertIsNotNone(m, f'--{token} token is missing')
            value = m.group(1).strip().lower()
            self.assertNotIn(
                value, painted,
                f'--{token} repeats --{painted.get(value)}; two rungs would '
                f'render identically')
            painted[value] = token


class HighlightRenderSiteTests(unittest.TestCase):
    def test_every_list_tab_emits_the_tier_class_and_the_tooltip(self):
        """R12, R15, and KTD10 — nine sites, one rule.

        A site that reads the payload but never emits the class is the whole
        failure: the field is published, the tests on the producer pass, and the
        tab renders exactly as before with nothing to say why.
        """
        for name in RENDER_SITES:
            body = _strip_js_comments(_fn_body(name))
            emits = 'hlClass(' in body or 'hlChipClass(' in body
            self.assertTrue(emits, f'{name} emits no highlight class')
            names = 'hlTitle(' in body or 'HL_TIER_TIP[' in body
            self.assertTrue(names, f'{name} names no rung in a tooltip')

    def test_no_viz_render_path_emits_a_tier(self):
        """KD2. Node colour in those graphs already carries theme strength and
        selection, so a fifth meaning on the same channel would read as noise.

        All four Viz tabs render through `renderNetwork`, so one slice covers
        them.
        """
        start = APP.index('\n  function renderNetwork(')
        end = APP.index('\n  // ── THEMES TAB', start)
        viz = APP[start:end]
        for token in ('hlClass(', 'hlChipClass(', 'hlTitle(', 'HL_TIER_',
                      'hl-short', 'hl-ma-up', 'hl-ma-split'):
            self.assertNotIn(token, viz, f'the Viz render path emits {token}')

    def test_the_themes_chip_still_pushes_the_tight_class_from_its_own_boolean(self):
        """The radar payload carries `tight-range` as well as the tier, and the tight
        strip, the leaf badges and the share sort all read that boolean. The
        chip must keep reading it too, or the tint and the strip could disagree
        about the same chip."""
        self.assertIn("if (t.tight) cls.push('tight-range');", APP)
        themes = _fn_body('renderThemes')
        self.assertIn('t.tight ?', themes)

    def test_the_chip_helper_cannot_add_a_second_tint(self):
        """R1, on the one tab that has two inputs for one rung.

        `renderThemes` pushes `tight-range` from the boolean. If the chip helper also
        mapped the tier's `tight` value, that chip would carry the class twice —
        harmless to paint, but it would mean the two inputs had silently become
        one, and a later edit dropping the boolean would look safe.
        """
        body = re.search(r'function hlChipClass\(tier\)\s*\{([^}]*)\}', APP)
        self.assertIsNotNone(body, 'hlChipClass is gone')
        self.assertNotIn('tight', body.group(1))
        table = re.search(r'const HL_TIER_CLASS = \{([^}]*)\}', APP)
        self.assertIsNotNone(table, 'HL_TIER_CLASS is gone')
        keys = re.findall(r'(\w+):', table.group(1))
        self.assertEqual(['short', 'ma_up', 'ma_split'], keys)
        # One value in, at most one class out — the table is a lookup, not a
        # list, so no payload value can produce two.
        values = re.findall(r":\s*'([^']+)'", table.group(1))
        self.assertEqual(sorted(NEW_TIERS), sorted(values))

    def test_the_span_helper_maps_the_tight_rung(self):
        """The mirror of the test above. The eight table tabs hold no `tight-range`
        boolean — only the tier — so the span helper is the only route the tight
        tint has to those tabs."""
        body = re.search(r'function hlClass\(tier\)\s*\{(.*?)\n  \}', APP, re.S)
        self.assertIsNotNone(body, 'hlClass is gone')
        self.assertIn("'tight'", body.group(1))
        self.assertIn("'tight-range'", body.group(1))

    def test_an_absent_tier_renders_nothing(self):
        """Code pull requests reset `docs/data/`, so the field is missing until
        the next daily workflow run. A missing field must draw no tint rather
        than draw a rung — the same fail-open the V cutoff relies on."""
        for fn in ('hlClass', 'hlChipClass', 'hlTitle'):
            body = re.search(r'function ' + fn + r'\(tier\)\s*\{(.*?)\n  \}',
                             APP, re.S)
            self.assertIsNotNone(body, f'{fn} is gone')
            self.assertIn("''", body.group(1),
                          f'{fn} has no empty-string branch for an absent tier')

    def test_the_helpers_add_no_attribute_the_filter_or_the_clamp_reads(self):
        """`applyTickerFilters` keys off `data-dvol` / `data-adr`, and
        `syncRadarClamps` measures chip `offsetTop`. A tier attribute in that
        space would let a cutoff change move the "+N more" count."""
        for fn in ('hlClass', 'hlChipClass', 'hlTitle'):
            body = re.search(r'function ' + fn + r'\(tier\)\s*\{(.*?)\n  \}',
                             APP, re.S)
            self.assertNotIn('data-', body.group(1))
        attrs = re.search(r'function filterAttrs\(t\)\s*\{(.*?)\n  \}', APP, re.S)
        self.assertIsNotNone(attrs)
        self.assertNotIn('highlight', attrs.group(1))


class HighlightTooltipTests(unittest.TestCase):
    def test_every_rung_is_named_in_words(self):
        """R15. The tints are the only other signal, and green and orange are
        the two closest hues on the board."""
        table = re.search(r'const HL_TIER_TIP = \{(.*?)\n  \};', APP, re.S)
        self.assertIsNotNone(table, 'HL_TIER_TIP is gone')
        keys = re.findall(r'^\s*(\w+):', table.group(1), re.M)
        self.assertEqual(['tight', 'short', 'ma_up', 'ma_split'], keys,
                         'every rung the server can return needs a wording')
        for text in re.findall(r":\s*'([^']*)'", table.group(1)):
            self.assertGreater(len(text), 8, f'{text!r} names no rung')

    def test_the_browser_knows_every_tier_the_ladder_can_return(self):
        """One vocabulary, two languages, and nothing else binds them.

        Rename a rung in Python and the tint and tooltip for it go blank on every
        tab, while each side still reads as internally consistent — the ladder
        returns a string nobody looks up, and the tables hold a key nobody sends.
        The asymmetry below is deliberate: the tooltip table names all four rungs,
        while the class table omits `tight`, because the Themes chip takes that one
        class from the radar's own boolean.
        """
        from src.indicators.create_technical_indicators import HIGHLIGHT_TIERS

        def keys_of(table):
            m = re.search(r'const ' + table + r' = \{(.*?)\n  \};', APP, re.S)
            self.assertIsNotNone(m, f'{table} is gone')
            return re.findall(r'^\s*(\w+):', m.group(1), re.M)

        self.assertEqual(list(HIGHLIGHT_TIERS), keys_of('HL_TIER_TIP'))
        self.assertEqual(
            [t for t in HIGHLIGHT_TIERS if t != 'tight'], keys_of('HL_TIER_CLASS'),
            'the class table must carry every rung except tight')

    def test_the_short_column_colour_break_sits_on_the_ladder_floor(self):
        """Five render sites band the Short% figure at the same 20 the rung uses.

        `CLAUDE.md` says the two agree by coincidence of literal, not by
        construction. So a retuned floor would leave the tint and the number
        beside it disagreeing on the same row — a crowded short shown as crowded
        by one and not the other — with nothing on screen and no test to say so.
        Binding them here is cheaper than the shared constant that would need a
        payload field to reach the browser.
        """
        from src.indicators.create_technical_indicators import (
            HIGHLIGHT_SHORT_FLOOR,
        )

        # The five sites share one band table (COLOR_BANDS.short), whose green
        # band opens at SHORT_CROWDED_PCT. Bind that constant to the floor, the
        # table to the constant, and the five sites to the table.
        const = re.search(r'const SHORT_CROWDED_PCT = (\d+(?:\.\d+)?);', APP)
        self.assertIsNotNone(
            const, 'SHORT_CROWDED_PCT moved or was renamed; re-point this test')
        self.assertEqual(
            float(const.group(1)), HIGHLIGHT_SHORT_FLOOR,
            f'the Short% column bands at {const.group(1)} while the rung gates '
            f'at {HIGHLIGHT_SHORT_FLOOR:g}; the tint and the number would '
            f'disagree on one row')
        table = re.search(r'\n    short: \{\n\s*bands: \[\{([^}]*)\}', APP)
        self.assertIsNotNone(table, 'COLOR_BANDS.short moved; re-point this test')
        self.assertIn("at: SHORT_CROWDED_PCT, out: 'up'", table.group(1))
        sites = re.findall(r'pickBand\(COLOR_BANDS\.short,', APP)
        self.assertGreaterEqual(
            len(sites), 5,
            'a Short% column stopped reading COLOR_BANDS.short; re-point this test')

    def test_the_short_tooltip_quotes_the_floor_the_ladder_actually_gates_on(self):
        """The tooltip hand-types the percentage the Python constant decides.

        Nothing else binds them: the floor reaches no payload the browser reads,
        so a retuned `HIGHLIGHT_SHORT_FLOOR` would leave this tooltip telling a
        trader 20% while the ladder gated somewhere else. The docstring on
        `compute_highlight_tier` openly invites that retune — it records the
        level as chosen against the SI tab's 12% and the EP screener's 10% — so
        the drift this pins is a likely edit, not a hypothetical one.
        """
        from src.indicators.create_technical_indicators import (
            HIGHLIGHT_SHORT_FLOOR,
        )

        table = re.search(r'const HL_TIER_TIP = \{(.*?)\n  \};', APP, re.S)
        self.assertIsNotNone(table, 'HL_TIER_TIP is gone')
        short_tip = re.search(r"short:\s*'([^']*)'", table.group(1))
        self.assertIsNotNone(short_tip, 'the short rung lost its wording')
        self.assertIn(
            f'{HIGHLIGHT_SHORT_FLOOR:g}%', short_tip.group(1),
            f'the tooltip says {short_tip.group(1)!r} but the ladder gates at '
            f'{HIGHLIGHT_SHORT_FLOOR:g}%')

    def test_the_split_stack_tooltip_states_positions_and_asserts_no_direction(self):
        """AE7 and R5. Two opposite trades produce this one reading.

        A stock reclaiming its short-term averages from below and a stock whose
        50-day has just been lost both read EMA10 > EMA20 with EMA20 under
        SMA50. The rung separates neither, so the wording may name only where
        the averages sit.
        """
        table = re.search(r'const HL_TIER_TIP = \{(.*?)\n  \};', APP, re.S)
        m = re.search(r"ma_split:\s*'([^']*)'", table.group(1))
        self.assertIsNotNone(m, 'no ma_split wording')
        text = m.group(1).lower()
        for word in ('turn', 'roll', 'recover', 'weaken', 'break', 'cross',
                     'reclaim', 'lose', 'lost', 'reversal', 'fail'):
            self.assertNotIn(
                word, text,
                f'{word!r} gives the split-stack rung a direction it does not '
                f'have: {m.group(1)!r}')
        for average in ('ema10', 'ema20', 'sma50'):
            self.assertIn(average, text,
                          'the wording must name all three averages it compares')

    def test_the_stacked_rung_reads_differently_from_the_split_rung(self):
        """The two share a channel a colour-blind reader cannot split, so the
        words must carry the whole difference."""
        table = re.search(r'const HL_TIER_TIP = \{(.*?)\n  \};', APP, re.S)
        up = re.search(r"ma_up:\s*'([^']*)'", table.group(1)).group(1)
        split = re.search(r"ma_split:\s*'([^']*)'", table.group(1)).group(1)
        self.assertNotEqual(up, split)


if __name__ == '__main__':
    unittest.main()
