"""Trade-classification tests for the bid/ask tape dashboard.

The first test in this file is the most important one in the feature. Getting
the directional mapping backwards inverts the entire product while every other
test still passes and the UI still looks plausible, so it is asserted directly
against hardcoded values rather than derived from any shared helper.
"""

import unittest

from src.bidask.classify import (
    REJECT_AUCTION,
    REJECT_CROSSED,
    REJECT_NO_QUOTE,
    REJECT_NO_TRADE,
    REJECT_WIDE_SPREAD,
    REJECT_WARMUP,
    Tick,
    classify,
)
from src.bidask.config import load_config

CFG = load_config()


def tick(last, bid, ask, volume):
    return Tick(last=last, bid=bid, ask=ask, volume=volume)


class TestDirectionalMapping(unittest.TestCase):
    """The one mapping that must never invert."""

    def test_print_at_ask_is_buying_pressure(self):
        # A trade printing at the offer means a buyer crossed the spread to get
        # filled: the offer was lifted. That is BUYING pressure, sign +1, green.
        obs = classify(
            cur=tick(last=10.10, bid=10.08, ask=10.10, volume=2000),
            prev=tick(last=10.09, bid=10.08, ask=10.10, volume=1000),
            prior_different_price=10.09,
            cfg=CFG,
        )
        self.assertEqual(obs.sign, +1)
        self.assertTrue(obs.is_buy)
        self.assertFalse(obs.is_sell)

    def test_print_at_bid_is_selling_pressure(self):
        # A trade printing at the bid means a seller crossed the spread: the bid
        # was hit. That is SELLING pressure, sign -1, red.
        obs = classify(
            cur=tick(last=10.08, bid=10.08, ask=10.10, volume=2000),
            prev=tick(last=10.09, bid=10.08, ask=10.10, volume=1000),
            prior_different_price=10.09,
            cfg=CFG,
        )
        self.assertEqual(obs.sign, -1)
        self.assertTrue(obs.is_sell)
        self.assertFalse(obs.is_buy)


class TestToleranceBand(unittest.TestCase):
    def test_sub_penny_below_ask_still_counts_as_buy(self):
        # Retail wholesaler fills print at sub-penny improvement off the quote,
        # so an exact `last == ask` test would essentially never fire. Inside the
        # 30%-of-spread band it must still classify as buyer-initiated.
        obs = classify(
            cur=tick(last=10.0995, bid=10.08, ask=10.10, volume=2000),
            prev=tick(last=10.09, bid=10.08, ask=10.10, volume=1000),
            prior_different_price=10.09,
            cfg=CFG,
        )
        self.assertEqual(obs.sign, +1)

    def test_sub_penny_above_bid_still_counts_as_sell(self):
        obs = classify(
            cur=tick(last=10.0805, bid=10.08, ask=10.10, volume=2000),
            prev=tick(last=10.09, bid=10.08, ask=10.10, volume=1000),
            prior_different_price=10.09,
            cfg=CFG,
        )
        self.assertEqual(obs.sign, -1)


class TestMidpointTickRule(unittest.TestCase):
    def test_midpoint_with_rising_prior_price_is_buy(self):
        # Middle 40% of the spread falls to the tick rule.
        obs = classify(
            cur=tick(last=10.09, bid=10.08, ask=10.10, volume=2000),
            prev=tick(last=10.08, bid=10.08, ask=10.10, volume=1000),
            prior_different_price=10.05,
            cfg=CFG,
        )
        self.assertEqual(obs.sign, +1)

    def test_midpoint_with_falling_prior_price_is_sell(self):
        obs = classify(
            cur=tick(last=10.09, bid=10.08, ask=10.10, volume=2000),
            prev=tick(last=10.10, bid=10.08, ask=10.10, volume=1000),
            prior_different_price=10.15,
            cfg=CFG,
        )
        self.assertEqual(obs.sign, -1)

    def test_midpoint_with_no_prior_different_price_is_unclassified(self):
        obs = classify(
            cur=tick(last=10.09, bid=10.08, ask=10.10, volume=2000),
            prev=tick(last=10.09, bid=10.08, ask=10.10, volume=1000),
            prior_different_price=None,
            cfg=CFG,
        )
        self.assertEqual(obs.sign, 0)
        self.assertFalse(obs.classified)


class TestPreconditions(unittest.TestCase):
    def test_unchanged_volume_is_rejected_even_at_the_ask(self):
        # A republished quote is not a trade. Without this gate, a thin name
        # sitting at the offer would accrue a hit on every poll forever.
        obs = classify(
            cur=tick(last=10.10, bid=10.08, ask=10.10, volume=1000),
            prev=tick(last=10.10, bid=10.08, ask=10.10, volume=1000),
            prior_different_price=10.09,
            cfg=CFG,
        )
        self.assertEqual(obs.reason, REJECT_NO_TRADE)
        self.assertFalse(obs.classified)

    def test_no_prior_poll_is_warmup(self):
        obs = classify(
            cur=tick(last=10.10, bid=10.08, ask=10.10, volume=1000),
            prev=None,
            prior_different_price=None,
            cfg=CFG,
        )
        self.assertEqual(obs.reason, REJECT_WARMUP)

    def test_crossed_market_is_rejected(self):
        obs = classify(
            cur=tick(last=10.09, bid=10.11, ask=10.10, volume=2000),
            prev=tick(last=10.09, bid=10.08, ask=10.10, volume=1000),
            prior_different_price=10.08,
            cfg=CFG,
        )
        self.assertEqual(obs.reason, REJECT_CROSSED)

    def test_locked_market_is_rejected(self):
        obs = classify(
            cur=tick(last=10.10, bid=10.10, ask=10.10, volume=2000),
            prev=tick(last=10.09, bid=10.08, ask=10.10, volume=1000),
            prior_different_price=10.09,
            cfg=CFG,
        )
        self.assertEqual(obs.reason, REJECT_CROSSED)

    def test_missing_quote_is_rejected(self):
        obs = classify(
            cur=tick(last=10.10, bid=0.0, ask=0.0, volume=2000),
            prev=tick(last=10.09, bid=10.08, ask=10.10, volume=1000),
            prior_different_price=10.09,
            cfg=CFG,
        )
        self.assertEqual(obs.reason, REJECT_NO_QUOTE)

    def test_absurd_spread_is_rejected(self):
        # 10.00 x 12.00 is a 18% spread against a 2% cap: a stale or broken quote.
        obs = classify(
            cur=tick(last=11.00, bid=10.00, ask=12.00, volume=2000),
            prev=tick(last=10.90, bid=10.00, ask=12.00, volume=1000),
            prior_different_price=10.90,
            cfg=CFG,
        )
        self.assertEqual(obs.reason, REJECT_WIDE_SPREAD)

    def test_auction_window_is_rejected_for_equities(self):
        obs = classify(
            cur=tick(last=10.10, bid=10.08, ask=10.10, volume=2000),
            prev=tick(last=10.09, bid=10.08, ask=10.10, volume=1000),
            prior_different_price=10.09,
            cfg=CFG,
            in_auction_window=True,
        )
        self.assertEqual(obs.reason, REJECT_AUCTION)

    def test_crypto_is_exempt_from_auction_window(self):
        # Crypto trades continuously; there is no auction to exclude, so the
        # caller passes in_auction_window=False and the observation stands.
        obs = classify(
            cur=tick(last=10.10, bid=10.08, ask=10.10, volume=2000),
            prev=tick(last=10.09, bid=10.08, ask=10.10, volume=1000),
            prior_different_price=10.09,
            cfg=CFG,
            in_auction_window=False,
        )
        self.assertEqual(obs.sign, +1)


class TestPreTradeBook(unittest.TestCase):
    """The quote rule reads the book that prevailed BEFORE the interval's trades.

    Measured live on 2026-08-20 over 13,821 observations: scored against the
    book snapshotted *after* the trade, the quote rule's sign runs backwards --
    Spearman against the same-window mid return is -0.185, and against an
    independent 1-minute price series it is -0.009. Scored against the previous
    poll's book it is +0.339. The average print sits at 0.353 of the spread when
    the mid rose and 0.633 when it fell, because the book has already reacted to
    the trade by the time we look. This is the misalignment Lee & Ready's lag
    rule exists to prevent.

    The tick rule masks it whenever it disagrees, so both cases below hold the
    tick rule silent. That is the state a run of same-price prints produces, and
    it is where the wrong book decides alone.
    """

    def test_sliding_book_reads_a_bid_hit_not_a_lift(self):
        # A seller hit the bid at 10.10. The book then slid to 10.08 x 10.10, so
        # 10.10 is now the OFFER and the post-trade book reads it as a lift.
        obs = classify(
            cur=tick(last=10.10, bid=10.08, ask=10.10, volume=2000),
            prev=tick(last=10.10, bid=10.10, ask=10.12, volume=1000),
            prior_different_price=10.10,
            cfg=CFG,
        )
        self.assertEqual(obs.sign, -1)

    def test_rising_book_reads_a_lift_not_a_bid_hit(self):
        # The mirror. A buyer lifted the offer at 10.10 and the book ratcheted to
        # 10.10 x 10.12, so 10.10 is now the BID.
        obs = classify(
            cur=tick(last=10.10, bid=10.10, ask=10.12, volume=2000),
            prev=tick(last=10.10, bid=10.08, ask=10.10, volume=1000),
            prior_different_price=10.10,
            cfg=CFG,
        )
        self.assertEqual(obs.sign, +1)

    def test_unusable_previous_book_leaves_the_tick_rule_to_decide(self):
        # The previous poll carried no quote, so there is no pre-trade book to
        # classify against. Abstain rather than fall back to the post-trade one.
        obs = classify(
            cur=tick(last=10.10, bid=10.08, ask=10.10, volume=2000),
            prev=tick(last=10.05, bid=0.0, ask=0.0, volume=1000),
            prior_different_price=10.05,
            cfg=CFG,
        )
        self.assertEqual(obs.sign, +1)   # from the tick rule, 10.10 > 10.05

    def test_a_stale_wide_previous_book_does_not_classify(self):
        """A `wide_spread` poll still becomes the next poll's `prev`.

        The accumulator stores every snapshot, rejected or not. A 20% band off a
        stale quote swallows any print, so the same staleness cap applies to the
        book the quote rule reads, not only to the current snapshot.
        """
        obs = classify(
            cur=tick(last=10.10, bid=10.08, ask=10.10, volume=2000),
            prev=tick(last=10.10, bid=10.00, ask=12.00, volume=1000),
            prior_different_price=10.10,
            cfg=CFG,
        )
        self.assertFalse(obs.classified)

    def test_unusable_previous_book_with_silent_tick_is_unclassified(self):
        obs = classify(
            cur=tick(last=10.10, bid=10.08, ask=10.10, volume=2000),
            prev=tick(last=10.10, bid=0.0, ask=0.0, volume=1000),
            prior_different_price=10.10,
            cfg=CFG,
        )
        self.assertFalse(obs.classified)


class TestQuoteDriftOverride(unittest.TestCase):
    def test_drifted_quote_with_rising_tick_agrees_once_the_book_is_lagged(self):
        # A buyer lifted the offer at 10.10; by this poll the book has ratcheted
        # to 10.10 x 10.12, leaving `last` on the new BID. Against the pre-trade
        # book the quote rule already calls this a lift, so there is nothing for
        # the tick rule to override and the observation is certain.
        obs = classify(
            cur=tick(last=10.10, bid=10.10, ask=10.12, volume=2000),
            prev=tick(last=10.09, bid=10.08, ask=10.10, volume=1000),
            prior_different_price=10.09,
            cfg=CFG,
        )
        self.assertEqual(obs.sign, +1)
        self.assertTrue(obs.certain)

    def test_frozen_book_still_yields_to_a_disagreeing_tick(self):
        # The override must NOT be gated on the book having moved. A frozen book
        # means the mid did not move, so the poll carries no directional
        # information -- but the quote rule still emits a sign, and that sign is
        # this ticker's habitual position inside its own spread. Measured live on
        # 2026-08-20: handing that cohort to the quote rule is 3.4% of
        # observations and it lifts the board's correlation with a static
        # position-in-spread from +0.045 to +0.217, while adding nothing to its
        # correlation with price. Those counts are pure accumulated bias.
        obs = classify(
            cur=tick(last=10.08, bid=10.08, ask=10.10, volume=2000),
            prev=tick(last=10.05, bid=10.08, ask=10.10, volume=1000),
            prior_different_price=10.05,
            cfg=CFG,
        )
        self.assertEqual(obs.sign, +1)
        self.assertFalse(obs.certain)

    def test_drifted_quote_with_agreeing_tick_stays_certain(self):
        obs = classify(
            cur=tick(last=10.12, bid=10.10, ask=10.12, volume=2000),
            prev=tick(last=10.09, bid=10.08, ask=10.10, volume=1000),
            prior_different_price=10.09,
            cfg=CFG,
        )
        self.assertEqual(obs.sign, +1)
        self.assertTrue(obs.certain)


class TestNonFiniteGuard(unittest.TestCase):
    """pandas yields NaN for null cells, and every NaN comparison is False.

    Without an explicit guard, `nan <= 0` does not fire and a row with no quote
    falls through every precondition to the tick rule, returning certain=True.
    """

    def test_nan_quote_is_rejected_not_classified(self):
        nan = float("nan")
        obs = classify(
            cur=tick(last=10.10, bid=nan, ask=nan, volume=2000),
            prev=tick(last=10.05, bid=nan, ask=nan, volume=1000),
            prior_different_price=10.05,
            cfg=CFG,
        )
        self.assertEqual(obs.reason, REJECT_NO_QUOTE)
        self.assertFalse(obs.classified)
        self.assertFalse(obs.certain)

    def test_nan_last_price_is_rejected(self):
        obs = classify(
            cur=tick(last=float("nan"), bid=10.08, ask=10.10, volume=2000),
            prev=tick(last=10.05, bid=10.08, ask=10.10, volume=1000),
            prior_different_price=10.05,
            cfg=CFG,
        )
        self.assertEqual(obs.reason, REJECT_NO_QUOTE)

    def test_nan_volume_does_not_score_a_hit(self):
        obs = classify(
            cur=tick(last=10.10, bid=10.08, ask=10.10, volume=float("nan")),
            prev=tick(last=10.05, bid=10.08, ask=10.10, volume=1000),
            prior_different_price=10.05,
            cfg=CFG,
        )
        self.assertFalse(obs.classified)

    def test_infinite_value_is_rejected(self):
        obs = classify(
            cur=tick(last=float("inf"), bid=10.08, ask=10.10, volume=2000),
            prev=tick(last=10.05, bid=10.08, ask=10.10, volume=1000),
            prior_different_price=10.05,
            cfg=CFG,
        )
        self.assertEqual(obs.reason, REJECT_NO_QUOTE)


class TestVolumeDelta(unittest.TestCase):
    def test_volume_delta_is_reported(self):
        obs = classify(
            cur=tick(last=10.10, bid=10.08, ask=10.10, volume=2500),
            prev=tick(last=10.09, bid=10.08, ask=10.10, volume=1000),
            prior_different_price=10.09,
            cfg=CFG,
        )
        self.assertEqual(obs.volume_delta, 1500)


if __name__ == "__main__":
    unittest.main()
