"""Guards for the NAAIM Exposure Index fetch from StockCharts.

The whole point of this module is the survey date, and the trap is that the
feed will happily hand you a wrong one. StockCharts copies each weekly NAAIM
reading onto every trading day and stamps every copy with that day's own date,
so the newest bar says "today" six days after the survey. Reading that
timestamp would rebuild the exact failure that killed the tile the first time:
a frozen number with nothing on screen to say so.

So the survey date is derived, not read -- walk back from the newest row while
the close is unchanged, and the first row of that run is the survey date. Two
consequences these tests pin:

  * On a week whose reading exactly repeats the prior week's, the walk reports
    a date one week old. That is the correct direction to be wrong in, and
    `test_an_exact_repeat_reports_the_earlier_run_start` exists so nobody
    "fixes" it into looking fresher.
  * A window with no change at all yields nothing rather than a reading dated
    to the window's first day -- a series that stopped moving upstream is not
    a survey that happened 60 days ago.

The CSV carries no instrument identity: its header is only Date/Open/High/Low/
Close/Volume. If the symbol ever stops resolving and the endpoint serves some
other well-formed daily series, the parse would succeed and the tile would
publish a different instrument's number under the NAAIM label. The range guard
is what refuses that, the same job the range and sum checks do in the AAII
parse next door.

Fixture dates and values are the live series as measured on 2026-09-01.
"""

import unittest
from unittest.mock import patch

import src.reporting.export_dashboard_data as edd


def _csv(rows):
    """Build a response body in the endpoint's own CSV shape.

    Two lines precede the data: a title line carrying the symbol, then the
    header. Both are padded with the leading whitespace the live endpoint
    sends, because a parse that only works on stripped input would pass here
    and fail in production.
    """
    body = ["!NAAIM, Daily",
            "      Date,       Open,       High,        Low,      Close,        Volume "]
    for date, close in rows:
        body.append(
            f"{date},{close:>11.3f},{close:>11.3f},{close:>11.3f},{close:>11.3f},"
            f"{0:>14} "
        )
    return "\n".join(body) + "\n"


# Three surveys, ascending, exactly as the endpoint orders them. The newest run
# is 102.66 and starts 08-26 -- a Wednesday, though nothing in the code cares.
THREE_WEEKS = _csv([
    ("08/12/2026", 95.52), ("08/13/2026", 95.52), ("08/14/2026", 95.52),
    ("08/17/2026", 95.52), ("08/18/2026", 95.52),
    ("08/19/2026", 94.49), ("08/20/2026", 94.49), ("08/21/2026", 94.49),
    ("08/24/2026", 94.49), ("08/25/2026", 94.49),
    ("08/26/2026", 102.66), ("08/27/2026", 102.66), ("08/28/2026", 102.66),
    ("08/31/2026", 102.66), ("09/01/2026", 102.66),
])


class ParseNaaimSeriesTests(unittest.TestCase):
    """The CSV -> rows half. All-or-nothing: a partial series yields a
    confidently wrong survey date, which is worse than no tile."""

    def test_the_live_shape_parses_ascending(self):
        rows = edd.parse_naaim_series(THREE_WEEKS)
        self.assertEqual(len(rows), 15)
        self.assertEqual(rows[0], ("2026-08-12", 95.52))
        self.assertEqual(rows[-1], ("2026-09-01", 102.66))

    def test_a_renamed_header_yields_nothing(self):
        """The header is the only structural landmark; a redesign that renames
        it must read as a shape change, not as an empty week."""
        moved = THREE_WEEKS.replace("Close", "Last")
        self.assertIsNone(edd.parse_naaim_series(moved))

    def test_a_missing_header_yields_nothing(self):
        body = "\n".join(THREE_WEEKS.splitlines()[2:])
        self.assertIsNone(edd.parse_naaim_series(body))

    def test_a_non_numeric_close_yields_nothing_rather_than_raising(self):
        """parse is called outside the request's try block, and neither
        update_breadth_history nor export_all guards the chain -- an escaping
        ValueError would abort the whole daily export, not just this tile.

        The corruption has to land in the Close column specifically: the parse
        reads only the date and the close, so a junk Open is correctly ignored
        and would not exercise this path.
        """
        lines = THREE_WEEKS.splitlines()
        cols = lines[-1].split(',')
        cols[4] = "      n/a  "
        lines[-1] = ",".join(cols)
        self.assertIsNone(edd.parse_naaim_series("\n".join(lines)))

    def test_a_non_numeric_date_yields_nothing_rather_than_raising(self):
        lines = THREE_WEEKS.splitlines()
        cols = lines[-1].split(',')
        cols[0] = "not-a-date"
        lines[-1] = ",".join(cols)
        self.assertIsNone(edd.parse_naaim_series("\n".join(lines)))

    def test_a_truncated_row_yields_nothing(self):
        """A response cut mid-stream must not read as a shorter series."""
        lines = THREE_WEEKS.splitlines()
        lines[-1] = "09/01/2026,    102.660"
        self.assertIsNone(edd.parse_naaim_series("\n".join(lines)))

    def test_an_empty_body_yields_nothing(self):
        self.assertIsNone(edd.parse_naaim_series(""))

    def test_a_body_with_only_headers_yields_nothing(self):
        self.assertIsNone(edd.parse_naaim_series(_csv([])))


class DeriveNaaimReadingTests(unittest.TestCase):
    """The rows -> {value, as_of} half, where the survey date is recovered."""

    def test_the_newest_run_start_is_the_survey_date(self):
        reading = edd.derive_naaim_reading(edd.parse_naaim_series(THREE_WEEKS))
        self.assertEqual(reading, {"value": 102.66, "as_of": "2026-08-26"})

    def test_a_one_row_run_reports_its_own_date(self):
        """Survey day itself: the run is one bar old and that bar is the date."""
        rows = edd.parse_naaim_series(_csv([
            ("08/19/2026", 94.49), ("08/20/2026", 94.49),
            ("08/26/2026", 102.66),
        ]))
        self.assertEqual(
            edd.derive_naaim_reading(rows),
            {"value": 102.66, "as_of": "2026-08-26"},
        )

    def test_an_exact_repeat_reports_the_earlier_run_start(self):
        """A week that repeats the prior week's reading exactly is
        indistinguishable from a week that did not publish, so the walk spans
        both and the date reads one week old.

        Do not "fix" this into looking fresher. It errs stale, never fresh --
        the safe direction for a staleness signal -- and it is rare: 6 exact
        week-over-week repeats in 1,053 weeks of history.
        """
        rows = edd.parse_naaim_series(_csv([
            ("08/12/2026", 95.52), ("08/13/2026", 95.52),
            ("08/19/2026", 94.49), ("08/20/2026", 94.49),
            ("08/26/2026", 94.49), ("08/27/2026", 94.49),
        ]))
        self.assertEqual(
            edd.derive_naaim_reading(rows),
            {"value": 94.49, "as_of": "2026-08-19"},
        )

    def test_a_change_free_window_yields_nothing(self):
        """60 days is roughly eight surveys, so a window with no step at all
        means the series stopped upstream. Publishing it dated to the window's
        first day would invent a survey that never happened."""
        rows = edd.parse_naaim_series(_csv([
            ("08/12/2026", 95.52), ("08/19/2026", 95.52), ("08/26/2026", 95.52),
        ]))
        self.assertIsNone(edd.derive_naaim_reading(rows))

    def test_a_single_row_yields_nothing(self):
        rows = edd.parse_naaim_series(_csv([("09/01/2026", 102.66)]))
        self.assertIsNone(edd.derive_naaim_reading(rows))

    def test_a_reading_outside_the_exposure_range_yields_nothing(self):
        """The wrong-series guard. The CSV names no instrument, so if the
        symbol stops resolving and the endpoint serves another well-formed
        daily series, everything above still succeeds -- this is the only leg
        that refuses it. NAAIM runs about -200..200; an equity index does not.
        """
        rows = edd.parse_naaim_series(_csv([
            ("08/19/2026", 5820.10), ("08/20/2026", 5820.10),
            ("08/26/2026", 5904.33),
        ]))
        self.assertIsNone(edd.derive_naaim_reading(rows))

    def test_a_negative_reading_is_accepted(self):
        """Net-short member positioning is a real reading, not a parse error."""
        rows = edd.parse_naaim_series(_csv([
            ("08/19/2026", 12.40), ("08/20/2026", 12.40),
            ("08/26/2026", -37.50),
        ]))
        self.assertEqual(
            edd.derive_naaim_reading(rows),
            {"value": -37.5, "as_of": "2026-08-26"},
        )

    def test_no_rows_yields_nothing(self):
        self.assertIsNone(edd.derive_naaim_reading(None))
        self.assertIsNone(edd.derive_naaim_reading([]))


class FetchNaaimExposureTests(unittest.TestCase):
    """The request half: never raises, and separates its failure modes."""

    def _fetch(self, body=None, exc=None):
        class _Resp:
            def __enter__(self_inner):
                return self_inner

            def __exit__(self_inner, *a):
                return False

            def read(self_inner):
                return body.encode("utf-8")

        def _open(req, timeout=None):
            if exc is not None:
                raise exc
            _open.sent_agent = req.get_header("User-agent")
            return _Resp()

        _open.sent_agent = None
        with patch.object(edd.urllib.request, "urlopen", _open):
            return edd.fetch_naaim_exposure(), _open

    def test_a_good_response_returns_the_derived_reading(self):
        reading, _ = self._fetch(body=THREE_WEEKS)
        self.assertEqual(reading, {"value": 102.66, "as_of": "2026-08-26"})

    def test_a_transport_failure_returns_none(self):
        reading, _ = self._fetch(exc=OSError("connection reset"))
        self.assertIsNone(reading)

    def test_a_200_that_parses_to_nothing_returns_none(self):
        """Distinct from the transport path on purpose. Conflating 'did not
        arrive' with 'arrived and made no sense' is what let the old NAAIM
        breakage read as an ordinary quiet week for weeks."""
        reading, _ = self._fetch(body="<html>Symbol not found</html>")
        self.assertIsNone(reading)

    def test_the_configured_user_agent_is_sent(self):
        """A wrong User-Agent gets 404 here, not 403 -- so a regression looks
        exactly like a delisted symbol and would be chased in the wrong place.
        """
        _, opener = self._fetch(body=THREE_WEEKS)
        self.assertEqual(
            opener.sent_agent,
            edd.CONFIG["market_breadth"]["user_agent"],
        )

    def test_the_request_targets_the_configured_endpoint(self):
        """Pins that the URL comes from config rather than a literal, and that
        the symbol survives templating."""
        captured = {}

        class _Resp:
            def __enter__(self_inner):
                return self_inner

            def __exit__(self_inner, *a):
                return False

            def read(self_inner):
                return THREE_WEEKS.encode("utf-8")

        def _open(req, timeout=None):
            captured["url"] = req.full_url
            return _Resp()

        with patch.object(edd.urllib.request, "urlopen", _open):
            edd.fetch_naaim_exposure()

        self.assertIn("%21NAAIM", captured["url"])
        self.assertIn("out=csv", captured["url"])
        self.assertNotIn("{start}", captured["url"])
        self.assertNotIn("{cachebust}", captured["url"])


if __name__ == "__main__":
    unittest.main()
