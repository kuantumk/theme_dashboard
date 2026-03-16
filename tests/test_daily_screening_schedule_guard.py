import unittest
from datetime import datetime, timezone

from src.ci.daily_screening_schedule_guard import (
    PDT_CRON,
    PST_CRON,
    evaluate_schedule_guard,
    expected_schedule_for,
)


class DailyScreeningScheduleGuardTests(unittest.TestCase):
    def test_manual_trigger_always_runs(self) -> None:
        should_run, reason = evaluate_schedule_guard(
            event_name="workflow_dispatch",
            event_schedule=None,
            now_utc=datetime(2026, 3, 16, 20, 45, tzinfo=timezone.utc),
        )

        self.assertTrue(should_run)
        self.assertIn("Manual trigger", reason)

    def test_pdt_expected_cron_is_used(self) -> None:
        now_utc = datetime(2026, 3, 16, 20, 45, tzinfo=timezone.utc)

        self.assertEqual(expected_schedule_for(now_utc), PDT_CRON)

        should_run, _ = evaluate_schedule_guard(
            event_name="schedule",
            event_schedule=PDT_CRON,
            now_utc=now_utc,
        )

        self.assertTrue(should_run)

    def test_pst_expected_cron_is_used(self) -> None:
        now_utc = datetime(2026, 1, 12, 21, 45, tzinfo=timezone.utc)

        self.assertEqual(expected_schedule_for(now_utc), PST_CRON)

        should_run, _ = evaluate_schedule_guard(
            event_name="schedule",
            event_schedule=PST_CRON,
            now_utc=now_utc,
        )

        self.assertTrue(should_run)

    def test_delayed_pdt_run_still_executes_when_correct_cron_fired(self) -> None:
        should_run, _ = evaluate_schedule_guard(
            event_name="schedule",
            event_schedule=PDT_CRON,
            now_utc=datetime(2026, 3, 16, 21, 20, tzinfo=timezone.utc),
        )

        self.assertTrue(should_run)

    def test_wrong_cron_is_skipped_for_current_pacific_offset(self) -> None:
        should_run, reason = evaluate_schedule_guard(
            event_name="schedule",
            event_schedule=PST_CRON,
            now_utc=datetime(2026, 3, 16, 20, 45, tzinfo=timezone.utc),
        )

        self.assertFalse(should_run)
        self.assertIn("does not match", reason)

    def test_missing_schedule_is_rejected_for_scheduled_runs(self) -> None:
        should_run, reason = evaluate_schedule_guard(
            event_name="schedule",
            event_schedule=None,
            now_utc=datetime(2026, 3, 16, 20, 45, tzinfo=timezone.utc),
        )

        self.assertFalse(should_run)
        self.assertIn("missing", reason)


if __name__ == "__main__":
    unittest.main()
