"""Decide whether a scheduled workflow should continue.

GitHub Actions schedules are expressed in UTC, so each workflow keeps two cron
entries — one for PDT and one for PST.  This guard validates the cron
expression that actually triggered the run against the current Pacific UTC
offset.  That keeps the workflow correct even if GitHub starts the job late.

The expected cron pair is read from environment variables so that every
workflow can re-use the same guard:

    SCHEDULE_GUARD_PDT_CRON   (default: "15 20 * * 1-5" → 1:15 PM PDT)
    SCHEDULE_GUARD_PST_CRON   (default: "15 21 * * 1-5" → 1:15 PM PST)
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Tuple
from zoneinfo import ZoneInfo


PACIFIC_TZ = ZoneInfo("America/Los_Angeles")
PDT_CRON = os.environ.get("SCHEDULE_GUARD_PDT_CRON", "15 20 * * 1-5")
PST_CRON = os.environ.get("SCHEDULE_GUARD_PST_CRON", "15 21 * * 1-5")


def parse_utc_datetime(raw_value: str | None) -> datetime:
    """Parse an ISO 8601 timestamp into UTC."""
    if not raw_value:
        return datetime.now(timezone.utc)

    normalized = raw_value.strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        raise ValueError("Timestamp must include a timezone offset")
    return parsed.astimezone(timezone.utc)


def expected_schedule_for(now_utc: datetime) -> str:
    """Return the UTC cron that should be active for the Pacific offset."""
    pacific_now = now_utc.astimezone(PACIFIC_TZ)
    offset = pacific_now.utcoffset()
    if offset is None:
        raise ValueError("Pacific timezone offset could not be determined")

    offset_hours = int(offset.total_seconds() // 3600)
    if offset_hours == -7:
        return PDT_CRON
    if offset_hours == -8:
        return PST_CRON

    raise ValueError(f"Unexpected Pacific UTC offset: {offset}")


def evaluate_schedule_guard(
    event_name: str,
    event_schedule: str | None,
    now_utc: datetime | None = None,
) -> Tuple[bool, str]:
    """Determine whether the workflow should run."""
    current_utc = now_utc or datetime.now(timezone.utc)
    pacific_now = current_utc.astimezone(PACIFIC_TZ)

    if event_name == "workflow_dispatch":
        return True, "Manual trigger - always run"

    if event_name != "schedule":
        return True, f"Non-scheduled event '{event_name}' - allowing run"

    if not event_schedule:
        return False, "Scheduled trigger is missing github.event.schedule"

    expected_schedule = expected_schedule_for(current_utc)
    pacific_stamp = pacific_now.strftime("%Y-%m-%d %H:%M:%S %Z")

    if event_schedule == expected_schedule:
        return (
            True,
            "Scheduled trigger matches Pacific DST mapping "
            f"(pacific_now={pacific_stamp}, expected_cron='{expected_schedule}')",
        )

    return (
        False,
        "Scheduled trigger does not match Pacific DST mapping "
        f"(pacific_now={pacific_stamp}, expected_cron='{expected_schedule}', "
        f"received_cron='{event_schedule}')",
    )


def append_github_output(name: str, value: str) -> None:
    """Write a key/value output for GitHub Actions if available."""
    output_path = os.environ.get("GITHUB_OUTPUT")
    if not output_path:
        return

    with Path(output_path).open("a", encoding="utf-8") as handle:
        handle.write(f"{name}={value}\n")


def build_parser() -> argparse.ArgumentParser:
    """Create a CLI parser for local verification and workflow usage."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--event-name",
        default=os.environ.get("WORKFLOW_EVENT_NAME") or os.environ.get("GITHUB_EVENT_NAME"),
        help="GitHub event name, for example 'schedule' or 'workflow_dispatch'",
    )
    parser.add_argument(
        "--event-schedule",
        default=os.environ.get("WORKFLOW_EVENT_SCHEDULE"),
        help="Cron string from github.event.schedule",
    )
    parser.add_argument(
        "--now-utc",
        default=None,
        help="Optional ISO 8601 timestamp used for local verification",
    )
    return parser


def main() -> int:
    """CLI entry point."""
    parser = build_parser()
    args = parser.parse_args()

    if not args.event_name:
        parser.error("--event-name is required")

    now_utc = parse_utc_datetime(args.now_utc)
    should_run, reason = evaluate_schedule_guard(
        event_name=args.event_name,
        event_schedule=args.event_schedule,
        now_utc=now_utc,
    )

    print(reason)
    append_github_output("should_run", "true" if should_run else "false")
    append_github_output("reason", reason.replace("\n", " "))
    return 0


if __name__ == "__main__":
    sys.exit(main())
