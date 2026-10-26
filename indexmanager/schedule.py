"""When a rule runs: weekly (chosen days), daily, or only by hand.

A schedule is a dict {"type": "weekly" | "daily" | "manual", "days": [0..6, Monday = 0], "time": "HH:MM:SS",
"tz": "Europe/Bucharest"}. The time is wall-clock in the IANA zone "tz"; without "tz" (older rules) it is server
local time. Moments are returned as aware datetimes.
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta, tzinfo
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

DAY_NAMES = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")


def parse_time(value: str) -> time:
    try:
        parsed = datetime.strptime(value, "%H:%M:%S" if value.count(":") == 2 else "%H:%M")
    except (AttributeError, ValueError):
        raise ValueError("The time must look like 00:00:00 (hours:minutes:seconds).") from None
    return parsed.time()


def parse_zone(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError, OSError):
        raise ValueError(f"Unknown time zone '{name}'.") from None


def validate(schedule: dict) -> dict:
    """The schedule in stored form. Raises ValueError with a UI message if it is invalid."""
    kind = schedule.get("type")
    if kind == "manual":
        return {"type": "manual"}
    if kind not in ("weekly", "daily"):
        raise ValueError("The schedule must be weekly, daily or manual.")
    at = parse_time(schedule.get("time"))
    result = {"type": kind, "time": at.strftime("%H:%M:%S")}
    if schedule.get("tz"):
        parse_zone(schedule["tz"])
        result["tz"] = schedule["tz"]
    if kind == "weekly":
        days = sorted(set(schedule.get("days") or []))
        if not days:
            raise ValueError("Choose at least one day of the week.")
        if any(not isinstance(d, int) or not 0 <= d <= 6 for d in days):
            raise ValueError("Days must be numbers from 0 (Monday) to 6 (Sunday).")
        result["days"] = days
    return result


def describe(schedule: dict) -> str:
    if schedule["type"] == "manual":
        return "Manual only"
    if schedule["type"] == "daily" or len(schedule["days"]) == 7:
        text = f"Every day at {schedule['time']}"
    else:
        text = "Every " + ", ".join(DAY_NAMES[d][:3] for d in schedule["days"]) + f" at {schedule['time']}"
    return text + (f" ({schedule['tz']})" if schedule.get("tz") else "")


def _zone(schedule: dict) -> tzinfo | None:
    return parse_zone(schedule["tz"]) if schedule.get("tz") else None


def _wall_date(moment: datetime, zone: tzinfo | None) -> date:
    return moment.astimezone(zone).date()    # naive datetimes are server local time


def _moment(day: date, at: time, zone: tzinfo | None) -> datetime:
    """The aware moment of wall time `at` on `day`; in a repeated hour, the first one."""
    wall = datetime.combine(day, at)
    return wall.replace(tzinfo=zone) if zone else wall.astimezone()


def _occurs_on(schedule: dict, day: date) -> bool:
    return schedule["type"] == "daily" or day.weekday() in schedule["days"]


def latest_occurrence(schedule: dict, after: datetime, upto: datetime) -> datetime | None:
    """The most recent scheduled moment in (after, upto], or None."""
    if schedule["type"] == "manual":
        return None
    at, zone = parse_time(schedule["time"]), _zone(schedule)
    first, last = _wall_date(after, zone), _wall_date(upto, zone)
    for back in range(0, min((last - first).days, 14) + 1):
        day = last - timedelta(days=back)
        if not _occurs_on(schedule, day):
            continue
        moment = _moment(day, at, zone)
        if after < moment <= upto:
            return moment
    return None


def next_occurrence(schedule: dict, now: datetime) -> datetime | None:
    """The first scheduled moment after `now`, or None for a manual-only schedule."""
    if schedule["type"] == "manual":
        return None
    at, zone = parse_time(schedule["time"]), _zone(schedule)
    today = _wall_date(now, zone)
    for ahead in range(0, 9):
        day = today + timedelta(days=ahead)
        if _occurs_on(schedule, day):
            moment = _moment(day, at, zone)
            if moment > now:
                return moment
    return None
