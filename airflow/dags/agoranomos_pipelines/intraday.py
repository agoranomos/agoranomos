"""intraday: the period of the short cycle of the current day, which the DAGs of the short cycle
read when the DAG processor parses them and when their tasks run (ADR-0051), and the hours of its
runs, which keep away from the daily backup (ADR-0020).

A load of the kind intraday takes the current day of each account in its time zone, and the day
before while no whole load covers it, at the level of campaigns, into the raw tables of its source,
and the one transform takes it like any load. AGORANOMOS_INTRADAY_HOURS of .env gives the hours
between two such loads: a divisor of 24, 1, 2, 3, 4, 6, 8, 12 or 24, so that the loads fall at the
same hours every day, at MINUTE past the hour in UTC, or 0 for no short cycle; empty takes 3, as
compose.yaml does. Any other value stops the parse of the DAG with a line that says what is
allowed, as ./agoranomos stops before up. A run that waited longer than the period, behind a load
of its source in the pool of the loads or for a slot of the executor, ends without a load, as the
next one comes (waited_too_long).

The runs start from midnight in UTC and keep at least BACKUP_DISTANCE from the start of the daily
backup, BACKUP_TIME of .env in BACKUP_TZ, at each offset from UTC that the zone has in the year, as
ADR-0020 asks of a DAG of the base package on a time schedule: when a run would fall closer, every
run moves later by the fewest whole hours that keep them all away, and when no move does, as with a
period of 1 or 2 hours, the move that keeps the most runs is taken and the runs that still fall
closer are left out (run_hours). The hours stay the same every day, also when the zone of the
backup changes to summer time. An empty BACKUP_TIME or BACKUP_TZ takes 03:00 or UTC, as compose.yaml
does, and a value that is not a time of day or a time zone stops the parse of the DAG, as
./agoranomos stops before up in every mode, with or without the ops profile.
"""

from __future__ import annotations

import os
import re
from collections.abc import Iterable
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

SETTING = "AGORANOMOS_INTRADAY_HOURS"
DEFAULT_HOURS = 3
# The periods in hours: the divisors of 24, and 0 for none.
PERIODS = (0, 1, 2, 3, 4, 6, 8, 12, 24)
# The minute of each load in UTC: away from the daily loads of Meta and of Google Ads, at 10:07 and
# 10:37 UTC, and from the watcher, at 4, 19, 34 and 49 past each hour.
MINUTE = 27
# The settings of the daily backup (ADR-0020), which compose.yaml gives every Airflow service as it
# gives them the backup service, with their defaults there.
BACKUP_TIME_SETTING = "BACKUP_TIME"
BACKUP_TZ_SETTING = "BACKUP_TZ"
DEFAULT_BACKUP_TIME = "03:00"
DEFAULT_BACKUP_TZ = "UTC"
# The least time between a run and the start of the daily backup, before or after it (ADR-0020): a
# run ends within an hour, its dagrun_timeout.
BACKUP_DISTANCE = timedelta(hours=1)
# A time of day as HH:MM, from 00:00 to 23:59, as ./agoranomos and postgres/backup.sh check it.
TIME_OF_DAY = re.compile(r"(?:[01][0-9]|2[0-3]):[0-5][0-9]")
DAY = 24 * 60


def period(value: str | None = None) -> int:
    """The hours between two loads of the short cycle, from value or, without one, from the
    setting in the environment: a divisor of 24, or 0 for none; empty takes DEFAULT_HOURS. Another
    value raises ValueError with a line that names the setting."""
    text = (os.environ.get(SETTING, "") if value is None else value).strip()
    if not text:
        return DEFAULT_HOURS
    allowed = {str(hours): hours for hours in PERIODS}
    if text not in allowed:
        listed = ", ".join(str(hours) for hours in PERIODS[1:-1])
        raise ValueError(
            f"{SETTING} in .env must be {listed} or 24, the hours between two loads of the "
            f"current day, or 0 for none, not {text!r}"
        )
    return allowed[text]


def backup_minutes(
    backup_time: str | None = None, zone: str | None = None, today: date | None = None
) -> tuple[int, ...]:
    """The minutes of the day in UTC at which the daily backup starts in the year from today, in
    order: backup_time as HH:MM in the time zone zone, or, without them, BACKUP_TIME and BACKUP_TZ
    from the environment, empty taking 03:00 and UTC. A zone with summer time gives two, one for
    each offset from UTC. A value that is not a time of day or a time zone raises ValueError with
    a line that names the setting."""
    text = (
        os.environ.get(BACKUP_TIME_SETTING, "") if backup_time is None else backup_time
    ).strip() or DEFAULT_BACKUP_TIME
    if not TIME_OF_DAY.fullmatch(text):
        raise ValueError(
            f"{BACKUP_TIME_SETTING} in .env must be a time of day as HH:MM, such as 03:00, "
            f"not {text!r}"
        )
    name = (os.environ.get(BACKUP_TZ_SETTING, "") if zone is None else zone).strip()
    try:
        # UTC needs no data of the time zones; the others come from tzdata, which Airflow has
        # through pendulum.
        info = UTC if name in ("", DEFAULT_BACKUP_TZ) else ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError):
        raise ValueError(
            f"{BACKUP_TZ_SETTING} in .env must be a time zone, such as Europe/Berlin, not {name!r}"
        ) from None
    start = time.fromisoformat(text)
    first = datetime.now(UTC).date() if today is None else today
    minutes = set()
    # Each day of a year, so that both offsets of a zone with summer time come.
    for days_after in range(366):
        moment = datetime.combine(first + timedelta(days=days_after), start, tzinfo=info)
        utc = moment.astimezone(UTC)
        minutes.add(utc.hour * 60 + utc.minute)
    return tuple(sorted(minutes))


def distance(first: int, second: int) -> int:
    """The minutes between two minutes of the day, either way round midnight."""
    apart = (first - second) % DAY
    return min(apart, DAY - apart)


def run_hours(
    hours: int, backup: Iterable[int] | None = None, minute: int = MINUTE
) -> tuple[int, ...]:
    """The hours of the day in UTC, in order, of the runs of a cycle every hours hours at minute
    past the hour, which keep at least BACKUP_DISTANCE from each minute of the day in UTC of backup,
    or, without it, of backup_minutes() from the environment. The runs start at midnight unless one
    would fall closer to the backup: then they all move later by the fewest whole hours that keep
    every run away, and when no move does, by the fewest hours that keep the most runs, without
    the runs that still fall closer. No hours for 0, which has no runs."""
    if hours not in PERIODS:
        raise ValueError(f"{hours} hours is not a period of the short cycle")
    if hours == 0:
        return ()
    starts = backup_minutes() if backup is None else tuple(backup)
    least = BACKUP_DISTANCE // timedelta(minutes=1)
    best: tuple[int, ...] = ()
    for shift in range(hours):
        kept = tuple(
            hour
            for hour in range(shift, 24, hours)
            if all(distance(hour * 60 + minute, start) >= least for start in starts)
        )
        if len(kept) > len(best):
            best = kept
        if len(kept) == 24 // hours:
            break
    return tuple(sorted(best))


def cron(hours: int, backup: Iterable[int] | None = None, minute: int = MINUTE) -> str | None:
    """The cron expression in UTC of the runs of a cycle every hours hours at minute past the hour,
    at the hours of run_hours, which keep away from the daily backup, or none for 0: the DAG then
    has no schedule."""
    kept = run_hours(hours, backup, minute)
    if not kept:
        return None
    if kept == tuple(range(0, 24, hours)):
        # From midnight, with no run left out: the short form.
        hour = "*" if hours == 1 else "0" if hours == 24 else f"*/{hours}"
    else:
        hour = ",".join(str(each) for each in kept)
    return f"{minute} {hour} * * *"


def waited_too_long(run_after: datetime, now: datetime, hours: int) -> bool:
    """Whether a run that was to start at run_after has, at now, waited longer than the period of
    hours hours, after which the next run comes: such a run ends without a load. A run by hand of
    a DAG without a schedule, at 0, never waits too long."""
    return hours > 0 and now - run_after > timedelta(hours=hours)


def days(current: date, whole: date) -> tuple[date, date]:
    """The first and the last day of a load of the kind intraday of an account whose current day,
    in its time zone, is current: that day, and the day before while no whole load covers it, whole
    being the last day that one covers, so that between midnight in the account and the daily load
    the day before goes on growing (ADR-0051). Both come in one read."""
    before = current - timedelta(days=1)
    return (current if whole >= before else before), current
