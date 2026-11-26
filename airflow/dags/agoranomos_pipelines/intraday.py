"""intraday: the period of the short cycle of the current day, which the DAGs of the short cycle
read when the DAG processor parses them and when their tasks run (ADR-0051).

A load of the kind intraday takes the current day of each account in its time zone, and the day
before while no whole load covers it, at the level of campaigns, into the raw tables of its source,
and the one transform takes it like any load. AGORANOMOS_INTRADAY_HOURS of .env gives the hours
between two such loads: a divisor of 24, 1, 2, 3, 4, 6, 8, 12 or 24, so that the loads fall at the
same hours every day, at MINUTE past the hour in UTC, or 0 for no short cycle; empty takes 3, as
compose.yaml does. Any other value stops the parse of the DAG with a line that says what is
allowed, as ./agoranomos stops before up. A run that waited longer than the period, behind a load
of its source in the pool of the loads or for a slot of the executor, ends without a load, as the
next one comes (waited_too_long).
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta

SETTING = "AGORANOMOS_INTRADAY_HOURS"
DEFAULT_HOURS = 3
# The periods in hours: the divisors of 24, and 0 for none.
PERIODS = (0, 1, 2, 3, 4, 6, 8, 12, 24)
# The minute of each load in UTC: away from the daily loads of Meta and of Google Ads, at 10:07 and
# 10:37 UTC, and from the watcher, at 4, 19, 34 and 49 past each hour.
MINUTE = 27


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


def cron(hours: int) -> str | None:
    """The cron expression of a load every hours hours at MINUTE past the hour in UTC, starting at
    midnight, or none for 0: the DAG then has no schedule."""
    if hours not in PERIODS:
        raise ValueError(f"{hours} hours is not a period of the short cycle")
    if hours == 0:
        return None
    if hours == 1:
        hour = "*"
    elif hours == 24:
        hour = "0"
    else:
        hour = f"*/{hours}"
    return f"{MINUTE} {hour} * * *"


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


def whole_end(
    account_id: str, core: dict[str, date], journal: dict[str, tuple[date, date]]
) -> date | None:
    """The last day that a whole load of an account covers, accepted in the core (core, the last
    accepted day of each account) or whole in the journal (journal, as loads.recorded gives it),
    pinned or not: the short cycle takes the day before today while this is earlier (days). None
    when no whole load has taken the account, which the short cycle then leaves for its first daily
    load, as the check of the accounts of the transform knows an account of an intraday load from
    the list of a whole load. Google Ads, Meta and TikTok share it."""
    known = [day for day in (core.get(account_id), journal.get(account_id, (None, None))[1]) if day]
    return max(known) if known else None
