"""What every fictional platform of the demo shares: the seed, the hash that every number comes
from, the calendar of the demo world, the days of a load, and how a number that a platform
reports matures with the age of its day.

Every number is an integer function of a BLAKE2b hash of the seed and of the parts that name it,
so the records are the same on every Python version and machine. Money is computed in whole
micros or cents. No random module and no clock: the caller passes the moment of the load, now,
an aware datetime.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator, Sequence
from datetime import date, datetime, timedelta
from itertools import pairwise
from zoneinfo import ZoneInfo

SEED = "agoranomos-demo-v2"
# The days of history that the first load brings: 18 months, more than 13 for a comparison of a
# day with the same day a year before, and fewer than the 37 months that Google Ads and Meta keep.
HISTORY_DAYS = 548
# The day that the periodic rules of the demo world count from: a Monday.
EPOCH = date(2020, 1, 6)
# How much the demo world buys, in percent: Monday to Sunday, and January to December.
DOW_PCT = (105, 110, 108, 104, 98, 85, 90)
MONTH_PCT = (90, 88, 95, 98, 100, 97, 94, 96, 100, 104, 125, 140)

# How much of its final value a number shows at an age in days, in per mille: points of (age,
# share), with straight lines between them. A curve ends at 1000 on the last day of the reload
# window of its platform: a day older than that never changes again.
Curve = Sequence[tuple[int, int]]
# Impressions, clicks and spend: nearly final the next day, and final the day after.
DELIVERY: Curve = ((1, 980), (2, 1000))


def h(*parts: object) -> int:
    """A number of 64 bits that the seed and the parts fix."""
    key = "|".join([SEED, *map(str, parts)]).encode()
    return int.from_bytes(hashlib.blake2b(key, digest_size=8).digest(), "big")


def pct(low: int, high: int, *parts: object) -> int:
    """A whole percentage from low to high, both included, that the parts fix."""
    return low + h(*parts) % (high - low + 1)


def days(first: date, last: date) -> Iterator[date]:
    """The days from first to last, both included; none when first is after last."""
    day = first
    while day <= last:
        yield day
        day += timedelta(days=1)


def local_today(now: datetime, time_zone: str) -> date:
    """The day that it is at the moment now in a time zone of IANA, such as Etc/GMT+8."""
    if now.tzinfo is None:
        raise ValueError("now must be an aware datetime")
    return now.astimezone(ZoneInfo(time_zone)).date()


def last_final_day(now: datetime, time_zone: str) -> date:
    """Yesterday in the time zone of an account: the last day that a load takes (ADR-0004)."""
    return local_today(now, time_zone) - timedelta(days=1)


def history_first_day(now: datetime, time_zone: str) -> date:
    """The first day of the history that the first load takes for an account of the time zone."""
    return last_final_day(now, time_zone) - timedelta(days=HISTORY_DAYS - 1)


def age(now: datetime, time_zone: str, day: date) -> int:
    """How many days old a day is at the moment now, in the time zone of its account: 1 for
    yesterday."""
    return (local_today(now, time_zone) - day).days


def demand_pct(day: date) -> int:
    """How much the demo world buys on a day, in percent of an average day: the day of the week,
    the month, and a growth of 8 percent a year from 2025."""
    growth = max(80, 100 + 8 * (day.year - 2025))
    return DOW_PCT[day.weekday()] * MONTH_PCT[day.month - 1] * growth // 10_000


def share(curve: Curve, days_old: int) -> int:
    """The share of its final value, in per mille, that a number of the curve shows at an age."""
    if days_old <= curve[0][0]:
        return curve[0][1]
    for (age0, share0), (age1, share1) in pairwise(curve):
        if days_old <= age1:
            return share0 + (share1 - share0) * (days_old - age0) // (age1 - age0)
    return curve[-1][1]


def matured(final: int, days_old: int, curve: Curve) -> int:
    """The value that a number of the final value, a whole number of its smallest unit, shows at
    an age, by the curve."""
    return final * share(curve, days_old) // 1000


def major(minor: int, places: int = 2) -> str:
    """An amount in its smallest unit as a string in major units: 5312 is "53.12"."""
    sign = "-" if minor < 0 else ""
    whole, part = divmod(abs(minor), 10**places)
    return f"{sign}{whole}.{part:0{places}d}"


def trimmed(minor: int, places: int = 2) -> str:
    """As major(), without the zeros at the end, as Meta writes money: "5339.5", "53"."""
    return major(minor, places).rstrip("0").rstrip(".") or "0"
