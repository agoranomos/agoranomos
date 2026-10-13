"""demo_fx: a fictional source of the reference rates of the euro, a rate of US dollars for one
euro on each day of publication, which is each weekday but four of the closing days of TARGET,
with four decimals, as the European Central Bank writes them (ADR-0025). The days without a rate
take the last one in the model."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date, datetime, timedelta

from agoranomos_pipelines.demo import world
from agoranomos_pipelines.demo.common import days, h, history_first_day, last_final_day, major

PLATFORM = "demo_fx"
API = "a source of euro reference rates"
HOLIDAYS = ((1, 1), (5, 1), (12, 25), (12, 26))
# The rate rises and falls over the year, in ten-thousandths, around 1.0800.
WAVE = (0, 40, 80, 120, 80, 40, 0, -40, -80, -120, -80, -40)
# The rates start a week before the first day of any history, so that its first day has one.
LEAD_DAYS = 7


def accounts() -> tuple[world.Account, ...]:
    return (world.RATES,)


def published(day) -> bool:
    return day.weekday() < 5 and (day.month, day.day) not in HOLIDAYS


def rate_ten_thousandths(day) -> int:
    return 10_800 + WAVE[day.month - 1] + h(PLATFORM, day.isoformat()) % 61 - 30


def rates_daily(now, first=None, skip=frozenset()) -> Iterator[dict[str, object]]:
    [(start, last)] = windows(now, first).values()
    for day in days(start, last):
        if published(day):
            yield {
                "rate_date": day.isoformat(),
                "base": "EUR",
                "quote": "USD",
                "rate": major(rate_ten_thousandths(day), 4),
            }


def records(now, first=None, skip=frozenset()) -> dict[str, Iterator[dict[str, object]]]:
    return {"rates_daily": rates_daily(now, first, skip)}


def windows(now: datetime, first=None) -> dict[str, tuple[date, date]]:
    """The days whose rates a load at now takes: from first(RATES), by default a week before the
    first day of any history, to the last yesterday of any account."""
    if first:
        start = first(world.RATES)
    else:
        start = min(history_first_day(now, a.time_zone) for a in world.ACCOUNTS) - timedelta(
            days=LEAD_DAYS
        )
    return {
        world.RATES.account_id: (
            start,
            max(last_final_day(now, a.time_zone) for a in world.ACCOUNTS),
        )
    }


def name_versions(now: datetime) -> list[datetime]:
    return []
