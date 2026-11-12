"""demo_fx: a fictional source of the reference rates of the euro, a rate of US dollars and one of
pounds sterling for one euro on each day of publication, which is each weekday but four of the
closing days of TARGET, with four decimals, as the European Central Bank writes them (ADR-0025).
The days without a rate take the last one in the model. The pound is a currency of no account of
the demo, so that a reporting currency of GBP shows how the core takes the history of the rates
of a new reporting currency again (agoranomos_pipelines/rates.py, ADR-0042)."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date, datetime, timedelta

from agoranomos_pipelines.demo import world
from agoranomos_pipelines.demo.common import days, h, history_first_day, last_final_day, major

PLATFORM = "demo_fx"
API = "a source of euro reference rates"
HOLIDAYS = ((1, 1), (5, 1), (12, 25), (12, 26))
# The rate rises and falls over the year, in ten-thousandths, around 1.0800 for the US dollar and
# 0.8600 for the pound, each in the order in which a day of publication lists them.
WAVE = (0, 40, 80, 120, 80, 40, 0, -40, -80, -120, -80, -40)
QUOTES = {
    "USD": (10_800, WAVE, 61),
    "GBP": (8_600, (0, -20, -40, -60, -40, -20, 0, 20, 40, 60, 40, 20), 41),
}
# The rates start a week before the first day of any history, so that its first day has one.
LEAD_DAYS = 7


def accounts() -> tuple[world.Account, ...]:
    return (world.RATES,)


def published(day) -> bool:
    return day.weekday() < 5 and (day.month, day.day) not in HOLIDAYS


def rate_ten_thousandths(day, quote: str = "USD") -> int:
    """The rate of a currency for one euro on a day, in ten-thousandths. The US dollar keeps the
    seed of the rates before the pound came, so its rates stay those of earlier releases."""
    middle, wave, spread = QUOTES[quote]
    parts = (PLATFORM, day.isoformat()) if quote == "USD" else (PLATFORM, quote, day.isoformat())
    return middle + wave[day.month - 1] + h(*parts) % spread - spread // 2


def rates_daily(now, first=None, skip=frozenset()) -> Iterator[dict[str, object]]:
    [(start, last)] = windows(now, first).values()
    for day in days(start, last):
        if published(day):
            for quote in QUOTES:
                yield {
                    "rate_date": day.isoformat(),
                    "base": "EUR",
                    "quote": quote,
                    "rate": major(rate_ten_thousandths(day, quote), 4),
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
