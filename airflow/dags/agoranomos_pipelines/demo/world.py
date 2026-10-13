"""The fictional business of the demo: two clients, their ad accounts on three fictional ad
platforms and their properties on a fictional platform of web analytics, and the campaigns of each
account (ADR-0024, ADR-0025).

The world runs on absolute dates with periodic rules, so that the value of a day never depends on
the day that it is loaded, but through its maturity (common.py), and the demo looks alike whenever
it starts. A campaign runs every day, in flights of some weeks on and some off, in a season of each
year, or as a test that a new campaign repeats every few months and that ends removed. Two
campaigns change their names, one at the start of each half-year and one at the start of each
year. The names are made up and name no real brand.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, time, timedelta

from agoranomos_pipelines.demo.common import (
    EPOCH,
    age,
    days,
    demand_pct,
    h,
    history_first_day,
    last_final_day,
    pct,
)

# How long a test campaign that has ended still shows in the list of the campaigns of its
# platform: two years.
ENTITY_HORIZON_DAYS = 730


@dataclass(frozen=True)
class Client:
    key: str
    name: str


@dataclass(frozen=True)
class Account:
    """An ad account, a property of web analytics, or the rates of a currency. TikTok reports in
    the time zone of an advertiser and shows people another one, display_time_zone."""

    platform: str
    account_id: str
    name: str
    client: str | None
    currency: str
    time_zone: str
    display_time_zone: str | None = None


@dataclass(frozen=True)
class Always:
    def active(self, day: date) -> bool:
        return True


@dataclass(frozen=True)
class Flight:
    """On for `on` days out of every `period` days, counted from `offset` days after the epoch."""

    period: int
    on: int
    offset: int = 0

    def active(self, day: date) -> bool:
        return ((day - EPOCH).days - self.offset) % self.period < self.on


@dataclass(frozen=True)
class Season:
    """On from the month and day `start` to the month and day `end` of each year."""

    start: tuple[int, int]
    end: tuple[int, int]

    def active(self, day: date) -> bool:
        return self.start <= (day.month, day.day) <= self.end


@dataclass(frozen=True)
class Fixed:
    name: str

    def at(self, day: date) -> str:
        return self.name

    def changes(self, first: date, last: date) -> list[date]:
        return []


@dataclass(frozen=True)
class HalfYear:
    """One name from January to June, and another from July to December."""

    first_half: str
    second_half: str

    def at(self, day: date) -> str:
        return self.first_half if day.month <= 6 else self.second_half

    def changes(self, first: date, last: date) -> list[date]:
        """The days after first, up to last, on which the name changes."""
        found = [date(y, m, 1) for y in range(first.year, last.year + 1) for m in (1, 7)]
        return [day for day in found if first < day <= last]


@dataclass(frozen=True)
class Yearly:
    """A name with the year in it, such as "Retargeting {year}"."""

    template: str

    def at(self, day: date) -> str:
        return self.template.format(year=day.year)

    def changes(self, first: date, last: date) -> list[date]:
        found = [date(year, 1, 1) for year in range(first.year, last.year + 1)]
        return [day for day in found if first < day <= last]


@dataclass(frozen=True)
class Campaign:
    """A campaign of an ad account, and how it runs. Money is in the smallest unit of the currency
    of its account: cents, for USD and EUR."""

    platform: str
    account_id: str
    campaign_id: str
    kind: str  # the channel type of Google, or the objective of Meta or of TikTok
    rule: Always | Flight | Season
    names: Fixed | HalfYear | Yearly
    impressions: int  # on an average day
    ctr_bp: int  # clicks for each impression, in basis points
    cpc_minor: int  # the cost of a click
    cvr_bp: int  # conversions for each click, in basis points
    aov_minor: int  # the value of a conversion
    start: date | None = None  # the first day of a test of a cycle
    end: date | None = None  # its last day; it is removed after it

    def exists(self, day: date) -> bool:
        return self.start is None or self.start <= day

    def removed(self, day: date) -> bool:
        return self.end is not None and day > self.end

    def active(self, day: date) -> bool:
        return self.exists(day) and not self.removed(day) and self.rule.active(day)

    def name_at(self, day: date) -> str:
        return self.names.at(day)


@dataclass(frozen=True)
class Cycle:
    """A test that a new campaign repeats every `period` days, counted from `offset` days after the
    epoch, for `life` days, and that ends removed. The campaign of round n has the ID of the
    template plus n, and the name of the template with " #n"."""

    template: Campaign
    period: int
    life: int
    offset: int

    def round_of(self, n: int) -> Campaign:
        start = EPOCH + timedelta(days=self.offset + n * self.period)
        return replace(
            self.template,
            campaign_id=str(int(self.template.campaign_id) + n),
            names=Fixed(f"{self.template.names.at(start)} #{n}"),
            start=start,
            end=start + timedelta(days=self.life - 1),
        )

    def rounds(self, first: date, last: date) -> Iterator[Campaign]:
        """The campaigns of the rounds that run on any day from first to last."""
        n = max(0, ((first - EPOCH).days - self.offset - self.life) // self.period)
        while True:
            campaign = self.round_of(n)
            if campaign.start > last:
                return
            if campaign.end >= first:
                yield campaign
            n += 1


@dataclass(frozen=True)
class Final:
    """The statistics of a campaign on a day, once they no longer change."""

    impressions: int
    clicks: int
    spend_micros: int
    conversions_hundredths: int
    value_minor: int


CLIENTS = (Client("tarnwick", "Tarnwick Outfitters"), Client("brindlemoor", "Brindlemoor Haus"))

# The manager account of demo_search: Google gives no metrics of a manager account.
MANAGER = Account(
    "demo_search", "4170000001", "Demo Manager Account", None, "USD", "America/New_York"
)

ACCOUNTS = (
    Account(
        "demo_search",
        "4170000011",
        "Tarnwick Outfitters - Search",
        "tarnwick",
        "USD",
        "America/New_York",
    ),
    Account(
        "demo_search",
        "4170000012",
        "Brindlemoor Haus - Suche",
        "brindlemoor",
        "EUR",
        "Europe/Berlin",
    ),
    Account(
        "demo_social",
        "802000000000001",
        "Tarnwick Outfitters",
        "tarnwick",
        "USD",
        "America/Los_Angeles",
    ),
    Account(
        "demo_social", "802000000000002", "Brindlemoor Haus", "brindlemoor", "EUR", "Europe/Berlin"
    ),
    Account(
        "demo_video",
        "7300000000000000001",
        "Tarnwick Outfitters",
        "tarnwick",
        "USD",
        "Etc/GMT+8",
        display_time_zone="America/Los_Angeles",
    ),
    Account("demo_web", "390000001", "tarnwick.example", "tarnwick", "USD", "America/New_York"),
    Account("demo_web", "390000002", "brindlemoor.example", "brindlemoor", "EUR", "Europe/Berlin"),
)
# The rates of the euro that demo_fx publishes, in the time zone of their publisher.
RATES = Account("demo_fx", "EUR", "Euro reference rates", None, "EUR", "Europe/Berlin")


def _campaign(platform, account_id, campaign_id, kind, rule, name, *economics) -> Campaign:
    names = Fixed(name) if isinstance(name, str) else name
    return Campaign(platform, account_id, campaign_id, kind, rule, names, *economics)


# platform, account, campaign, kind, rule, name, impressions, CTR (bp), CPC, CVR (bp), value.
CAMPAIGNS = (
    _campaign(
        "demo_search",
        "4170000011",
        "21000000101",
        "SEARCH",
        Always(),
        "Brand Search",
        12_000,
        600,
        80,
        800,
        8_500,
    ),
    _campaign(
        "demo_search",
        "4170000011",
        "21000000102",
        "SEARCH",
        Always(),
        HalfYear("Prospecting - Spring Range", "Prospecting - Autumn Range"),
        40_000,
        180,
        95,
        250,
        9_200,
    ),
    _campaign(
        "demo_search",
        "4170000011",
        "21000000103",
        "PERFORMANCE_MAX",
        Always(),
        "All Channels",
        60_000,
        90,
        60,
        300,
        7_800,
    ),
    _campaign(
        "demo_search",
        "4170000011",
        "21000000104",
        "SHOPPING",
        Flight(84, 56),
        "Shopping - Core",
        30_000,
        120,
        55,
        350,
        6_400,
    ),
    _campaign(
        "demo_search",
        "4170000011",
        "21000000105",
        "SEARCH",
        Season((11, 1), (12, 31)),
        "Holiday Sale",
        25_000,
        300,
        110,
        600,
        9_900,
    ),
    _campaign(
        "demo_search",
        "4170000011",
        "21000000106",
        "VIDEO",
        Flight(182, 91, 30),
        "Video Awareness",
        80_000,
        30,
        40,
        50,
        7_000,
    ),
    _campaign(
        "demo_search",
        "4170000012",
        "21000000201",
        "SEARCH",
        Always(),
        "Marke",
        8_000,
        650,
        70,
        900,
        12_000,
    ),
    _campaign(
        "demo_search",
        "4170000012",
        "21000000202",
        "SEARCH",
        Always(),
        "Generic Furniture",
        30_000,
        200,
        85,
        220,
        15_000,
    ),
    _campaign(
        "demo_search",
        "4170000012",
        "21000000203",
        "PERFORMANCE_MAX",
        Always(),
        "All Channels DE",
        40_000,
        80,
        55,
        280,
        11_000,
    ),
    _campaign(
        "demo_search",
        "4170000012",
        "21000000204",
        "SEARCH",
        Season((1, 2), (1, 31)),
        "Winter Sale",
        20_000,
        280,
        90,
        500,
        13_000,
    ),
    _campaign(
        "demo_search",
        "4170000012",
        "21000000205",
        "SHOPPING",
        Always(),
        "Shopping",
        25_000,
        110,
        50,
        320,
        9_000,
    ),
    _campaign(
        "demo_social",
        "802000000000001",
        "120200000000101",
        "OUTCOME_SALES",
        Always(),
        "Prospecting - Broad",
        90_000,
        110,
        60,
        200,
        8_800,
    ),
    _campaign(
        "demo_social",
        "802000000000001",
        "120200000000102",
        "OUTCOME_SALES",
        Always(),
        Yearly("Retargeting {year}"),
        30_000,
        160,
        50,
        450,
        9_100,
    ),
    _campaign(
        "demo_social",
        "802000000000001",
        "120200000000103",
        "OUTCOME_LEADS",
        Flight(56, 35, 7),
        "Lead Magnet",
        40_000,
        120,
        45,
        600,
        2_000,
    ),
    _campaign(
        "demo_social",
        "802000000000001",
        "120200000000104",
        "OUTCOME_SALES",
        Season((11, 15), (12, 2)),
        "Black Friday",
        70_000,
        180,
        70,
        500,
        9_500,
    ),
    _campaign(
        "demo_social",
        "802000000000002",
        "120200000000201",
        "OUTCOME_SALES",
        Always(),
        "Prospecting DE",
        60_000,
        100,
        55,
        180,
        12_500,
    ),
    _campaign(
        "demo_social",
        "802000000000002",
        "120200000000202",
        "OUTCOME_SALES",
        Always(),
        "Retargeting DE",
        20_000,
        150,
        45,
        420,
        12_800,
    ),
    _campaign(
        "demo_social",
        "802000000000002",
        "120200000000203",
        "OUTCOME_LEADS",
        Flight(70, 42, 14),
        "Leads DE",
        30_000,
        110,
        40,
        550,
        2_500,
    ),
    _campaign(
        "demo_video",
        "7300000000000000001",
        "1790000000000101",
        "WEB_CONVERSIONS",
        Always(),
        "Creator Videos - Always On",
        100_000,
        80,
        30,
        150,
        7_500,
    ),
    _campaign(
        "demo_video",
        "7300000000000000001",
        "1790000000000102",
        "WEB_CONVERSIONS",
        Flight(63, 28, 21),
        "Creator Collab",
        60_000,
        100,
        35,
        180,
        7_200,
    ),
    _campaign(
        "demo_video",
        "7300000000000000001",
        "1790000000000103",
        "WEB_CONVERSIONS",
        Season((11, 10), (12, 24)),
        "Holiday Push",
        80_000,
        110,
        40,
        220,
        8_000,
    ),
)
CYCLES = (
    Cycle(
        _campaign(
            "demo_search",
            "4170000011",
            "21000000700",
            "DISPLAY",
            Always(),
            "Display Test",
            50_000,
            40,
            35,
            80,
            6_000,
        ),
        period=120,
        life=45,
        offset=10,
    ),
    Cycle(
        _campaign(
            "demo_social",
            "802000000000001",
            "120200000000700",
            "OUTCOME_TRAFFIC",
            Always(),
            "Creative Test",
            50_000,
            90,
            40,
            60,
            5_000,
        ),
        period=90,
        life=30,
        offset=5,
    ),
    Cycle(
        _campaign(
            "demo_video",
            "7300000000000000001",
            "1790000000000700",
            "TRAFFIC",
            Always(),
            "Hashtag Test",
            70_000,
            60,
            25,
            40,
            5_000,
        ),
        period=150,
        life=40,
        offset=60,
    ),
)

FirstDay = Callable[[Account], date]


def accounts_of(platform: str) -> tuple[Account, ...]:
    return tuple(account for account in ACCOUNTS if account.platform == platform)


def campaigns_of(account_id: str, first: date, last: date) -> list[Campaign]:
    """The campaigns of an account that may run on a day from first to last: its own, and the
    rounds of its tests."""
    own = [c for c in CAMPAIGNS if c.account_id == account_id]
    tests = [
        campaign
        for cycle in CYCLES
        if cycle.template.account_id == account_id
        for campaign in cycle.rounds(first, last)
    ]
    return own + tests


def listed_campaigns(platform: str, day: date) -> list[Campaign]:
    """The campaigns that the API of the platform lists on a day: every one that exists, with the
    tests of the last two years. TikTok leaves out the removed ones, as its default filter
    STATUS_NOT_DELETE does."""
    horizon = day - timedelta(days=ENTITY_HORIZON_DAYS)
    found = [c for c in CAMPAIGNS if c.platform == platform] + [
        campaign
        for cycle in CYCLES
        if cycle.template.platform == platform
        for campaign in cycle.rounds(horizon, day)
        if campaign.exists(day)
    ]
    if platform == "demo_video":
        found = [campaign for campaign in found if not campaign.removed(day)]
    return found


def final(campaign: Campaign, day: date) -> Final | None:
    """The final statistics of a campaign on a day, or None on a day that it did not run."""
    if not campaign.active(day):
        return None
    key = (campaign.platform, campaign.campaign_id, day.isoformat())
    impressions = campaign.impressions * demand_pct(day) // 100 * pct(95, 105, *key, "i") // 100
    clicks = impressions * campaign.ctr_bp // 10_000 * pct(97, 103, *key, "c") // 100
    # Micros that are not whole cents, as those of Google are not.
    spend_micros = (
        clicks * campaign.cpc_minor * pct(96, 104, *key, "s") * 100 + h(*key, "m") % 9_973
    )
    conversions = clicks * campaign.cvr_bp * pct(90, 110, *key, "v") // 10_000
    value = conversions * campaign.aov_minor * pct(90, 110, *key, "a") // 10_000
    return Final(impressions, clicks, spend_micros, conversions, value)


def observed(
    platform: str,
    now: datetime,
    first: FirstDay | None = None,
    skip: frozenset[tuple[str, date]] = frozenset(),
) -> Iterator[tuple[Account, Campaign, date, Final, int]]:
    """Every campaign and day of the platform that a load at now reports, from first(account),
    by default the first day of the history, to yesterday in the time zone of the account, with
    its final statistics and its age; skip names the (account, day) that the load leaves out."""
    for account in accounts_of(platform):
        start = first(account) if first else history_first_day(now, account.time_zone)
        last = last_final_day(now, account.time_zone)
        for campaign in campaigns_of(account.account_id, start, last):
            for day in days(start, last):
                if (account.account_id, day) in skip:
                    continue
                stats = final(campaign, day)
                if stats is not None:
                    yield account, campaign, day, stats, age(now, account.time_zone, day)


def windows(
    platform: str, now: datetime, first: FirstDay | None = None
) -> dict[str, tuple[date, date]]:
    """The days of each account of a platform that a load at now takes: from first(account), by
    default the first day of the history, to yesterday in the time zone of the account. A load
    replaces exactly these days in the warehouse (sources.py)."""
    return {
        account.account_id: (
            first(account) if first else history_first_day(now, account.time_zone),
            last_final_day(now, account.time_zone),
        )
        for account in accounts_of(platform)
    }


def name_versions(platform: str, now: datetime) -> list[datetime]:
    """The moments whose names of campaigns the first load replays, in order, at 00:00 UTC: the
    first day of the history, and each day inside it on which a campaign of the platform changes
    its name. None for a platform whose campaigns keep their names."""
    accounts = accounts_of(platform)
    first = min(history_first_day(now, account.time_zone) for account in accounts)
    last = max(last_final_day(now, account.time_zone) for account in accounts)
    changes = sorted(
        {day for c in CAMPAIGNS if c.platform == platform for day in c.names.changes(first, last)}
    )
    if not changes:
        return []
    return [datetime.combine(day, time(), UTC) for day in (first, *changes)]
