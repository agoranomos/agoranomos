"""Tests of the five fictional platforms of the demo generator and of their list
(airflow/dags/agoranomos_pipelines/demo/): the shape of every record, determinism, the maturity of
the numbers within the reload window of each platform, the names as of the load, the missing day,
and the rules that each API has of its own."""

import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest
from agoranomos_pipelines.demo import (
    common,
    fx,
    platforms,
    search,
    shapes,
    social,
    video,
    web,
    world,
)

NOW = datetime(2026, 10, 12, 9, 0, tzinfo=UTC)
MODULES = [p.module for p in platforms.PLATFORMS.values()]
FACT_TABLES = {
    "demo_search": "campaign_stats",
    "demo_social": "insights_campaign",
    "demo_video": "report_campaign_daily",
    "demo_web": "report_sessions_daily",
    "demo_fx": "rates_daily",
}
# The checksums of the full history as of NOW, one for each platform: a change of the generator
# shows here. They come from a first run of the generator, checked by a person.
GOLDEN = {
    "demo_fx": "5abc425fd6176c725d9a5de32d69121604b951fb96972781cd64cfa59e2643fb",
    "demo_search": "a91765f33f4dbdb2a97e997a1e2f5eb3cd639e84c927a8c6b38aa970b94d1be1",
    "demo_social": "6920405dfd561491dcad5e580c0ca6f8fc6f51009f3f1d5c10ab814183cee2d2",
    "demo_video": "38387e18756627fe081f53cb998e24f26fc1654fc7c321e5cf1187e293d8a19b",
    "demo_web": "f0de0da43fbf82c790f62eabf6422bde13a958115624661fb1698518de8e4b58",
}


# Moments at which the time zones of the demo disagree on the day: at 23:30 UTC Berlin has begun
# the next day, and at 05:00 UTC Los Angeles and Etc/GMT+8 are still on the day before.
EDGES = [datetime(2026, 10, 12, 23, 30, tzinfo=UTC), datetime(2026, 10, 12, 5, 0, tzinfo=UTC)]
# The account and the day of a row of the main table of each platform, as its API writes them.
ACCOUNT_AND_DAY = {
    "demo_search": lambda r: (r["customer"]["id"], r["segments"]["date"]),
    "demo_social": lambda r: (r["account_id"], r["date_start"]),
    "demo_video": lambda r: (r["advertiser_id"], r["dimensions"]["stat_time_day"][:10]),
    "demo_web": lambda r: (
        r["property"].split("/")[1],
        f"{r['date'][:4]}-{r['date'][4:6]}-{r['date'][6:]}",
    ),
}


def last_month(account):
    return common.last_final_day(NOW, account.time_zone) - timedelta(days=29)


def last_days(code, now):
    """The last day of the facts of each account of a platform, from the records of its main
    table as of now."""
    platform = platforms.PLATFORMS[code]
    found = {}
    for record in platform.module.records(now)[platform.main_table]:
        account, day = ACCOUNT_AND_DAY[code](record)
        found[account] = max(found.get(account, day), day)
    return {account: date.fromisoformat(day) for account, day in found.items()}


def all_records(module, now=NOW, first=None, skip=frozenset()):
    return {table: list(rows) for table, rows in module.records(now, first, skip).items()}


def checksum(records):
    text = "\n".join(
        json.dumps({"table": t, "row": r}, sort_keys=True)
        for t in sorted(records)
        for r in records[t]
    )
    return hashlib.sha256(text.encode()).hexdigest()


@pytest.mark.parametrize("module", MODULES, ids=lambda m: m.PLATFORM)
def test_every_record_has_the_shape_of_its_api(module):
    records = all_records(module, first=last_month)
    assert set(records) == {t for p, t in shapes.SHAPES if p == module.PLATFORM}
    for table, rows in records.items():
        assert rows, f"{module.PLATFORM}.{table} yields no record"
        for row in rows:
            assert shapes.problems(module.PLATFORM, table, row) == [], (table, row)
    assert shapes.API[module.PLATFORM] == module.API


@pytest.mark.parametrize("module", MODULES, ids=lambda m: m.PLATFORM)
def test_the_same_moment_gives_the_same_records(module):
    assert checksum(all_records(module, first=last_month)) == checksum(
        all_records(module, first=last_month)
    )


@pytest.mark.parametrize("module", MODULES, ids=lambda m: m.PLATFORM)
def test_the_history_has_its_golden_checksum(module):
    # Checked on Python 3.12, 3.13 and 3.14 (Step 4).
    assert checksum(all_records(module)) == GOLDEN[module.PLATFORM]


WINDOW_FIELDS = {
    # The numbers of a record that mature, by platform, as functions of a record.
    "demo_search": lambda r: (
        r["metrics"]["impressions"],
        r["metrics"]["costMicros"],
        r["metrics"]["conversions"],
    ),
    "demo_social": lambda r: (
        r["impressions"],
        r["spend"],
        json.dumps(r["actions"], sort_keys=True),
    ),
    "demo_video": lambda r: (r["metrics"]["spend"], r["metrics"]["complete_payment"]),
    "demo_web": lambda r: (r["sessions"], r["keyEvents"], r["totalRevenue"]),
    "demo_fx": lambda r: (r["rate"],),
}
# The key of a record of a fact table, with its day first, as YYYY-MM-DD.
WINDOW_KEYS = {
    "demo_search": lambda r: (r["segments"]["date"], r["customer"]["id"], r["campaign"]["id"]),
    "demo_social": lambda r: (r["date_start"], r["account_id"], r["campaign_id"]),
    "demo_video": lambda r: (
        r["dimensions"]["stat_time_day"][:10],
        r["advertiser_id"],
        r["dimensions"]["campaign_id"],
    ),
    "demo_web": lambda r: (
        f"{r['date'][:4]}-{r['date'][4:6]}-{r['date'][6:]}",
        r["property"],
        r["sessionSource"],
        r["sessionMedium"],
        r["sessionCampaignId"],
        r["sessionCampaignName"],
    ),
    "demo_fx": lambda r: (r["rate_date"],),
}


def by_key(module, now):
    table = FACT_TABLES[module.PLATFORM]
    key, fields = WINDOW_KEYS[module.PLATFORM], WINDOW_FIELDS[module.PLATFORM]

    def first(account):
        return common.last_final_day(NOW, account.time_zone) - timedelta(days=120)

    return {key(r): fields(r) for r in module.records(now, first)[table]}


@pytest.mark.parametrize("module", MODULES, ids=lambda m: m.PLATFORM)
def test_a_day_changes_only_within_the_window_of_its_platform(module):
    window = platforms.PLATFORMS[module.PLATFORM].window_days
    before, after = by_key(module, NOW), by_key(module, NOW + timedelta(days=10))
    changed = [k for k in before if k in after and before[k] != after[k]]
    for k in changed:
        # Changed between the two loads: the day was inside the window at the first of them (at
        # 09:00 UTC it is 12 October in every time zone of the demo).
        assert (NOW.date() - date.fromisoformat(k[0])).days <= window, (module.PLATFORM, k)
    if module is not fx:
        assert changed, f"no day of {module.PLATFORM} matured between the two loads"


def test_meta_corrects_some_conversions_downwards():
    # The estimate of the first week runs above the final count: a day of age 7 shows more
    # purchases than the same day at age 14. The demo has some 10 to 30 purchases a campaign a
    # day, too few for the 3 percent to show in their count, so the test reads their value.
    def first(account):
        return common.last_final_day(NOW, account.time_zone) - timedelta(days=6)

    def purchases(now):
        found = {}
        for row in social.records(now, first)["insights_campaign"]:
            for action in row["action_values"]:
                if action["action_type"] == "offsite_conversion.fb_pixel_purchase":
                    found[(row["campaign_id"], row["date_start"])] = Decimal(action["value"])
        return found

    week, fortnight = purchases(NOW), purchases(NOW + timedelta(days=7))
    assert any(fortnight[k] < week[k] for k in week if k in fortnight)


def test_every_row_of_a_load_has_the_name_of_its_campaign_as_of_the_load():
    rows = [
        r for r in search.records(NOW)["campaign_stats"] if r["campaign"]["id"] == "21000000102"
    ]
    assert {r["segments"]["date"][:7] for r in rows} >= {"2026-03", "2026-09"}, (
        "the history spans the renames"
    )
    assert {r["campaign"]["name"] for r in rows} == {"Prospecting - Autumn Range"}
    listed = {
        r["campaign"]["id"]: r["campaign"]["name"]
        for r in search.records(datetime(2026, 3, 1, 9, tzinfo=UTC))["campaigns"]
    }
    assert listed["21000000102"] == "Prospecting - Spring Range"


def test_the_history_leaves_out_the_gap_day_of_one_account_only():
    skip = platforms.gap(NOW)
    [(account, day)] = skip
    rows = list(social.records(NOW, skip=skip)["insights_campaign"])
    assert not [
        r for r in rows if r["account_id"] == account and r["date_start"] == day.isoformat()
    ]
    assert [r for r in rows if r["account_id"] != account and r["date_start"] == day.isoformat()]
    tz = next(a.time_zone for a in social.accounts() if a.account_id == account)
    assert (common.last_final_day(NOW, tz) - day).days == platforms.GAP_DAYS_AGO
    assert max(p.window_days for p in platforms.PLATFORMS.values()) < platforms.GAP_DAYS_AGO


def test_a_campaign_off_its_flight_or_season_has_no_rows():
    days = {
        r["segments"]["date"]
        for r in search.records(NOW)["campaign_stats"]
        if r["campaign"]["id"] == "21000000105"
    }
    assert days
    assert all(d[5:7] in ("11", "12") for d in days)


def test_google_reports_micros_that_are_not_whole_cents_and_doubles_as_numbers():
    rows = search.records(NOW, last_month)["campaign_stats"]
    micros = [int(r["metrics"]["costMicros"]) for r in rows]
    assert sum(1 for m in micros if m % 10_000) > len(micros) * 0.9
    assert all(isinstance(r["metrics"]["conversions"], float) for r in rows)


def test_google_has_an_action_outside_the_conversions_and_the_manager_has_no_statistics():
    rows = list(search.records(NOW, last_month)["campaign_conversions"])
    calls = [r for r in rows if r["segments"]["conversionActionCategory"] == "PHONE_CALL_LEAD"]
    assert calls
    assert all(
        r["metrics"]["conversions"] == 0 and r["metrics"]["allConversions"] > 0 for r in calls
    )
    stats = list(search.records(NOW, last_month)["campaign_stats"])
    assert world.MANAGER.account_id not in {r["customer"]["id"] for r in stats}
    clients = list(search.records(NOW)["customer_clients"])
    assert [c["customerClient"]["manager"] for c in clients].count(True) == 1


def test_the_conversions_of_a_campaign_are_its_counted_actions():
    stats = {
        (r["customer"]["id"], r["campaign"]["id"], r["segments"]["date"]): r
        for r in search.records(NOW, last_month)["campaign_stats"]
    }
    summed = {}
    for r in search.records(NOW, last_month)["campaign_conversions"]:
        key = (r["customer"]["id"], r["campaign"]["id"], r["segments"]["date"])
        summed[key] = summed.get(key, Decimal(0)) + Decimal(str(r["metrics"]["conversions"]))
    for key, row in stats.items():
        assert Decimal(str(row["metrics"]["conversions"])) == summed.get(key, Decimal(0)), key


def test_meta_counts_purchases_twice_across_omni_purchase():
    row = next(
        r
        for r in social.records(NOW, last_month)["insights_campaign"]
        if r["campaign_id"] == "120200000000101"
    )
    types = {a["action_type"]: a["value"] for a in row["actions"]}
    assert types["omni_purchase"] == types["offsite_conversion.fb_pixel_purchase"]
    assert all(
        set(a) == {"action_type", "value", "1d_click", "7d_click", "1d_view"}
        for a in row["actions"]
    )


def test_tiktok_writes_a_dash_and_leaves_out_removed_campaigns():
    rows = list(video.records(NOW)["report_campaign_daily"])
    assert any(r["metrics"]["cost_per_conversion"] == "-" for r in rows)
    removed = {
        c.campaign_id
        for cycle in world.CYCLES
        if cycle.template.platform == "demo_video"
        for c in cycle.rounds(NOW.date() - timedelta(days=600), NOW.date())
        if c.removed(NOW.date())
    }
    assert removed
    assert not removed & {r["dimensions"]["campaign_id"] for r in rows}
    assert all(r["dimensions"]["stat_time_day"].endswith(" 00:00:00") for r in rows)


def test_web_analytics_links_the_sessions_of_ads_to_their_campaigns():
    rows = list(web.records(NOW, last_month)["report_sessions_daily"])
    ads = [r for r in rows if r["sessionMedium"] in ("cpc", "paid")]
    ids = {c.campaign_id for c in world.CAMPAIGNS} | {
        c.campaign_id
        for cycle in world.CYCLES
        for c in cycle.rounds(NOW.date() - timedelta(days=60), NOW.date())
    }
    assert ads
    assert {r["sessionCampaignId"] for r in ads} <= ids
    assert {r["sessionCampaignId"] for r in rows if r not in ads} == {"(not set)"}
    assert all(len(r["date"]) == 8 and r["date"].isdigit() for r in rows)


def test_the_rates_come_on_weekdays_but_holidays_and_start_before_any_history():
    rows = list(fx.records(NOW)["rates_daily"])
    days = [date.fromisoformat(r["rate_date"]) for r in rows]
    assert all(d.weekday() < 5 and (d.month, d.day) not in fx.HOLIDAYS for d in days)
    first = min(common.history_first_day(NOW, a.time_zone) for a in world.ACCOUNTS)
    assert min(days) <= first
    assert all(len(r["rate"].split(".")[1]) == 4 for r in rows)


@pytest.mark.parametrize("now", EDGES, ids=["late", "early"])
@pytest.mark.parametrize("code", sorted(ACCOUNT_AND_DAY))
def test_each_account_loads_up_to_its_own_yesterday(code, now):
    # At an edge of the day of UTC, a load that took the yesterday of UTC, or of another account,
    # would bring a day that an account has not finished, or leave out one that it has.
    assert len({common.last_final_day(now, a.time_zone) for a in world.ACCOUNTS}) > 1, now
    platform = platforms.PLATFORMS[code]
    expected = {
        a.account_id: common.last_final_day(now, a.time_zone) for a in platform.module.accounts()
    }
    assert last_days(code, now) == expected
    assert {account: last for account, (_, last) in platform.module.windows(now).items()} == (
        expected
    )


@pytest.mark.parametrize("now", EDGES, ids=["late", "early"])
def test_the_rates_cover_every_day_of_every_account_at_the_edges(now):
    # The newest day of the facts, Berlin's at 23:30 UTC, has its rate, and so has the oldest.
    [(first, last)] = fx.windows(now).values()
    assert last == max(common.last_final_day(now, a.time_zone) for a in world.ACCOUNTS)
    assert first <= min(common.history_first_day(now, a.time_zone) for a in world.ACCOUNTS)
    days = [date.fromisoformat(r["rate_date"]) for r in fx.records(now)["rates_daily"]]
    assert days == [day for day in common.days(first, last) if fx.published(day)]


def test_the_list_of_platforms_names_where_each_loads():
    assert platforms.LOAD_ORDER == (
        "demo_fx",
        "demo_search",
        "demo_social",
        "demo_video",
        "demo_web",
    )
    search_platform = platforms.PLATFORMS["demo_search"]
    assert (search_platform.dataset, search_platform.pipeline) == (
        "raw_demo_search",
        "agoranomos_demo_search",
    )
    assert (
        platforms.asset_uri(search_platform)
        == "postgres://warehouse-db:5432/warehouse/raw_demo_search/campaign_stats"
    )
    assert {p.code: p.window_days for p in platforms.PLATFORMS.values()} == {
        "demo_fx": 7,
        "demo_search": 90,
        "demo_social": 28,
        "demo_video": 28,
        "demo_web": 14,
    }
    for p in platforms.PLATFORMS.values():
        assert p.main_table in {t for q, t in shapes.SHAPES if q == p.code}


def test_each_platform_names_how_its_fact_tables_hold_an_account_and_a_day():
    # A load replaces the days of each account through these, and a reader finds the last day of
    # each account through them, without dlt.
    assert {p.code: [f.table for f in p.facts] for p in platforms.PLATFORMS.values()} == {
        "demo_fx": ["rates_daily"],
        "demo_search": ["campaign_stats", "campaign_conversions"],
        "demo_social": ["insights_campaign"],
        "demo_video": ["report_campaign_daily"],
        "demo_web": ["report_sessions_daily"],
    }
    for p in platforms.PLATFORMS.values():
        assert p.facts[0].table == p.main_table
        for fact in p.facts:
            assert (fact.account_sql is None) == (p.code == "demo_fx"), fact
            assert fact.day_text in ("%Y-%m-%d", "%Y%m%d"), fact
    [web] = platforms.PLATFORMS["demo_web"].facts
    assert web.account_text.format("390000001") == "properties/390000001"
    assert date(2026, 10, 11).strftime(web.day_text) == "20261011"
    assert web.day_date_sql == "to_date(\"date\", 'YYYYMMDD')"
    [video] = platforms.PLATFORMS["demo_video"].facts
    assert video.day_date_sql == 'left("dimensions__stat_time_day"::text, 10)::date'
