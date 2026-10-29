"""Tests of the common rules and the world of the demo generator
(airflow/dags/agoranomos_pipelines/demo/common.py and world.py)."""

from datetime import UTC, date, datetime, timedelta

import pytest
from agoranomos_pipelines.demo import common, world

NOW = datetime(2026, 10, 12, 5, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    ("time_zone", "yesterday"),
    [
        # 05:00 UTC is 01:00 in New York, 07:00 in Berlin, and still the evening of 11 October in
        # Los Angeles and in Etc/GMT+8, which is UTC-8 (the sign of the Etc zones of POSIX).
        ("America/New_York", date(2026, 10, 11)),
        ("Europe/Berlin", date(2026, 10, 11)),
        ("America/Los_Angeles", date(2026, 10, 10)),
        ("Etc/GMT+8", date(2026, 10, 10)),
    ],
)
def test_yesterday_is_that_of_the_time_zone_of_the_account(time_zone, yesterday):
    assert common.last_final_day(NOW, time_zone) == yesterday
    first = common.history_first_day(NOW, time_zone)
    assert (yesterday - first).days == common.HISTORY_DAYS - 1
    assert common.age(NOW, time_zone, yesterday) == 1


def test_a_moment_without_a_time_zone_is_refused():
    with pytest.raises(ValueError, match="aware"):
        common.local_today(datetime(2026, 10, 12, 5, 0), "UTC")


def test_a_curve_rises_along_its_points_and_ends_at_the_final_value():
    curve = ((1, 600), (2, 700), (7, 850), (90, 1000))
    assert [common.share(curve, n) for n in (0, 1, 2, 3, 7, 48, 90, 400)] == [
        600,
        600,
        700,
        730,
        850,
        924,
        1000,
        1000,
    ]
    assert common.matured(12_345, 1, curve) == 7_407
    assert common.matured(12_345, 91, curve) == 12_345


def test_money_is_written_as_the_platforms_write_it():
    assert common.major(5_312) == "53.12"
    assert common.major(7, 4) == "0.0007"
    assert common.trimmed(533_950) == "5339.5"
    assert common.trimmed(5_300) == "53"
    assert common.trimmed(0) == "0"


def test_the_world_has_two_clients_two_currencies_and_four_time_zones():
    ads = [a for a in world.ACCOUNTS if a.platform != "demo_web"]
    assert len(ads) == 5
    assert len([a for a in world.ACCOUNTS if a.platform == "demo_web"]) == 2
    assert {a.client for a in world.ACCOUNTS} == {c.key for c in world.CLIENTS}
    assert {a.currency for a in world.ACCOUNTS} == {"USD", "EUR"}
    assert {a.time_zone for a in ads} == {
        "America/New_York",
        "Europe/Berlin",
        "America/Los_Angeles",
        "Etc/GMT+8",
    }
    # One client runs no account on demo_video.
    video = {a.client for a in world.ACCOUNTS if a.platform == "demo_video"}
    assert video == {"tarnwick"}
    assert world.MANAGER.client is None


def test_the_ids_of_accounts_and_campaigns_are_unique_within_their_platform():
    keys = [(a.platform, a.account_id) for a in world.ACCOUNTS]
    assert len(keys) == len(set(keys))
    first, last = date(2025, 1, 1), date(2026, 12, 31)
    campaigns = [
        (c.platform, c.campaign_id)
        for a in world.ACCOUNTS
        for c in world.campaigns_of(a.account_id, first, last)
    ]
    assert len(campaigns) == len(set(campaigns))
    names = [
        c.name_at(last)
        for a in world.ACCOUNTS
        for c in world.campaigns_of(a.account_id, last, last)
    ]
    assert len(names) == len(set(names)), (
        "two campaigns share a name, which a chart by name would merge"
    )


def test_a_flight_a_season_and_a_cycle_run_when_they_say():
    shopping = next(c for c in world.CAMPAIGNS if c.campaign_id == "21000000104")
    on = [shopping.active(world.EPOCH + timedelta(days=n)) for n in range(84)]
    assert on == [True] * 56 + [False] * 28
    holiday = next(c for c in world.CAMPAIGNS if c.campaign_id == "21000000105")
    assert holiday.active(date(2026, 11, 1))
    assert holiday.active(date(2026, 12, 31))
    assert not holiday.active(date(2026, 10, 31))
    assert not holiday.active(date(2027, 1, 1))
    tests = list(world.CYCLES[0].rounds(date(2026, 1, 1), date(2026, 12, 31)))
    assert tests, "no round of the test of demo_search in 2026"
    for campaign in tests:
        assert (campaign.end - campaign.start).days == world.CYCLES[0].life - 1
        assert campaign.removed(campaign.end + timedelta(days=1))
        assert campaign.name_at(campaign.start).startswith("Display Test #")


def test_two_campaigns_change_their_names_at_the_start_of_a_half_year_or_a_year():
    renamed = {
        c.campaign_id
        for c in world.CAMPAIGNS
        if c.names.changes(date(2025, 1, 1), date(2026, 12, 31))
    }
    assert renamed == {"21000000102", "120200000000102"}
    moments = world.name_versions("demo_search", NOW)
    first = min(
        common.history_first_day(NOW, a.time_zone) for a in world.accounts_of("demo_search")
    )
    assert moments[0].date() == first
    assert [m.date() for m in moments[1:]] == [date(2025, 7, 1), date(2026, 1, 1), date(2026, 7, 1)]
    assert all(m.tzinfo is UTC and m < NOW for m in moments)
    assert world.name_versions("demo_video", NOW) == []


def test_a_final_day_is_deterministic_and_plausible():
    campaign = world.CAMPAIGNS[0]
    day = date(2026, 10, 5)
    assert world.final(campaign, day) == world.final(campaign, day)
    final = world.final(campaign, day)
    assert 0 < final.clicks < final.impressions
    # Micros that are not even whole hundredths of a cent, as those of Google are not: clicks
    # times a price in cents times a share in percent would be.
    assert final.spend_micros % 100 != 0
    assert final.conversions_hundredths >= 0
    assert final.value_minor >= 0
    paused = next(c for c in world.CAMPAIGNS if c.campaign_id == "21000000105")
    assert world.final(paused, date(2026, 6, 1)) is None


def test_the_api_lists_the_tests_of_two_years_and_tiktok_leaves_out_the_removed_ones():
    day = date(2026, 10, 12)
    search = [c for c in world.listed_campaigns("demo_search", day) if c.start is not None]
    assert search
    assert all(c.start >= day - timedelta(days=world.ENTITY_HORIZON_DAYS) for c in search)
    assert any(c.removed(day) for c in search), "Google lists removed campaigns"
    video = world.listed_campaigns("demo_video", day)
    assert not any(c.removed(day) for c in video)


@pytest.mark.parametrize(
    ("total", "weights", "parts"),
    [
        (10, [1, 1, 1], [4, 3, 3]),
        (7, [60, 40], [4, 3]),
        (0, [5, 5], [0, 0]),
        (1, [1, 2], [0, 1]),
        (1_000_003, [75, 25], [750_002, 250_001]),
    ],
)
def test_a_number_divides_into_parts_that_add_up_to_it(total, weights, parts):
    assert world.divide(total, weights) == parts
    assert sum(world.divide(total, weights)) == total


def test_every_campaign_but_performance_max_has_ad_groups_with_ads_of_unique_ids():
    day = date(2026, 10, 12)
    for platform in ("demo_search", "demo_social", "demo_video"):
        campaigns = world.listed_campaigns(platform, day)
        groups = [g for c in campaigns for g in world.ad_groups_of(c)]
        ads = [a for g in groups for a in g.ads]
        for campaign in campaigns:
            found = world.ad_groups_of(campaign)
            assert bool(found) == (campaign.kind != "PERFORMANCE_MAX"), campaign.campaign_id
            assert all(g.campaign_id == campaign.campaign_id and g.ads for g in found)
        assert len({g.ad_group_id for g in groups}) == len(groups), platform
        assert len({a.ad_id for a in ads}) == len(ads), platform
    # The rounds of a test have the ad groups of its template, under IDs of their own.
    [cycle] = [c for c in world.CYCLES if c.template.platform == "demo_search"]
    first, second = cycle.round_of(1), cycle.round_of(2)
    assert [g.name for g in world.ad_groups_of(first)] == ["Remarketing", "Similar Interests"]
    assert {g.ad_group_id for g in world.ad_groups_of(first)}.isdisjoint(
        g.ad_group_id for g in world.ad_groups_of(second)
    )


def test_a_campaign_divides_among_its_ad_groups_and_an_ad_group_among_its_ads():
    campaign = next(c for c in world.CAMPAIGNS if c.campaign_id == "21000000102")
    day = date(2026, 10, 1)
    numbers = {"impressions": 40_000, "clicks": 700, "conversions": 1_751}
    converting = frozenset({"conversions"})
    groups = world.ad_groups_of(campaign)
    split = world.divided(numbers, converting, groups, day)
    assert [sum(part[key] for part in split) for key in numbers] == list(numbers.values())
    assert split == world.divided(numbers, converting, groups, day)
    # Boots convert better than the campaign and base layers worse, for each click.
    boots, layers = split[1], split[2]
    assert boots["conversions"] / boots["clicks"] > layers["conversions"] / layers["clicks"]
    for group, part in zip(groups, split, strict=True):
        ads = world.divided(part, converting, group.ads, day)
        assert {key: sum(ad[key] for ad in ads) for key in numbers} == part
    # The shares change a little from day to day.
    assert split != world.divided(numbers, converting, groups, day + timedelta(days=1))
