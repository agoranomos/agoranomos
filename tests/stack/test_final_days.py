"""Whether a row of a view of a fact is final, is_final (ADR-0037, C8a of #228): true when the day
on which the load that wrote the row ended, in the time zone of its account, is at least the window
of restatements of its platform after the day of the row (restatement_window_days of the seed of
the platforms), and the day is whole; false, never none, otherwise.

The first test reads the demo as the stack has it, the history of each platform and any later load,
and checks every row of the five views against the rule, computed here from loaded_at, time_zone and
the windows that agoranomos_pipelines.windows reads from the seed: demo_search with 90 days,
demo_social with 28 and demo_web with 14 have both final rows and rows that are not, and a row that
a load of the history took older than the window is final.

The second test moves the load of one day of each of the three platforms in time, as the superuser,
in a transaction that it rolls back, so that the core is as before even if the test is killed: one
minute before midnight of the day that is the window after the day, in the time zone of the
account, the rows of that day are not final in any view, and one minute after it they are, as after
a daily load that ended on that day. A row of demo_search on its newest day that the test marks
partial is not final in the view of campaigns, while its ad groups are.

The assertions print platforms, IDs, days and counts, never a secret.
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest
from agoranomos_pipelines.windows import restatement_window_days
from stack_helpers import warehouse_output

pytestmark = [pytest.mark.demo, pytest.mark.usefixtures("daily_load_paused")]

# The views of the facts of the ad platforms, with the fact of the core under each, and that of the
# web analytics, whose IDs of an account are those of a property.
AD_VIEWS = (
    "ad_stats_campaign_daily",
    "ad_stats_ad_group_daily",
    "ad_stats_ad_daily",
    "ad_conversions_campaign_daily",
)
WEB_VIEWS = ("web_sessions_daily",)
ACCOUNT = {view: "account_id" for view in AD_VIEWS} | {"web_sessions_daily": "property_id"}
PLATFORMS = {"demo_search": AD_VIEWS, "demo_social": AD_VIEWS, "demo_web": WEB_VIEWS}
# The fact of the core that each platform has every day, whose load the second test moves.
DAILY_FACT = {
    "demo_search": "core.ad_stats_campaign_daily",
    "demo_social": "core.ad_stats_campaign_daily",
    "demo_web": "core.web_sessions_daily",
}
# The rows of a view by platform, partial or not, the day of their load in the time zone of the
# account (none without a load), their day, and whether they are final: how many, and whether any
# is final none.
ROWS = (
    "SELECT platform, {partial}, "
    "(loaded_at AT TIME ZONE time_zone)::date - report_date, is_final, count(*), "
    "count(*) FILTER (WHERE is_final IS NULL) "
    "FROM marts.{view} GROUP BY 1, 2, 3, 4 ORDER BY 1, 2, 3, 4"
)
# The rows of a view of a platform on the days that a load of the history took older than the
# window when it ended: how many, and how many of them are final.
HISTORY = (
    "SELECT count(*), count(*) FILTER (WHERE v.is_final) FROM marts.{view} AS v "
    "JOIN core.dim_account AS a ON a.platform = v.platform AND a.account_id = v.{account} "
    "WHERE v.platform = '{platform}' AND EXISTS (SELECT FROM {fact} AS f "
    "JOIN core.loads AS l ON l.load_sk = f.load_sk AND l.kind = 'history' "
    "WHERE f.account_sk = a.account_sk AND f.report_date = v.report_date "
    "AND (l.finished_at AT TIME ZONE a.time_zone)::date - f.report_date >= {window})"
)
# The newest day of each platform in the fact that it has every day, of the account that comes
# first, with that account, its time zone and the load of the day.
NEWEST = (
    "SELECT a.account_id, a.time_zone, f.report_date, f.load_sk FROM {fact} AS f "
    "JOIN core.dim_account AS a ON a.account_sk = f.account_sk "
    "WHERE a.platform = '{platform}' AND NOT {partial} "
    "ORDER BY a.account_id, f.report_date DESC LIMIT 1"
)
# What the views say of the day of an account, as a line of its own among the tags of psql: the
# view, how many rows, how many final and how many final none.
DAY = (
    "SELECT '{tag}|{view}|' || count(*) || '|' || count(*) FILTER (WHERE is_final) || '|' "
    "|| count(*) FILTER (WHERE is_final IS NULL) FROM marts.{view} "
    "WHERE platform = '{platform}' AND {account} = '{account_id}' "
    "AND report_date = DATE '{day}';"
)
# The load of the day, moved to a minute from midnight in the time zone of the account.
MOVE = (
    "UPDATE core.loads SET finished_at = "
    "(DATE '{midnight}'::timestamp AT TIME ZONE '{zone}') + interval '{shift} minute' "
    "WHERE load_sk = {load_sk};"
)


def rows(env, sql):
    return [tuple(line.split("|")) for line in warehouse_output(env, sql).splitlines()]


def is_partial(view: str) -> str:
    return "is_partial_day" if view == "ad_stats_campaign_daily" else "false"


@pytest.mark.parametrize("view", [*AD_VIEWS, *WEB_VIEWS])
def test_a_row_is_final_when_its_load_ended_the_window_after_its_day(env, view):
    finals = {}
    for platform, partial, age, final, count, unknown in rows(
        env, ROWS.format(view=view, partial=is_partial(view))
    ):
        assert unknown == "0", f"{view}: {unknown} rows of {platform} with is_final none"
        window = restatement_window_days(platform)
        # Without a load the day has no age, and the row is not final.
        wanted = partial == "f" and age != "" and int(age) >= window
        assert final == ("t" if wanted else "f"), (view, platform, partial, age, final, count)
        finals.setdefault(platform, set()).add(final)
    for platform, views in PLATFORMS.items():
        if view in views:
            assert finals.get(platform) == {"t", "f"}, (view, platform, finals.get(platform))
    # A row that a load of the history took older than its window is final at once.
    for platform, views in PLATFORMS.items():
        if view not in views:
            continue
        found, final = rows(
            env,
            HISTORY.format(
                view=view,
                account=ACCOUNT[view],
                fact="core." + view,
                platform=platform,
                window=restatement_window_days(platform),
            ),
        )[0]
        assert int(found) > 0, f"{view}: no row of the history of {platform} older than its window"
        assert final == found, (view, platform, found, final)


@pytest.mark.parametrize("platform", sorted(PLATFORMS))
def test_a_row_turns_final_at_midnight_of_the_day_its_window_ends(env, platform):
    fact = DAILY_FACT[platform]
    partial = "f.is_partial_day" if fact == "core.ad_stats_campaign_daily" else "false"
    account_id, zone, day, load_sk = rows(
        env, NEWEST.format(fact=fact, platform=platform, partial=partial)
    )[0]
    midnight = date.fromisoformat(day) + timedelta(days=restatement_window_days(platform))
    views = PLATFORMS[platform]

    def day_in_views(tag: str) -> str:
        return "".join(
            DAY.format(
                tag=tag,
                view=view,
                platform=platform,
                account=ACCOUNT[view],
                account_id=account_id,
                day=day,
            )
            for view in views
        )

    # The load and the partial rows of the day before the test, which the rollback keeps.
    kept = (
        f"SELECT l.finished_at, count(*) FILTER (WHERE {partial}) FROM core.loads AS l "  # noqa: S608 (values of the demo)
        f"JOIN {fact} AS f ON f.load_sk = l.load_sk AND f.report_date = DATE '{day}' "
        f"WHERE l.load_sk = {load_sk} GROUP BY 1"
    )
    before = warehouse_output(env, kept)
    script = ["BEGIN ISOLATION LEVEL REPEATABLE READ;"]
    script.append(MOVE.format(midnight=midnight, zone=zone, shift=-1, load_sk=load_sk))
    script.append(day_in_views("before"))
    script.append(MOVE.format(midnight=midnight, zone=zone, shift=1, load_sk=load_sk))
    script.append(day_in_views("after"))
    if platform == "demo_search":
        # A partial day is never final: the campaigns of the day, marked partial, are not, and
        # their ad groups, which have no partial day, are.
        script.append(
            "UPDATE core.ad_stats_campaign_daily SET is_partial_day = true "  # noqa: S608 (values of the demo)
            f"WHERE load_sk = {load_sk} AND report_date = DATE '{day}';"
        )
        script.append(day_in_views("partial"))
    script.append("ROLLBACK;")
    found = {}
    for line in warehouse_output(env, "\n".join(script)).splitlines():
        if line.count("|") == 4:
            tag, view, count, final, unknown = line.split("|")
            found[tag, view] = (int(count), int(final), int(unknown))
    assert warehouse_output(env, kept) == before, f"the rollback changed the load {load_sk}"
    assert found[("before", views[0])][0] > 0, f"{platform} has no row of {account_id} on {day}"
    for view in views:
        count, final, unknown = found[("before", view)]
        assert (final, unknown) == (0, 0), (platform, view, day, "a minute before", found)
        count, final, unknown = found[("after", view)]
        assert (final, unknown) == (count, 0), (platform, view, day, "a minute after", found)
        if platform == "demo_search":
            count, final, unknown = found[("partial", view)]
            wanted = 0 if view == "ad_stats_campaign_daily" else count
            assert (final, unknown) == (wanted, 0), (platform, view, day, "partial", found)
