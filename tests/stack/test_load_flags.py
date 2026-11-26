"""The flags of the answers of a load (ADR-0054, C8b of #228): the journal of demo_web, in the shape
of GA4, holds the four flags of each property in each load, and the pin copies them into
core.loads, from which marts.web_sessions_daily gives each row the flags of the load that wrote it;
ops.ops_loads shows those of the last whole load of each property.

The tests read the demo as the stack has it, the history of each platform and any later load, and
run no load and no transform. demo_web flags one property, web.FLAGGED_PROPERTY, with web.FLAGGED,
on each load whose window holds the day web.FLAGGED_DAYS_AGO before its yesterday, so on the
history: that load carries the flag in core.loads and in each of its rows of the view, and every
other load and row has none of the four flags true. The rows of the ad platforms and of the rates in
core.loads have the four flags none, as their journals have no such column.

The stack is one that this release started, as in CI: a pin before the migration 0012 left the four
flags none in core.loads, on a stack that a release before it started.

The assertions print platforms, IDs, days, flags and counts, never a secret.
"""

from __future__ import annotations

import pytest
from agoranomos_pipelines.demo import web
from stack_helpers import warehouse_output

pytestmark = [pytest.mark.demo, pytest.mark.usefixtures("daily_load_paused")]

FLAGS = (
    "is_subject_to_thresholding",
    "has_data_loss_from_other_row",
    "is_sampled",
    "is_truncated",
)
# Each flag as t, f or an empty text for none, joined.
FLAG_TEXT = " || ".join(f"coalesce({{alias}}{flag}::text, '')" for flag in FLAGS)
# The loads of demo_web in core.loads that loaded a window: the property, whether the window holds
# the day of the flag, which is the given days before window_to, the yesterday of the property at
# the load, and the flags.
WEB_LOADS = (
    "SELECT account_id || '|' || (window_from <= window_to - {days})::text || '|' || "  # noqa: S608 (constants of this file)
    + FLAG_TEXT.format(alias="")
    + " FROM core.loads WHERE source = 'demo_web' AND outcome = 'loaded' "
    "ORDER BY load_sk"
)
# The rows of core.loads of the other sources with any flag that is not none, by source.
OTHER_FLAGS = (
    "SELECT source || '|' || count(*) FROM core.loads WHERE source NOT IN ('demo_web', 'ga4') "  # noqa: S608 (constants of this file)
    "AND ("
    + " OR ".join(f"{flag} IS NOT NULL" for flag in FLAGS)
    + ") GROUP BY source ORDER BY source"
)
# The rows of the view of sessions against their facts and loads in the core: how many, how many
# whose flags differ from those of their load, how many with the flag of the demo on the property
# of the flag and on the others, and how many with a flag none or another flag true.
VIEW = (
    "SELECT count(*) || '|' || "  # noqa: S608 (constants of this file)
    "count(*) FILTER (WHERE (" + ", ".join(f"v.{flag}" for flag in FLAGS) + ") "
    "IS DISTINCT FROM (" + ", ".join(f"l.{flag}" for flag in FLAGS) + ")) || '|' || "
    "count(*) FILTER (WHERE v.property_id = '{prop}' AND v.{flagged}) || '|' || "
    "count(*) FILTER (WHERE v.property_id <> '{prop}' AND v.{flagged}) || '|' || "
    "count(*) FILTER (WHERE " + " OR ".join(f"v.{flag} IS NULL" for flag in FLAGS) + ") || '|' || "
    "count(*) FILTER (WHERE "
    + " OR ".join(f"v.{flag}" for flag in FLAGS if flag != web.FLAGGED)
    + ") "
    "FROM marts.web_sessions_daily AS v "
    "JOIN core.dim_account AS a ON a.platform = v.platform AND a.account_id = v.property_id "
    "JOIN core.web_sessions_daily AS f ON f.account_sk = a.account_sk "
    "AND f.report_date = v.report_date AND f.session_source = v.session_source "
    "AND f.session_medium = v.session_medium AND f.session_campaign_name = v.session_campaign_name "
    "AND f.session_campaign_id = v.session_campaign_id "
    "AND f.session_default_channel_group = v.session_default_channel_group "
    "JOIN core.loads AS l ON l.load_sk = f.load_sk "
    "WHERE v.platform = 'demo_web'"
)
# The rows of the facts of the property of the flag whose load has the flag.
FLAGGED_ROWS = (
    "SELECT count(*) FROM core.web_sessions_daily AS f "
    "JOIN core.dim_account AS a ON a.account_sk = f.account_sk "
    "JOIN core.loads AS l ON l.load_sk = f.load_sk "
    "WHERE a.platform = 'demo_web' AND a.account_id = '{prop}' AND l.{flagged}"
)
# The flags of ops_loads by platform and account, and those of the last whole load of each
# property in the journal of demo_web.
OPS = (
    "SELECT platform || '|' || account_id || '|' || "  # noqa: S608 (constants of this file)
    + FLAG_TEXT.format(alias="")
    + " FROM ops.ops_loads ORDER BY 1"
)
JOURNAL = (
    "SELECT DISTINCT ON (account_id) account_id || '|' || "  # noqa: S608 (constants of this file)
    + FLAG_TEXT.format(alias="")
    + " FROM raw_demo_web._agoranomos_loads "
    "WHERE status = 'loaded' AND kind IS DISTINCT FROM 'intraday' "
    "ORDER BY account_id, loaded_at DESC"
)


def lines(env, sql):
    return [line.split("|") for line in warehouse_output(env, sql).splitlines()]


def flags_text(*values: bool) -> str:
    return "".join("true" if value else "false" for value in values)


def test_the_demo_flags_its_property_on_the_loads_that_hold_the_day_in_core_loads(env):
    found = lines(env, WEB_LOADS.format(days=web.FLAGGED_DAYS_AGO))
    assert found, "core.loads holds no loaded window of demo_web"
    flagged = 0
    for account_id, holds, text in found:
        wanted = account_id == web.FLAGGED_PROPERTY and holds == "true"
        flagged += wanted
        assert text == flags_text(*(flag == web.FLAGGED and wanted for flag in FLAGS)), (
            account_id,
            holds,
            text,
        )
    # The history of the property holds the day.
    assert flagged >= 1, f"no load of {web.FLAGGED_PROPERTY} holds the day of the flag"


def test_the_loads_of_the_other_sources_have_no_flag(env):
    assert warehouse_output(env, OTHER_FLAGS) == ""


def test_each_row_of_the_view_of_sessions_carries_the_flags_of_its_load(env):
    total, differ, on_property, elsewhere, unknown, others = lines(
        env, VIEW.format(prop=web.FLAGGED_PROPERTY, flagged=web.FLAGGED)
    )[0]
    assert int(total) > 0, "marts.web_sessions_daily holds no row of demo_web"
    assert differ == "0", f"{differ} rows of the view have flags other than their load"
    assert unknown == "0", f"{unknown} rows of the view have a flag none"
    assert (elsewhere, others) == ("0", "0"), (elsewhere, others)
    wanted = warehouse_output(
        env, FLAGGED_ROWS.format(prop=web.FLAGGED_PROPERTY, flagged=web.FLAGGED)
    )
    assert int(wanted) > 0, f"no row of {web.FLAGGED_PROPERTY} has the flag in the core"
    assert on_property == wanted, (on_property, wanted)


def test_ops_loads_shows_the_flags_of_the_last_whole_load_of_each_property(env):
    shown = {(platform, account_id): text for platform, account_id, text in lines(env, OPS)}
    web_rows = {key: text for key, text in shown.items() if key[0] == "demo_web"}
    assert web_rows == {("demo_web", account_id): text for account_id, text in lines(env, JOURNAL)}
    assert all(text for text in web_rows.values()), web_rows
    # The ad platforms and the rates have no flag; GA4 loads no property in the stack tests.
    assert {
        key: text for key, text in shown.items() if key[0] not in ("demo_web", "ga4") and text
    } == {}
