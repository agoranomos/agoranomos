"""The demo of Agoranomos, made ready by the one-shot demo-init service on every start of the demo
profile.

It runs the demo pipeline once: agoranomos_demo_history, which loads the history of the fictional
platforms of the demo generator into raw_demo_<platform>, and then agoranomos_demo_transform, which
the five assets of the history start. Then it unpauses agoranomos_demo_daily, which brings the new
days every day, if that DAG has never run. Then it makes the demo visible in Metabase: it syncs the
schema of the warehouse, applies the metadata of the marts (metadata.py), as metabase-init does,
and puts the demo, its questions and its two dashboards, Advertising overview and Web analytics,
in the collection Agoranomos / Demo as the code says (content.py, ADR-0018): an edit is reverted
and logged, an item in the trash comes back, and every group but Administrators gets at most View
on them. The questions get the titles of their columns and axes from the display names of the
metadata, money in the reporting currency of the metadata, and the colors of the project; each
has the filters of its dashboard, the period, the client and, for the advertising, the platform,
which the filters of the dashboard set. The demo collection of v0.1, the one whose dashboard the
account of demo-init made, moves into Agoranomos as Demo once, after every item in it that the
demo does not ship has gone to the root, and its dashboard and questions become those of the
advertising.

Each step looks at the current state first. A start that finds a successful run of the history and
a successful run of the transform after it does not run the pipeline again and leaves the pause
state of both DAGs as it is, and a start that finds a run of the daily load leaves its pause state
as it is too. The content of Metabase is found by the entity IDs that Metabase gave it, in the
registry of the content, never by name, but for the one move of the collection of an earlier
release.

The whole job has one deadline, DEADLINE seconds from its start, which every wait shares, and a
step that runs out of time names itself; ./agoranomos up waits longer than that for the stack.

It runs in a plain Python image and uses the client of metabase_init.py. Like metabase-init, it
takes every secret from the environment, sends it only in the body of a request, and ends a failure
with one line that names a step and an HTTP status, or the class of an error, never what a server
answered.
"""

from __future__ import annotations

import copy
import os
import sys
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, urlencode

import advertising
import content
import metadata
import metrics
from metabase_init import (
    SHAPE_ERRORS,
    ApiClient,
    Metabase,
    SetupError,
    find_warehouse,
    items_of,
    list_databases,
    logged,
    read_settings,
    sign_in,
    unexpected_shape,
)

LOAD_DAG = "agoranomos_demo_history"
TRANSFORM_DAG = "agoranomos_demo_transform"
DAILY_DAG = "agoranomos_demo_daily"
# The transform runs when each platform of the demo has loaded since its last run: it waits for
# one asset of each (airflow/dags/agoranomos_pipelines/assets.py), and the events of some of them
# start no run.
TRANSFORM_ASSETS = 5
# The demo as a package of the content (content.py): its key in the registry, and its name, the
# name of its collection in Agoranomos.
DEMO_KEY = "demo"
DEMO_NAME = "Demo"
DEMO_DESCRIPTION = (
    "The demo of Agoranomos: a fictional business, as the demo data of this release shows it."
)
# The two dashboards of the demo, by their keys in the package: their names and descriptions.
ADVERTISING = "advertising"
WEB = "web-analytics"
DASHBOARDS = {
    ADVERTISING: (
        "Advertising overview (demo)",
        "The advertising statistics of the demo, in the reporting currency, from "
        "marts.ad_stats_campaign_daily and marts.ad_conversions_campaign_daily.",
    ),
    WEB: (
        "Web analytics (demo)",
        "The sessions of web analytics of the demo, from marts.web_sessions_daily.",
    ),
}
# The names of the demo of v0.1, which the jobs look for once, to move it into Agoranomos: its
# collection at the root, its dashboard, and its questions, with the key of each in the package,
# which puts them right as the code says. Written out, so that they stay as v0.1 named them when
# the questions of the code change.
COLLECTION_V01 = "Agoranomos demo"
DASHBOARD_V01 = "Ad spend (demo)"
V01_KEYS = {
    "Spend, last 7 days": "spend",
    "Spend by campaign, last 7 days": "top-campaigns",
    "Spend by day": "spend-by-day",
}
MART_SCHEMA = "marts"
AD_STATS = "ad_stats_campaign_daily"
CONVERSIONS = "ad_conversions_campaign_daily"
SESSIONS = "web_sessions_daily"
# The one deadline of the job, in seconds from its start, shared by the waits for the DAGs, the
# history, the transform and the sync. ./agoranomos up waits 900 seconds for the whole stack.
DEADLINE = 600.0
# How long to wait between two looks at Airflow or Metabase, in seconds.
POLL_INTERVAL = 5.0
# The clock and the pause of the waits; the tests replace both with a fake clock.
clock = time.monotonic
pause = time.sleep
FINISHED = ("success", "failed")
AIRFLOW_ADMIN_HINT = (
    "Check AIRFLOW_ADMIN_PASSWORD in .env. If you changed the password of admin in Airflow, put "
    "the same value in .env."
)
# Where to look when Metabase does not show a table of the marts. When the warehouse has it and no
# sync shows it, Metabase is the suspect. When the warehouse does not have it and the pipeline ran
# before and not now, Airflow has kept its runs, and the warehouse has lost the table since, as
# after a reset of the warehouse volume alone. When the pipeline ran now, its transform is.
SYNC_HINT = "see ./agoranomos logs metabase"
RAN_BEFORE_HINT = (
    "the demo pipeline ran before, so the warehouse has lost them since, as after a reset of its "
    f"volume: trigger {LOAD_DAG} in Airflow, and the transform that follows it builds them again"
)
RAN_NOW_HINT = f"see the last run of {TRANSFORM_DAG} in Airflow"


# The filters of the questions, by the names of their template tags: their display names, the type
# of their widget, and the column of the mart that each filters on. A dashboard filter of the same
# slug sets each of them. The period starts as the past 30 days, which end yesterday.
FILTERS = {
    "period": ("Period", "date/all-options", "report_date"),
    "client": ("Client", "string/=", "client_name"),
    "platform": ("Platform", "string/=", "platform_name"),
}
DASHBOARD_FILTERS = {
    "period": content.DashboardFilter("period", "Period", "date/all-options", "date", "past30days"),
    "client": content.DashboardFilter("client", "Client", "string/=", "string"),
    "platform": content.DashboardFilter("platform", "Platform", "string/=", "string"),
}
AD_FILTERS = ("period", "client", "platform")
WEB_FILTERS = ("period", "client")

# The types that Metabase 0.64.1 gives the columns of the answers of the questions, read on the
# stand in October 2026: the contracts of the marts fix the types of the columns that the questions
# return as they are, and every sum or ratio is numeric in PostgreSQL.
TEXT = ("type/Text", "text")
NUMBER = ("type/Decimal", "numeric")
COLUMN_TYPES = {
    "report_date": ("type/Date", "date"),
    "is_primary": ("type/Boolean", "bool"),
    **dict.fromkeys(
        (
            "platform_name",
            "client_name",
            "campaign_name",
            "conversion_action_name",
            "conversion_category",
            "session_default_channel_group",
            "session_source",
            "session_medium",
            "session_campaign_name",
        ),
        TEXT,
    ),
}
# The semantic types that Metabase 0.64.1 gives the columns of the answers by their names, each
# time that it computes their metadata (src/metabase/analyze/classifiers/name.clj at the tag
# v0.64.1; seen on the stand in October 2026): a decimal whose name ends in rate is a share, one
# whose name has cost is a cost, and a name with source or channel is a source.
SEMANTIC_TYPES = {
    "click_through_rate": "type/Share",
    "engagement_rate": "type/Share",
    "cost_per_conversion": "type/Cost",
    "session_source": "type/Source",
    "session_default_channel_group": "type/Source",
}
# The columns of the answers that are no column of a mart, with their titles: the ratios.
RATIO_TITLES = {
    "click_through_rate": "CTR",
    "cost_per_conversion": "Cost per Conversion",
    "roas": "ROAS",
    "engagement_rate": "Engagement Rate",
}
# How the numbers of a column are shown: money in the reporting currency, a share as a percentage,
# and a ratio or a count with this many decimals.
MONEY = frozenset(
    {
        "spend_reporting",
        "conversion_value_reporting",
        "total_revenue_reporting",
        "cost_per_conversion",
    }
)
PERCENT = frozenset({"click_through_rate", "engagement_rate"})
DECIMALS = {"roas": 2, "conversions": 0, "key_events": 0}


def where(*filters: str) -> str:
    """The WHERE clause of a question: the template tag of each filter, which Metabase replaces."""
    return "WHERE " + " AND ".join(f"{{{{{name}}}}}" for name in filters)


AD_WHERE = where(*AD_FILTERS)
WEB_WHERE = where(*WEB_FILTERS)
# The ratios, as SQL over the sums of the rows of the answer, each null where its divisor is 0.
CTR = "sum(clicks)::numeric / nullif(sum(impressions), 0) AS click_through_rate"
CPA = "sum(spend_reporting) / nullif(sum(conversions), 0) AS cost_per_conversion"
ROAS = "sum(conversion_value_reporting) / nullif(sum(spend_reporting), 0) AS roas"
ENGAGEMENT = "sum(engaged_sessions)::numeric / nullif(sum(sessions), 0) AS engagement_rate"


def sql(*lines: str) -> str:
    return "\n".join(lines)


# The questions of the demo, by dashboard and in the order of its cards, with their place on its
# grid of 24 columns. key is the key of a question in the package, which the registry of the content
# keeps; mart is the mart that it reads, whose display names give the titles of its columns; and
# columns are the columns of the answer, in their order. Each question has the filters of its
# dashboard, and its descriptions say what it counts, as facts for people and AI clients.
QUESTIONS: tuple[dict[str, Any], ...] = (
    {
        "dashboard": ADVERTISING,
        "key": "spend",
        "name": "Spend",
        "description": "The spend of the ad accounts, in the reporting currency.",
        "display": "scalar",
        "mart": AD_STATS,
        "columns": ("spend_reporting",),
        "query": sql(
            "SELECT sum(spend_reporting) AS spend_reporting",
            f"FROM {MART_SCHEMA}.{AD_STATS}",
            AD_WHERE,
        ),
        "layout": {"row": 0, "col": 0, "size_x": 6, "size_y": 3},
    },
    {
        "dashboard": ADVERTISING,
        "key": "conversions",
        "name": "Conversions",
        "description": "The conversions of the primary conversion actions of the ad accounts.",
        "display": "scalar",
        "mart": AD_STATS,
        "columns": ("conversions",),
        "query": sql(
            "SELECT sum(conversions) AS conversions", f"FROM {MART_SCHEMA}.{AD_STATS}", AD_WHERE
        ),
        "layout": {"row": 0, "col": 6, "size_x": 6, "size_y": 3},
    },
    {
        "dashboard": ADVERTISING,
        "key": "cost-per-conversion",
        "name": "Cost per conversion",
        "description": (
            "The spend divided by the conversions of the primary conversion actions, in the "
            "reporting currency."
        ),
        "display": "scalar",
        "mart": AD_STATS,
        "columns": ("cost_per_conversion",),
        "query": sql(f"SELECT {CPA}", f"FROM {MART_SCHEMA}.{AD_STATS}", AD_WHERE),
        "layout": {"row": 0, "col": 12, "size_x": 6, "size_y": 3},
    },
    {
        "dashboard": ADVERTISING,
        "key": "roas",
        "name": "Return on ad spend",
        "description": (
            "The conversion value of the primary conversion actions divided by the spend, both in "
            "the reporting currency."
        ),
        "display": "scalar",
        "mart": AD_STATS,
        "columns": ("roas",),
        "query": sql(f"SELECT {ROAS}", f"FROM {MART_SCHEMA}.{AD_STATS}", AD_WHERE),
        "layout": {"row": 0, "col": 18, "size_x": 6, "size_y": 3},
    },
    {
        "dashboard": ADVERTISING,
        "key": "spend-by-day",
        "name": "Spend by day and platform",
        "description": "The spend of each day on each platform, in the reporting currency.",
        "display": "area",
        "mart": AD_STATS,
        "columns": ("report_date", "platform_name", "spend_reporting"),
        "visualization_settings": {
            "graph.dimensions": ["report_date", "platform_name"],
            "graph.metrics": ["spend_reporting"],
            "stackable.stack_type": "stacked",
        },
        "query": sql(
            "SELECT report_date, platform_name, sum(spend_reporting) AS spend_reporting",
            f"FROM {MART_SCHEMA}.{AD_STATS}",
            AD_WHERE,
            "GROUP BY report_date, platform_name",
            "ORDER BY report_date, platform_name",
        ),
        "layout": {"row": 3, "col": 0, "size_x": 16, "size_y": 6},
    },
    {
        "dashboard": ADVERTISING,
        "key": "by-client",
        "name": "Spend by client",
        "description": "The spend of the ad accounts of each client, in the reporting currency.",
        "display": "row",
        "mart": AD_STATS,
        "columns": ("client_name", "spend_reporting"),
        "visualization_settings": {
            "graph.dimensions": ["client_name"],
            "graph.metrics": ["spend_reporting"],
        },
        "query": sql(
            "SELECT client_name, sum(spend_reporting) AS spend_reporting",
            f"FROM {MART_SCHEMA}.{AD_STATS}",
            AD_WHERE,
            "GROUP BY client_name",
            "ORDER BY spend_reporting DESC",
        ),
        "layout": {"row": 3, "col": 16, "size_x": 8, "size_y": 6},
    },
    {
        "dashboard": ADVERTISING,
        "key": "by-platform",
        "name": "Results by platform",
        "description": (
            "The impressions, clicks, spend and conversions of each platform, with the ratios "
            "between them: the click-through rate, the cost per conversion and the return on ad "
            "spend. Money is in the reporting currency, and the conversions are those of the "
            "primary conversion actions."
        ),
        "display": "table",
        "mart": AD_STATS,
        "columns": (
            "platform_name",
            "impressions",
            "clicks",
            "click_through_rate",
            "spend_reporting",
            "conversions",
            "cost_per_conversion",
            "roas",
        ),
        "query": sql(
            "SELECT platform_name, sum(impressions) AS impressions, sum(clicks) AS clicks,",
            f"  {CTR}, sum(spend_reporting) AS spend_reporting,",
            f"  sum(conversions) AS conversions, {CPA}, {ROAS}",
            f"FROM {MART_SCHEMA}.{AD_STATS}",
            AD_WHERE,
            "GROUP BY platform_name",
            "ORDER BY spend_reporting DESC",
        ),
        "layout": {"row": 9, "col": 0, "size_x": 24, "size_y": 5},
    },
    {
        "dashboard": ADVERTISING,
        "key": "top-campaigns",
        "name": "Top campaigns by spend",
        "description": (
            "The 20 campaigns with the most spend, with their platform and client, conversions of "
            "the primary conversion actions, and money in the reporting currency."
        ),
        "display": "table",
        "mart": AD_STATS,
        "columns": (
            "campaign_name",
            "platform_name",
            "client_name",
            "spend_reporting",
            "conversions",
            "cost_per_conversion",
            "roas",
        ),
        "query": sql(
            "SELECT campaign_name, platform_name, client_name,",
            "  sum(spend_reporting) AS spend_reporting, sum(conversions) AS conversions,",
            f"  {CPA}, {ROAS}",
            f"FROM {MART_SCHEMA}.{AD_STATS}",
            AD_WHERE,
            "GROUP BY platform, account_id, campaign_id, campaign_name, platform_name, client_name",
            "ORDER BY spend_reporting DESC",
            "LIMIT 20",
        ),
        "layout": {"row": 14, "col": 0, "size_x": 24, "size_y": 8},
    },
    {
        "dashboard": ADVERTISING,
        "key": "conversions-by-action",
        "name": "Conversions by action",
        "description": (
            "The conversions and the conversion value of each conversion action of each platform, "
            "with its category and whether it is primary, the value in the reporting currency."
        ),
        "display": "table",
        "mart": CONVERSIONS,
        "columns": (
            "platform_name",
            "conversion_action_name",
            "conversion_category",
            "is_primary",
            "conversions",
            "conversion_value_reporting",
        ),
        "query": sql(
            "SELECT platform_name, conversion_action_name, conversion_category, is_primary,",
            "  sum(conversions) AS conversions,",
            "  sum(conversion_value_reporting) AS conversion_value_reporting",
            f"FROM {MART_SCHEMA}.{CONVERSIONS}",
            AD_WHERE,
            "GROUP BY platform, conversion_action_id, platform_name, conversion_action_name,",
            "  conversion_category, is_primary",
            "ORDER BY is_primary DESC, conversions DESC",
        ),
        "layout": {"row": 22, "col": 0, "size_x": 24, "size_y": 7},
    },
    {
        "dashboard": WEB,
        "key": "sessions",
        "name": "Sessions",
        "description": "The sessions of the properties of web analytics.",
        "display": "scalar",
        "mart": SESSIONS,
        "columns": ("sessions",),
        "query": sql(
            "SELECT sum(sessions) AS sessions", f"FROM {MART_SCHEMA}.{SESSIONS}", WEB_WHERE
        ),
        "layout": {"row": 0, "col": 0, "size_x": 6, "size_y": 3},
    },
    {
        "dashboard": WEB,
        "key": "engagement-rate",
        "name": "Engagement rate",
        "description": "The engaged sessions divided by the sessions.",
        "display": "scalar",
        "mart": SESSIONS,
        "columns": ("engagement_rate",),
        "query": sql(f"SELECT {ENGAGEMENT}", f"FROM {MART_SCHEMA}.{SESSIONS}", WEB_WHERE),
        "layout": {"row": 0, "col": 6, "size_x": 6, "size_y": 3},
    },
    {
        "dashboard": WEB,
        "key": "key-events",
        "name": "Key events",
        "description": "The key events of the sessions.",
        "display": "scalar",
        "mart": SESSIONS,
        "columns": ("key_events",),
        "query": sql(
            "SELECT sum(key_events) AS key_events", f"FROM {MART_SCHEMA}.{SESSIONS}", WEB_WHERE
        ),
        "layout": {"row": 0, "col": 12, "size_x": 6, "size_y": 3},
    },
    {
        "dashboard": WEB,
        "key": "revenue",
        "name": "Revenue",
        "description": "The revenue of the sessions, in the reporting currency.",
        "display": "scalar",
        "mart": SESSIONS,
        "columns": ("total_revenue_reporting",),
        "query": sql(
            "SELECT sum(total_revenue_reporting) AS total_revenue_reporting",
            f"FROM {MART_SCHEMA}.{SESSIONS}",
            WEB_WHERE,
        ),
        "layout": {"row": 0, "col": 18, "size_x": 6, "size_y": 3},
    },
    {
        "dashboard": WEB,
        "key": "sessions-by-day",
        "name": "Sessions by day and channel group",
        "description": "The sessions of each day in each default channel group of web analytics.",
        "display": "area",
        "mart": SESSIONS,
        "columns": ("report_date", "session_default_channel_group", "sessions"),
        "visualization_settings": {
            "graph.dimensions": ["report_date", "session_default_channel_group"],
            "graph.metrics": ["sessions"],
            "stackable.stack_type": "stacked",
        },
        "query": sql(
            "SELECT report_date, session_default_channel_group, sum(sessions) AS sessions",
            f"FROM {MART_SCHEMA}.{SESSIONS}",
            WEB_WHERE,
            "GROUP BY report_date, session_default_channel_group",
            "ORDER BY report_date, session_default_channel_group",
        ),
        "layout": {"row": 3, "col": 0, "size_x": 24, "size_y": 6},
    },
    {
        "dashboard": WEB,
        "key": "by-channel-group",
        "name": "Results by channel group",
        "description": (
            "The sessions, engaged sessions, key events and revenue of each default channel "
            "group, with the engagement rate; revenue in the reporting currency."
        ),
        "display": "table",
        "mart": SESSIONS,
        "columns": (
            "session_default_channel_group",
            "sessions",
            "engaged_sessions",
            "engagement_rate",
            "key_events",
            "total_revenue_reporting",
        ),
        "query": sql(
            "SELECT session_default_channel_group, sum(sessions) AS sessions,",
            f"  sum(engaged_sessions) AS engaged_sessions, {ENGAGEMENT},",
            "  sum(key_events) AS key_events,",
            "  sum(total_revenue_reporting) AS total_revenue_reporting",
            f"FROM {MART_SCHEMA}.{SESSIONS}",
            WEB_WHERE,
            "GROUP BY session_default_channel_group",
            "ORDER BY sessions DESC",
        ),
        "layout": {"row": 9, "col": 0, "size_x": 24, "size_y": 7},
    },
    {
        "dashboard": WEB,
        "key": "by-source",
        "name": "Top sources and mediums",
        "description": (
            "The 20 pairs of a source and a medium with the most sessions, with their key events "
            "and revenue, in the reporting currency."
        ),
        "display": "table",
        "mart": SESSIONS,
        "columns": (
            "session_source",
            "session_medium",
            "sessions",
            "key_events",
            "total_revenue_reporting",
        ),
        "query": sql(
            "SELECT session_source, session_medium, sum(sessions) AS sessions,",
            "  sum(key_events) AS key_events,",
            "  sum(total_revenue_reporting) AS total_revenue_reporting",
            f"FROM {MART_SCHEMA}.{SESSIONS}",
            WEB_WHERE,
            "GROUP BY session_source, session_medium",
            "ORDER BY sessions DESC",
            "LIMIT 20",
        ),
        "layout": {"row": 16, "col": 0, "size_x": 24, "size_y": 7},
    },
    {
        "dashboard": WEB,
        "key": "ad-campaigns",
        "name": "Sessions of ad campaigns",
        "description": (
            "The sessions, key events and revenue of each campaign that web analytics names, of "
            "the sessions that it gives an ad platform, with revenue in the reporting currency."
        ),
        "display": "table",
        "mart": SESSIONS,
        "columns": (
            "session_campaign_name",
            "session_source",
            "session_medium",
            "sessions",
            "key_events",
            "total_revenue_reporting",
        ),
        "query": sql(
            "SELECT session_campaign_name, session_source, session_medium,",
            "  sum(sessions) AS sessions, sum(key_events) AS key_events,",
            "  sum(total_revenue_reporting) AS total_revenue_reporting",
            f"FROM {MART_SCHEMA}.{SESSIONS}",
            f"{WEB_WHERE} AND ad_platform IS NOT NULL",
            "GROUP BY session_campaign_id, session_campaign_name, session_source, session_medium",
            "ORDER BY sessions DESC",
        ),
        "layout": {"row": 23, "col": 0, "size_x": 24, "size_y": 7},
    },
)
# The filters of the questions of each dashboard.
QUESTION_FILTERS = {ADVERTISING: AD_FILTERS, WEB: WEB_FILTERS}

ENV_NAMES = {
    "airflow_url": "AIRFLOW_URL",
    "airflow_password": "AIRFLOW_ADMIN_PASSWORD",
    "metabase_url": "METABASE_URL",
    "admin_email": "METABASE_ADMIN_EMAIL",
    "admin_password": "METABASE_ADMIN_PASSWORD",
    "warehouse_host": "WAREHOUSE_HOST",
    "warehouse_port": "WAREHOUSE_PORT",
    "warehouse_db": "WAREHOUSE_DB",
    "warehouse_user": "WAREHOUSE_USER",
    "metadata_export": "METADATA_EXPORT",
    "metadata_overrides": "METADATA_OVERRIDES",
    "content_registry": "CONTENT_REGISTRY",
}


@dataclass(frozen=True)
class Config:
    airflow_url: str
    airflow_password: str
    metabase_url: str
    admin_email: str
    admin_password: str
    warehouse_host: str
    warehouse_port: int
    warehouse_db: str
    warehouse_user: str
    metadata_export: str
    metadata_overrides: str
    content_registry: str

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Config:
        return cls(**read_settings(env, ENV_NAMES, urls=("airflow_url", "metabase_url")))


class Deadline:
    """The one deadline of the job: its length in seconds, and when on clock() it is over."""

    def __init__(self, seconds: float) -> None:
        self.seconds = seconds
        self.at = clock() + seconds

    def passed(self) -> bool:
        return clock() >= self.at


def wait_until[T](check: Callable[[], T], deadline: Deadline, what: str, hint: str) -> T:
    """Look with check every POLL_INTERVAL seconds until it gives a true result, and return it. A
    step that is not done by the deadline stops the job with a line that names the step (what)
    and says where to look (hint)."""
    while True:
        result = check()
        if result:
            return result
        if deadline.passed():
            raise SetupError(
                f"{what} within {deadline.seconds:g} seconds of the start of demo-init: {hint}"
            )
        pause(POLL_INTERVAL)


class Airflow(ApiClient):
    """The REST API of Airflow, signed in with a bearer token."""

    logs_hint = "See ./agoranomos logs airflow-apiserver."

    def __init__(self, url: str, timeout: float = 30.0) -> None:
        super().__init__(url, timeout)
        self.token: str | None = None

    def auth_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"} if self.token else {}


def sign_in_to_airflow(airflow: Airflow, password: str) -> None:
    """Sign in as the admin that airflow-init creates (user name admin)."""
    payload = airflow.call(
        "POST",
        "/auth/token",
        {"username": "admin", "password": password},
        hints={401: AIRFLOW_ADMIN_HINT, 403: AIRFLOW_ADMIN_HINT},
    )
    if not isinstance(payload, dict) or not payload.get("access_token"):
        raise SetupError(f"POST /auth/token gave no token. {airflow.logs_hint}")
    airflow.token = payload["access_token"]


def runs_path(dag_id: str, states: tuple[str, ...], since: str | None) -> str:
    """The query of the runs of a DAG in the states (any state when none is given) whose run_after
    is not before since, the newest first, one to a page. Airflow keeps every run, also after
    ./agoranomos down, and its API sorts the oldest first and gives 50 unless it is told otherwise:
    so the query says both, and a count comes from total_entries, never from a page."""
    query = [("order_by", "-run_after"), ("limit", "1"), *(("state", state) for state in states)]
    if since is not None:
        query.append(("run_after_gte", since))
    return f"/api/v2/dags/{dag_id}/dagRuns?{urlencode(query)}"


def count_runs(airflow: Airflow, dag_id: str, *states: str, since: str | None = None) -> int:
    return airflow.call("GET", runs_path(dag_id, states, since))["total_entries"]


def newest_run(airflow: Airflow, dag_id: str, *states: str) -> dict[str, Any] | None:
    page = airflow.call("GET", runs_path(dag_id, states, None))["dag_runs"]
    return page[0] if page else None


def transform_is_busy(airflow: Airflow) -> bool:
    """Whether the transform has a run queued or running, or a run that Airflow is about to
    create: an event queued for each of its assets. The events of some of its assets, as a history
    that failed half way leaves them, start no run until the others come, so they do not count.
    The list of queued events has no pages: it holds one event at most for each asset."""
    if count_runs(airflow, TRANSFORM_DAG, "queued", "running"):
        return True
    queued = airflow.call("GET", f"/api/v2/dags/{TRANSFORM_DAG}/assets/queuedEvents")
    return queued["total_entries"] >= TRANSFORM_ASSETS


def wait_for_dags(airflow: Airflow, deadline: Deadline) -> None:
    """Wait until the DAG processor has loaded the demo DAGs; until then Airflow answers 404."""
    for dag_id in (LOAD_DAG, TRANSFORM_DAG, DAILY_DAG):
        wait_until(
            lambda dag_id=dag_id: airflow.call("GET", f"/api/v2/dags/{dag_id}", missing_ok=True),
            deadline,
            f"Airflow did not load the DAG {dag_id}",
            "see ./agoranomos logs airflow-dag-processor",
        )


def wait_for_run(airflow: Airflow, dag_id: str, run_id: str, deadline: Deadline) -> dict[str, Any]:
    """Wait until a run of a DAG has finished, and return it."""

    def finished() -> dict[str, Any] | None:
        run = airflow.call("GET", f"/api/v2/dags/{dag_id}/dagRuns/{quote(run_id)}")
        return run if run["state"] in FINISHED else None

    return wait_until(finished, deadline, f"the DAG {dag_id} did not finish", "see it in Airflow")


def wait_for_transform(airflow: Airflow, since: str, deadline: Deadline) -> None:
    """Wait until a run of the transform that the history started has succeeded: a run whose
    run_after, the time of the newest of the asset events that started it, is not before since,
    the start of the history."""

    def succeeded() -> bool:
        if count_runs(airflow, TRANSFORM_DAG, "success", since=since):
            return True
        failed = count_runs(airflow, TRANSFORM_DAG, "failed", since=since)
        if failed and not transform_is_busy(airflow):
            # The run that the history started can succeed between the first count and the look at
            # whether the transform is busy, while a failed run of a person counts: so the
            # successes are counted once more before the job gives up.
            if count_runs(airflow, TRANSFORM_DAG, "success", since=since):
                return True
            raise SetupError(
                f"the DAG {TRANSFORM_DAG} failed after the demo history: see it in Airflow; a "
                "failed dbt test there means that dbt found broken data"
            )
        return False

    wait_until(
        succeeded,
        deadline,
        f"the DAG {TRANSFORM_DAG} did not succeed after the demo history",
        "see it in Airflow",
    )


def run_demo_pipeline(airflow: Airflow, deadline: Deadline) -> bool:
    """Run the demo pipeline, unless the history has succeeded and a run of the transform has
    succeeded after it; True if it ran.

    A start that finds them changes nothing in Airflow, so a pause that a person made stays. A
    deployment of v0.1 has a transform that succeeded and no history, so its first start of this
    release runs both; a history with no good transform after it, as after a failed critical test,
    runs again, which replaces the same days. Otherwise it unpauses both DAGs and waits until the
    transform is idle before the history: with max_active_runs=1, Airflow defers the run that the
    assets start while another run of the transform is queued or running, the deferred run
    consumes only the events up to the one that queued it, and an event that comes in between
    starts no run of its own. A transform that succeeds while the job waits, after a history that
    had succeeded, as when an earlier start ran out of time during it, counts, and the history
    does not run again. A history that is queued or running already is waited for, and not started
    again.
    """
    history = newest_run(airflow, LOAD_DAG, "success")
    if history is not None and count_runs(
        airflow, TRANSFORM_DAG, "success", since=history["run_after"]
    ):
        return False
    for dag_id in (LOAD_DAG, TRANSFORM_DAG):
        airflow.call("PATCH", f"/api/v2/dags/{dag_id}", {"is_paused": False})
    wait_until(
        lambda: not transform_is_busy(airflow),
        deadline,
        f"the DAG {TRANSFORM_DAG} did not finish its earlier runs",
        "see it in Airflow",
    )
    if history is not None and count_runs(
        airflow, TRANSFORM_DAG, "success", since=history["run_after"]
    ):
        return True
    load = newest_run(airflow, LOAD_DAG, "queued", "running")
    if load is None:
        load = airflow.call("POST", f"/api/v2/dags/{LOAD_DAG}/dagRuns", {"logical_date": None})
    load = wait_for_run(airflow, LOAD_DAG, load["dag_run_id"], deadline)
    if load["state"] != "success":
        raise SetupError(f"the DAG {LOAD_DAG} failed: see it in Airflow")
    wait_for_transform(airflow, load["start_date"], deadline)
    return True


def start_daily_load(airflow: Airflow) -> bool:
    """Unpause the daily load if it has never run, after the pipeline, and return True if it did:
    on the first start of the demo, and on the first start after an update that brings the DAG.
    Unpaused, the DAG makes a run at once, which loads nothing when the history has taken the day
    of every account already. A DAG that has run keeps its pause state, so a pause that a person
    made stays."""
    if count_runs(airflow, DAILY_DAG):
        return False
    airflow.call("PATCH", f"/api/v2/dags/{DAILY_DAG}", {"is_paused": False})
    return True


def marts_are_synced(
    metabase: Metabase, database_id: int, waiting: Mapping[tuple[str, str], Iterable[str]]
) -> bool:
    """Whether Metabase shows each table of the marts that waits, active, with the fields of its
    columns, by the rule of metadata.shows(). A person may have hidden a table in Admin > Table
    Metadata, and the metadata of a database leaves a hidden table out unless the request passes
    include_hidden=true (Metabase 0.63.19 and 0.64.1); a sync does not show it again. A table that a
    sync marked inactive, as after a reset of the warehouse, does not count."""
    shown = metadata.metadata_of(metabase, database_id)
    return all(
        metadata.shows(shown, metadata.Table(schema, name, None, None, ()), columns)
        for (schema, name), columns in waiting.items()
    )


def listed(keys: Iterable[tuple[str, str]]) -> str:
    """Tables by their schemas and names, as one phrase: a, b and c."""
    names = [f"{schema}.{name}" for schema, name in keys]
    return names[0] if len(names) == 1 else f"{', '.join(names[:-1])} and {names[-1]}"


def ensure_synced(
    metabase: Metabase,
    database_id: int,
    tables: tuple[metadata.Table, ...],
    deadline: Deadline,
    ran: bool,
) -> bool:
    """Make Metabase show the tables of the marts of the demo with the fields of their columns;
    True if a sync was needed. When Metabase does not show them so, the warehouse is asked through
    the connection of Metabase: a warehouse without one of them stops the job at once, with what to
    do (ran says whether the pipeline ran now), and the tables that it has get one sync, waited for
    until Metabase shows their fields, which come after the tables."""
    shown = metadata.metadata_of(metabase, database_id)
    wanted = metadata.Metadata({(t.schema, t.name): t for t in tables})
    sync = metadata.sync_needed(metabase, database_id, wanted, shown)
    if sync.absent:
        cause = RAN_NOW_HINT if ran else RAN_BEFORE_HINT
        raise SetupError(f"Metabase finds no {listed(sync.absent)} in the warehouse: {cause}")
    if not sync.tables:
        return False
    metabase.call("POST", f"/api/database/{database_id}/sync_schema")
    wait_until(
        lambda: marts_are_synced(metabase, database_id, sync.tables),
        deadline,
        f"Metabase did not find {listed(sync.tables)} after a sync of the warehouse",
        SYNC_HINT,
    )
    return True


def demo_marts(marts: metadata.Metadata) -> tuple[metadata.Table, ...]:
    """The tables of the marts that the questions of the demo read, in the effective metadata: the
    dbt project declares them."""
    tables = []
    for name in dict.fromkeys(q["mart"] for q in QUESTIONS):
        table = marts.effective.get((MART_SCHEMA, name))
        if table is None:
            raise SetupError(
                f"the dbt project declares no {MART_SCHEMA}.{name}: see ./agoranomos logs "
                "metadata-export"
            )
        tables.append(table)
    return tuple(tables)


def column_titles(marts: metadata.Metadata, name: str) -> dict[str, str]:
    """The display names of the columns of a mart, by name, in the effective metadata, and the
    titles of the ratios, which are no column of a mart."""
    table = marts.effective.get((MART_SCHEMA, name))
    columns = table.columns if table is not None else ()
    titles = {column.name: column.display_name for column in columns if column.display_name}
    return {**titles, **RATIO_TITLES}


def reporting_currency(marts: metadata.Metadata) -> str | None:
    """The reporting currency of the marts, as the metadata gives it to the money of the column
    spend_reporting: metadata-export renders it from AGORANOMOS_REPORTING_CURRENCY."""
    table = marts.effective.get((MART_SCHEMA, AD_STATS))
    columns = {c.name: c for c in table.columns} if table is not None else {}
    column = columns.get("spend_reporting")
    return column.currency if column is not None else None


Fields = Mapping[tuple[str, str], tuple[int, str]]


def fields_of(metabase: Metabase, database_id: int) -> dict[tuple[str, str], tuple[int, str]]:
    """The fields of the marts that Metabase shows, by the table and the column: the ID and the base
    type of each, which the filters of the questions name. Hidden ones count, as a filter on a field
    that a person hid still works."""
    shown = metadata.tables_of(metadata.metadata_of(metabase, database_id))
    return {
        (name, f["name"]): (f["id"], f["base_type"])
        for (schema, name), table in shown.items()
        if schema == MART_SCHEMA
        for f in table.get("fields") or []
        if f.get("parent_id") is None
    }


def column_format(name: str, currency: str | None) -> dict[str, Any]:
    """How a column shows its numbers: money in the reporting currency, a share as a percentage,
    and a ratio or a count with the decimals of DECIMALS. Metabase formats the column of a native
    question only from the settings of the question, as it is linked to no field."""
    if name in MONEY and currency:
        return {"number_style": "currency", "currency": currency}
    if name in PERCENT:
        return {"number_style": "percent", "decimals": 2}
    if name in DECIMALS:
        return {"decimals": DECIMALS[name]}
    return {}


def visualization_settings(
    question: Mapping[str, Any], titles: Mapping[str, str], currency: str | None
) -> dict:
    """The settings of a question: the title and the format of each of its columns, and for a chart
    the titles of its axes and the color of its series when it has one. A native question takes no
    title from the fields of a mart, so the titles are its own."""
    settings = copy.deepcopy(question.get("visualization_settings", {}))
    columns = {}
    for name in question["columns"]:
        shown = column_format(name, currency)
        if name in titles:
            shown = {"column_title": titles[name], **shown}
        if shown:
            columns[content.column_key(name)] = shown
    if columns:
        settings["column_settings"] = columns
    dimensions = settings.get("graph.dimensions") or [None]
    metrics = settings.get("graph.metrics") or [None]
    for key, name in zip(content.AXIS_TITLES, (dimensions[0], metrics[0]), strict=True):
        if name in titles:
            settings[key] = titles[name]
    if len(dimensions) == 1 and len(metrics) == 1 and metrics[0] is not None:
        settings["series_settings"] = {metrics[0]: {"color": content.SERIES_COLOR}}
    return settings


def answer_columns(question: Mapping[str, Any]) -> tuple[content.Column, ...]:
    """The columns of the answer of a question, with the types that Metabase gives them."""
    return tuple(
        content.Column(c, *COLUMN_TYPES.get(c, NUMBER), SEMANTIC_TYPES.get(c))
        for c in question["columns"]
    )


def question_filters(question: Mapping[str, Any], fields: Fields) -> tuple[content.Filter, ...]:
    """The filters of a question: those of its dashboard, each on the field of its column in the
    mart that the question reads."""
    filters = []
    for name in QUESTION_FILTERS[question["dashboard"]]:
        display_name, widget_type, column = FILTERS[name]
        field = fields.get((question["mart"], column))
        if field is None:
            raise SetupError(
                f"Metabase shows no field {MART_SCHEMA}.{question['mart']}.{column} for the filter "
                f"{name} of the demo: {SYNC_HINT}"
            )
        filters.append(content.Filter(name, display_name, widget_type, *field))
    return tuple(filters)


def demo_package(marts: metadata.Metadata, fields: Fields) -> content.Package:
    """The demo as a package of the content: its questions, with the titles and the formats of
    their columns, the colors of their series and their filters, and its two dashboards with their
    filters."""
    currency = reporting_currency(marts)
    return content.Package(
        key=DEMO_KEY,
        name=DEMO_NAME,
        description=DEMO_DESCRIPTION,
        questions=tuple(
            content.Question(
                key=q["key"],
                name=q["name"],
                description=q["description"],
                display=q["display"],
                query=q["query"],
                columns=answer_columns(q),
                visualization_settings=visualization_settings(
                    q, column_titles(marts, q["mart"]), currency
                ),
                filters=question_filters(q, fields),
            )
            for q in QUESTIONS
        ),
        dashboards=tuple(
            content.Dashboard(
                key,
                name,
                description,
                tuple(
                    content.Placement(q["key"], **q["layout"])
                    for q in QUESTIONS
                    if q["dashboard"] == key
                ),
                tuple(DASHBOARD_FILTERS[f] for f in QUESTION_FILTERS[key]),
            )
            for key, (name, description) in DASHBOARDS.items()
        ),
    )


# How each model of the items of a collection leaves the collection of v0.1 for the root, and its
# word in the log. A dashboard takes its own questions along, and a timeline its events (Metabase
# 0.64.1); a question of the dashboard of v0.1 leaves that dashboard too. An item of any other kind
# that a collection of Metabase 0.64.1 can hold, a document or a pulse, keeps the collection of v0.1
# where it is (valid-collection-models, collections/children.clj, read in October 2026).
TAKE_OUT = {
    "card": ("question", "card", {"collection_id": None}),
    "dataset": ("model", "card", {"collection_id": None}),
    "metric": ("metric", "card", {"collection_id": None}),
    "dashboard": ("dashboard", "dashboard", {"collection_id": None}),
    "collection": ("collection", "collection", {"parent_id": None}),
    "timeline": ("timeline", "timeline", {"collection_id": None}),
}


def take_out(
    metabase: Metabase, item: Mapping[str, Any], board_id: int, where: str, log: content.Log
) -> None:
    """Move an item that the demo does not ship to the root, where it stays in the trash if it was
    there, with one line by its model and its ID, never its name. A question of another dashboard
    moves with its dashboard; a question of the dashboard of the demo leaves it."""
    word, model, body = TAKE_OUT[item["model"]]
    if item.get("dashboard_id") and item["dashboard_id"] != board_id:
        return
    if item.get("dashboard_id"):
        body = {**body, "dashboard_id": None}
    if model == "collection" and item.get("archived"):
        body = {**body, "archived": True}  # a PUT of a collection without it restores it
    metabase.call("PUT", f"/api/{model}/{item['id']}", body)
    trash = ", in the trash," if item.get("archived") else ""
    log(
        f"moved out: {word} {item['id']} of {where}{trash} to the root, as the demo does not "
        "ship it"
    )


def made_by(metabase: Metabase, model: str, item_id: int, account_id: int) -> bool:
    """Whether the account made the question or the dashboard of an ID."""
    return metabase.call("GET", f"/api/{model}/{item_id}").get("creator_id") == account_id


def demo_of_v01(
    metabase: Metabase, items: list[dict[str, Any]], account_id: int
) -> dict[str, dict[str, Any]]:
    """The items of a collection of the old name that are the demo of v0.1, by their keys in the
    package: of each name of v0.1, the oldest question, and the oldest dashboard, that the account
    of demo-init made. A question of the demo is one of the collection, not of a dashboard."""
    found: dict[str, dict[str, Any]] = {}
    names = {**V01_KEYS, DASHBOARD_V01: ADVERTISING}
    for item in sorted(items, key=lambda item: item["id"]):
        model = "dashboard" if item.get("name") == DASHBOARD_V01 else "card"
        key = names.get(item.get("name"))
        if (
            key is not None
            and key not in found
            and item.get("model") == model
            and not item.get("archived")
            and not item.get("dashboard_id")
            and made_by(metabase, model, item["id"], account_id)
        ):
            found[key] = item
    return found


def collection_line(old: Mapping[str, Any]) -> str:
    return f"collection {old['id']} named {COLLECTION_V01!r} at the root"


def adopt_earlier_demo(metabase: Metabase, registry: content.Registry, log: content.Log) -> bool:
    """Move the demo collection of v0.1 into Agoranomos as Demo, once, and record it with its
    dashboard and questions, so that the next steps put them back as the code says (ADR-0018).

    Nothing is done when Metabase has the demo of the registry, also in the trash: a registry whose
    demo Metabase does not have, as after a restore of the Metabase of an earlier release, lets the
    collection of that release move again. The name of a collection proves nothing, as on a
    Metabase of v0.1 every account may curate the root: the demo of v0.1 is the collection named
    Agoranomos demo at the root, the one with the lowest ID of those, that holds the dashboard Ad
    spend (demo) that the account of demo-init made, the account that made the demo of v0.1. Of
    its questions, only those of the names of v0.1 that this account made are the demo's. When no
    collection is the demo, or one holds an item that the job cannot take out, nothing moves: one
    line says so, and the package makes its demo anew.

    Before the move, every other item of the collection, whoever made it, goes to the root, also
    from the trash, where it stays, with one line for each by its model and its ID, so that only
    the content of the package enters Agoranomos. Then the collection is locked where it is, at the
    root, so that no account but an admin can add an item to it, and listed once more: an item
    that reached it before the lock stops the job, and the collection stays at the root,
    view-only, for the next start, which takes that item out. Only then does the registry learn
    the demo, and the collection moves into Agoranomos as Demo: a job that stops before the
    registry leaves the collection to be checked again, and one that stops after it leaves the
    move to the next start, which puts the collection in its place. True if it moved one."""
    entry = registry.get(DEMO_KEY)
    if entry and content.find(metabase, "collection", entry["entity_id"]) is not None:
        return False
    candidates = sorted(
        (
            c
            for c in items_of(metabase.call("GET", "/api/collection"))
            if c.get("name") == COLLECTION_V01
            and c.get("location") == "/"
            and not c.get("personal_owner_id")
            and not c.get("archived")
        ),
        key=lambda c: c["id"],
    )
    if not candidates:
        return False
    account_id = metabase.call("GET", "/api/user/current")["id"]
    for old in candidates:
        items = content.everything_in(metabase, old["id"])
        adopted = demo_of_v01(metabase, items, account_id)
        if ADVERTISING in adopted:
            break
    else:
        ids = ", ".join(str(c["id"]) for c in candidates)
        one = len(candidates) == 1
        log(
            f"not moved: {'collection' if one else 'collections'} {ids} named {COLLECTION_V01!r} "
            f"at the root {'holds' if one else 'hold'} no dashboard {DASHBOARD_V01!r} that the "
            "account of demo-init made, "
            + (
                "so it is not the demo of v0.1, and it stays where it is"
                if one
                else "so none is the demo of v0.1, and they stay where they are"
            )
        )
        return False
    kept = {item["id"] for item in adopted.values() if item["model"] == "card"}
    board_id = adopted[ADVERTISING]["id"]
    others = [
        item
        for item in items
        if not (item.get("model") == "dashboard" and item["id"] == board_id)
        and not (item.get("model") == "card" and item["id"] in kept)
    ]
    unknown = next((item for item in others if item.get("model") not in TAKE_OUT), None)
    if unknown is not None:
        log(
            f"not moved: {collection_line(old)} holds a {unknown.get('model')} {unknown['id']} "
            "that demo-init cannot take out first, and it stays where it is"
        )
        return False
    for item in others:
        take_out(metabase, item, board_id, "the collection of v0.1", log)
    # Every account could curate the collection until now, and may have put an item into it since
    # the listing. Once it is locked, no account but an admin can add one, so what the next listing
    # finds is all there is.
    content.lock(metabase, {str(old["id"]): COLLECTION_V01}, log)
    named = {item["entity_id"] for item in adopted.values()}
    held = content.everything_in(metabase, old["id"])
    stray = next((i for i in held if i.get("entity_id") not in named), None)
    if stray is not None:
        word = TAKE_OUT.get(stray.get("model"), (stray.get("model"),))[0]
        raise content.ContentError(
            f"the collection of v0.1 got a {word} {stray['id']} before demo-init could lock it, "
            "which the demo does not ship: it stays at the root, view-only, and the next start "
            "takes that out and moves it"
        )
    for key, item in adopted.items():
        model = "dashboard" if key == ADVERTISING else "card"
        registry.set(f"{DEMO_KEY}/{key}", model, item["entity_id"])
    registry.set(DEMO_KEY, "collection", old["entity_id"])
    root = content.ensure_collection(
        metabase,
        registry,
        content.ROOT_KEY,
        content.ROOT_NAME,
        content.ROOT_DESCRIPTION,
        None,
        log,
    )
    moved = {"name": DEMO_NAME, "parent_id": root["id"]}
    metabase.call("PUT", f"/api/collection/{old['id']}", moved)
    log(
        f"moved: collection {COLLECTION_V01!r} of an earlier release into {content.ROOT_NAME} as "
        f"{DEMO_NAME}"
    )
    return True


def when(ran: bool) -> str:
    return "ran now" if ran else "ran before"


def main(env: Mapping[str, str] | None = None) -> int:
    # The server whose answers the job reads, for the line of an answer of an unexpected shape.
    server, hint = "Airflow", Airflow.logs_hint
    try:
        deadline = Deadline(DEADLINE)
        config = Config.from_env(os.environ if env is None else env)
        # The metadata and the registry of the content are read and checked first: a mistake in
        # the overrides of the deployment, or a registry that is none, stops the job before it
        # writes anything, in Airflow or in Metabase.
        marts = metadata.load(config.metadata_export, config.metadata_overrides)
        tables = demo_marts(marts)
        log = logged("demo-init")
        registry = content.Registry(config.content_registry, log)
        airflow = Airflow(config.airflow_url)
        sign_in_to_airflow(airflow, config.airflow_password)
        wait_for_dags(airflow, deadline)
        ran = run_demo_pipeline(airflow, deadline)
        if start_daily_load(airflow):
            log(f"unpaused {DAILY_DAG}, which loads the new days of the demo every day")
        server, hint = "Metabase", Metabase.logs_hint
        metabase = Metabase(config.metabase_url)
        sign_in(metabase, config.admin_email, config.admin_password)
        # The connection that metabase-init keeps up to date; its log names it when several match.
        database = find_warehouse(list_databases(metabase), config, warn=False)
        if database is None:
            raise SetupError(
                "Metabase has no connection to the warehouse: see ./agoranomos logs metabase-init"
            )
        ensure_synced(metabase, database["id"], tables, deadline, ran)
        metadata.apply(
            metabase,
            database["id"],
            marts,
            lambda check, what: wait_until(check, deadline, what, SYNC_HINT),
            log=log,
        )
        adopt_earlier_demo(metabase, registry, log)
        package = demo_package(marts, fields_of(metabase, database["id"]))
        ids = content.ensure_package(metabase, registry, package, database["id"], log)
        # The metrics and the dashboard Advertising of the marts that the first run of the
        # transform has just built, which metabase-init could not create before them.
        metrics.ensure(metabase, registry, config.metadata_export, database["id"], log)
        advertising.ensure(metabase, registry, marts, config.metadata_export, database["id"], log)
    except (SetupError, metadata.MetadataError, content.ContentError) as error:
        print(f"demo-init: {error}", file=sys.stderr)
        return 1
    except SHAPE_ERRORS as error:
        print(f"demo-init: {unexpected_shape(server, error, hint)}", file=sys.stderr)
        return 1
    boards = " and ".join(f"{name!r} ({ids[key]})" for key, (name, _) in DASHBOARDS.items())
    print(
        f"demo-init: the demo pipeline {when(ran)}, and the dashboards {boards} are in the "
        f"collection {content.ROOT_NAME} / {DEMO_NAME}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
