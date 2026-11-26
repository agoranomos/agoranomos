"""A generator of synthetic data for the profiles of ADR-0040, which writes the core of a running
stack directly, for the measurements of the profile of an agency (tests/stack/test_profile.py).

The cards of Metabase, the values of its filters and its scan read the views of marts, which read
the core and nothing else (ADR-0037), so the generator writes the core as the transform would: the
clients, the accounts, their loads, the campaigns, ad groups and ads, the conversion actions, the
rates, and the four facts of the ad platforms, by month, with the statistics of the ad groups and of
the campaigns summed from those of their ads, as a platform reports them. It writes no raw layer
and runs no dbt, so a period of a year at the density of the limit takes minutes and not a day of
loads, and nothing that the transform reads changes.

A profile has a density, the rows of ads a day, over a period that ends yesterday:

* agency, the working profile of ADR-0040: 25 accounts active every day, each with 45 campaigns,
  225 ad groups and 600 ads, so 15,000 rows of ads a day, among 200 accounts in the registry, the
  other 175 with no statistics, over 400 days by default;
* limit, the limit of one server: 200 accounts active every day, each with 30 campaigns, 150 ad
  groups and 400 ads, so 80,000 rows of ads a day, over 100 days by default, which covers the 12
  weeks of the dashboard Advertising and the 12 before them for its comparisons in part, and fits
  the disk of a runner of GitHub.

Each campaign has three conversion actions a day, two of them primary. The accounts spread over the
six ad platforms of the seed of the base package, and over 40 clients; one in five reports in EUR,
shifted by one account after each round of the platforms, so that each platform has accounts in
EUR, the others in USD, with a rate for each day into the reporting currency of .env. The numbers
come from the IDs and the day, so the same profile and period give the same rows.

Everything that it writes is marked: the IDs of the clients start with synthetic_, those of the
accounts and of the conversion actions with synthetic-, the rates have the source synthetic, and a
partition that it creates carries the comment MARK. remove() deletes all of it, in a transaction
that thaws the frozen months for itself (ADR-0035), and drops the partitions that it created and
that are then empty. The loads of the accounts are accepted, so that no run of the transform
replaces their days: a run reads the pinned loads only (dbt/macros/newest_load.sql). A run of the
transform still gives every account the client that the settings of the model name, which none of
these accounts has, so restore_accounts() puts their clients back after one.

For the models that read the lists of the entities, history_sql() gives the journal of the loads
the accepted loads that the maintenance keeps, and whole_load_sql() writes a whole load of every
platform in raw, as a connector or the demo writes it: the lists of the accounts, campaigns, ad
groups and ads of the synthetic accounts, with a window of a day on which the core holds no
statistics, which the next run of the transform pins and reads. remove() deletes their rows too,
and leaves the tables of raw that it made, empty.

As a command, in the root of the checkout, with the stack up:

    uv run python tests/stack/synthetic.py generate agency [--days 400]
    uv run python tests/stack/synthetic.py status
    uv run python tests/stack/synthetic.py remove

It connects as the superuser of the warehouse over loopback in the container, as the stack tests
do (stack_helpers.warehouse), and prints counts and times, never a secret.
"""

from __future__ import annotations

import argparse
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent))

from stack_helpers import REPO_ROOT, load_env, warehouse_output

# The comment of a partition that the generator created, by which remove() finds it.
MARK = "agoranomos synthetic data: a partition made by tests/stack/synthetic.py"
# The ad platforms of the seed of the base package (dbt/seeds/seed_ad_platforms.csv), which the
# views of marts name; the accounts take them in turn.
PLATFORMS = ("google_ads", "meta_ads", "tiktok_ads", "demo_search", "demo_social", "demo_video")
CLIENTS = 40
# The conversion actions of each platform: the ID after the prefix, the category, and whether it is
# primary, with the share of the conversions of a campaign that each takes.
ACTIONS = (
    ("purchase", "purchase", True, "0.5"),
    ("lead", "lead", True, "0.3"),
    ("page-view", "other", False, "0.2"),
)
# The facts that the generator writes, in the order of their months.
FACTS = (
    "ad_stats_ad_daily",
    "ad_stats_ad_group_daily",
    "ad_stats_campaign_daily",
    "ad_conversions_campaign_daily",
)
# The day from which the numbers count the days, so that a day gives the same numbers whenever the
# generator runs.
EPOCH = date(2000, 1, 1)
ACCOUNTS = "a.account_id LIKE 'synthetic-%'"


@dataclass(frozen=True)
class Profile:
    """The shape of the data of a profile: the accounts of the registry, those with statistics on
    every day of the period, and the entities of each of those."""

    name: str
    accounts: int
    active_accounts: int
    campaigns: int
    ad_groups: int
    ads: int
    days: int

    @property
    def ad_rows_a_day(self) -> int:
        return self.active_accounts * self.ads

    @property
    def rows_a_day(self) -> dict[str, int]:
        """The rows of each fact a day."""
        return {
            "ad_stats_ad_daily": self.active_accounts * self.ads,
            "ad_stats_ad_group_daily": self.active_accounts * self.ad_groups,
            "ad_stats_campaign_daily": self.active_accounts * self.campaigns,
            "ad_conversions_campaign_daily": self.active_accounts * self.campaigns * len(ACTIONS),
        }


PROFILES = {
    "agency": Profile("agency", 200, 25, 45, 225, 600, 400),
    "limit": Profile("limit", 200, 200, 30, 150, 400, 100),
}


def period(days: int, today: date | None = None) -> tuple[date, date]:
    """The first and the last day of a period of days that ends yesterday."""
    last = (today or date.today()) - timedelta(days=1)
    return last - timedelta(days=days - 1), last


def months(first: date, last: date) -> list[tuple[date, date]]:
    """The first and the last day of each month of the period that lies in it."""
    found = []
    start = first
    while start <= last:
        following = (start.replace(day=1) + timedelta(days=32)).replace(day=1)
        found.append((start, min(last, following - timedelta(days=1))))
        start = following
    return found


def registry_sql(profile: Profile, first: date, last: date, reporting: str) -> str:
    """The clients, the accounts, their loads, the conversion actions, the entities and the rates,
    in one transaction. The accounts with statistics are the first active_accounts of them."""
    platforms = "ARRAY[" + ", ".join(f"'{p}'" for p in PLATFORMS) + "]"
    actions = ", ".join(
        f"('synthetic-{key}', '{category}', {str(primary).lower()})"
        for key, category, primary, _ in ACTIONS
    )
    groups_per_campaign = profile.ad_groups // profile.campaigns
    return f"""
BEGIN;
INSERT INTO core.dim_client (client_id, client_name, is_default)
SELECT 'synthetic_' || lpad(n::text, 2, '0'), 'Synthetic client ' || lpad(n::text, 2, '0'), false
FROM generate_series(1, {CLIENTS}) AS n;
INSERT INTO core.dim_account (platform, account_id, account_name, client_id, currency, time_zone,
    account_status, is_active, is_stub, first_report_date, last_report_date)
SELECT ({platforms})[(n - 1) % {len(PLATFORMS)} + 1], 'synthetic-' || lpad(n::text, 4, '0'),
    'Synthetic account ' || lpad(n::text, 4, '0'),
    'synthetic_' || lpad(((n - 1) % {CLIENTS} + 1)::text, 2, '0'),
    CASE WHEN (n + (n - 1) / {len(PLATFORMS)}) % 5 = 0 THEN 'EUR' ELSE 'USD' END, 'UTC', 'ENABLED',
    true, false,
    CASE WHEN n <= {profile.active_accounts} THEN date '{first}' END,
    CASE WHEN n <= {profile.active_accounts} THEN date '{last}' END
FROM generate_series(1, {profile.accounts}) AS n;
-- One load of each account with statistics, accepted, so that no run of the transform reads it.
INSERT INTO core.loads (source, load_id, account_id, window_from, window_to, outcome, finished_at,
    invocation_id, status, accepted_at)
SELECT a.platform, '1700000000.' || substr(a.account_id, 11), a.account_id, date '{first}',
    date '{last}', 'loaded', now(), gen_random_uuid(), 'accepted', now()
FROM core.dim_account AS a
WHERE {ACCOUNTS} AND a.first_report_date IS NOT NULL;
INSERT INTO core.dim_conversion_action (platform, conversion_action_id, conversion_action_name,
    conversion_action_status, platform_category, conversion_category,
    include_in_conversions_metric, is_primary)
SELECT p, x.id, initcap(replace(substr(x.id, 11), '-', ' ')), 'ENABLED', x.category, x.category,
    x.is_primary, x.is_primary
FROM unnest({platforms}) AS p
CROSS JOIN (VALUES {actions}) AS x (id, category, is_primary);
INSERT INTO core.dim_campaign (account_sk, campaign_id, campaign_id_text, campaign_name,
    campaign_status, campaign_state, campaign_type, name_count, first_report_date, last_report_date,
    last_seen_at, is_listed)
SELECT a.account_sk, e.id, e.id::text,
    'Synthetic campaign ' || substr(a.account_id, 11) || '-' || lpad(c::text, 3, '0'),
    s.status, s.state, 'SEARCH', 1, date '{first}', date '{last}', now(), true
FROM core.dim_account AS a
CROSS JOIN generate_series(1, {profile.campaigns}) AS c
CROSS JOIN LATERAL (SELECT substr(a.account_id, 11)::bigint * 1000 + c AS id) AS e
CROSS JOIN LATERAL (SELECT CASE WHEN c % 20 = 0 THEN 'REMOVED' WHEN c % 7 = 0 THEN 'PAUSED'
    ELSE 'ENABLED' END AS status) AS t
CROSS JOIN LATERAL (SELECT t.status, CASE t.status WHEN 'REMOVED' THEN 'removed'
    WHEN 'PAUSED' THEN 'paused' ELSE 'active' END AS state) AS s
WHERE {ACCOUNTS} AND a.first_report_date IS NOT NULL;
INSERT INTO core.dim_ad_group (account_sk, ad_group_id, ad_group_id_text, campaign_id,
    ad_group_name, ad_group_status, ad_group_state, ad_group_type, first_report_date,
    last_report_date, last_seen_at, is_listed)
SELECT a.account_sk, e.id, e.id::text, e.campaign_id,
    'Synthetic ad group ' || substr(a.account_id, 11) || '-' || lpad(g::text, 4, '0'),
    'ENABLED', 'active', 'SEARCH_STANDARD', date '{first}', date '{last}', now(), true
FROM core.dim_account AS a
CROSS JOIN generate_series(1, {profile.ad_groups}) AS g
CROSS JOIN LATERAL (SELECT substr(a.account_id, 11)::bigint * 100000 + g AS id,
    substr(a.account_id, 11)::bigint * 1000 + (g - 1) / {groups_per_campaign} + 1 AS campaign_id)
    AS e
WHERE {ACCOUNTS} AND a.first_report_date IS NOT NULL;
INSERT INTO core.dim_ad (account_sk, ad_group_id, ad_id, ad_id_text, campaign_id, ad_name,
    ad_status, ad_state, ad_type, first_report_date, last_report_date, last_seen_at, is_listed)
SELECT a.account_sk, g.ad_group_id, e.id, e.id::text, g.campaign_id,
    'Synthetic ad ' || substr(a.account_id, 11) || '-' || lpad(n::text, 4, '0'),
    'ENABLED', 'active', 'RESPONSIVE_SEARCH_AD', date '{first}', date '{last}', now(), true
FROM core.dim_account AS a
CROSS JOIN generate_series(1, {profile.ads}) AS n
CROSS JOIN LATERAL (SELECT substr(a.account_id, 11)::bigint * 10000000 + n AS id,
    substr(a.account_id, 11)::bigint * 100000 + (n - 1) % {profile.ad_groups} + 1 AS group_id) AS e
JOIN core.dim_ad_group AS g ON g.account_sk = a.account_sk AND g.ad_group_id = e.group_id
WHERE {ACCOUNTS} AND a.first_report_date IS NOT NULL;
{rates_sql(first, last, reporting)}
COMMIT;
"""  # noqa: S608 (numbers, dates and constants of this file)


def rates_sql(first: date, last: date, reporting: str) -> str:
    """A rate for each day from first to last and each currency of the accounts into the
    reporting currency, where none is there. The model takes no rate of the source synthetic as
    published, so it carries none of them over a later day (macros/rates.sql)."""
    return f"""
INSERT INTO core.fx_rates_daily (rate_date, currency, reporting_currency,
    rate_into_reporting_currency, base_currency, base_rate, rate_source, published_on,
    is_carried_forward)
SELECT d::date, x.currency, '{reporting}',
    CASE WHEN x.currency = '{reporting}' THEN 1
        WHEN x.currency = 'EUR' THEN 1.08 ELSE round(1 / 1.08, 10) END,
    CASE WHEN x.currency <> '{reporting}' THEN 'EUR' END,
    CASE WHEN x.currency <> '{reporting}' THEN 1.08 END,
    'synthetic', CASE WHEN x.currency <> '{reporting}' THEN d::date END, false
FROM generate_series(date '{first}', date '{last}', interval '1 day') AS d
CROSS JOIN (VALUES ('USD'), ('EUR')) AS x (currency)
ON CONFLICT DO NOTHING;
"""  # noqa: S608 (dates and constants of this file)


def extend_rates(env: dict[str, str], last: date) -> None:
    """Give each currency of the accounts a synthetic rate on every day after the last that the
    rates of the profile hold, through last, as a source of the rates publishes one each day."""
    held = warehouse_output(
        env, "SELECT max(rate_date) FROM core.fx_rates_daily WHERE rate_source = 'synthetic'"
    )
    reporting = env.get("AGORANOMOS_REPORTING_CURRENCY") or "USD"
    first = date.fromisoformat(held) + timedelta(days=1)
    if first <= last:
        warehouse_output(env, rates_sql(first, last, reporting))


def partitions_sql(first: date, last: date) -> str:
    """The partitions of the months of the period of each fact, with MARK on those made now."""
    statements = []
    for start, _ in months(first, last):
        month = start.replace(day=1)
        for fact in FACTS:
            name = f"core.{fact}_{month:%Y_%m}"
            statements.append(
                f"DO $$ BEGIN IF core.ensure_partition('core.{fact}', date '{month}') THEN "
                f"COMMENT ON TABLE {name} IS '{MARK}'; END IF; END $$;"
            )
    return "\n".join(statements)


def month_sql(first: date, last: date) -> str:
    """The statistics of every ad of the accounts with statistics on each day from first to last,
    in one month, then those of their ad groups and campaigns, summed from the ads, and the
    conversions of the campaigns by action, in one transaction."""
    shares = " ".join(f"WHEN 'synthetic-{key}' THEN {share}" for key, _, _, share in ACTIONS)
    measures = (
        "sum(impressions), sum(clicks), sum(spend), sum(conversions), sum(all_conversions), "
        "sum(conversion_value), sum(all_conversion_value)"
    )
    columns = (
        "impressions, clicks, spend, conversions, all_conversions, conversion_value, "
        "all_conversion_value, currency, load_sk"
    )
    return f"""
BEGIN;
CREATE TEMPORARY TABLE synthetic_days ON COMMIT DROP AS
SELECT d::date AS day, d::date - date '{EPOCH}' AS n
FROM generate_series(date '{first}', date '{last}', interval '1 day') AS d;
INSERT INTO core.ad_stats_ad_daily (account_sk, campaign_id, ad_group_id, ad_id, report_date,
    {columns})
SELECT e.account_sk, e.campaign_id, e.ad_group_id, e.ad_id, d.day, i.impressions, k.clicks,
    round(k.clicks * (0.2 + (e.ad_id % 50) / 25.0), 6),
    round(k.clicks * 0.04 * (1 + e.ad_id % 3), 2),
    round(k.clicks * 0.05 * (1 + e.ad_id % 3), 2),
    round(k.clicks * 1.6 * (1 + e.ad_id % 3), 6),
    round(k.clicks * 2.0 * (1 + e.ad_id % 3), 6),
    a.currency, l.load_sk
FROM core.dim_ad AS e
JOIN core.dim_account AS a ON a.account_sk = e.account_sk
JOIN core.loads AS l ON l.source = a.platform AND l.account_id = a.account_id
CROSS JOIN synthetic_days AS d
CROSS JOIN LATERAL (SELECT 50 + (e.ad_id * 40503 + d.n * 7919) % 950 AS impressions) AS i
CROSS JOIN LATERAL (SELECT i.impressions * (1 + e.ad_id % 7) / 100 AS clicks) AS k
WHERE {ACCOUNTS};
INSERT INTO core.ad_stats_ad_group_daily (account_sk, campaign_id, ad_group_id, report_date,
    {columns})
SELECT f.account_sk, f.campaign_id, f.ad_group_id, f.report_date, {measures}, f.currency,
    f.load_sk
FROM core.ad_stats_ad_daily AS f
JOIN core.dim_account AS a ON a.account_sk = f.account_sk
WHERE {ACCOUNTS} AND f.report_date BETWEEN date '{first}' AND date '{last}'
GROUP BY f.account_sk, f.campaign_id, f.ad_group_id, f.report_date, f.currency, f.load_sk;
INSERT INTO core.ad_stats_campaign_daily (account_sk, campaign_id, report_date, {columns})
SELECT f.account_sk, f.campaign_id, f.report_date, {measures}, f.currency, f.load_sk
FROM core.ad_stats_ad_group_daily AS f
JOIN core.dim_account AS a ON a.account_sk = f.account_sk
WHERE {ACCOUNTS} AND f.report_date BETWEEN date '{first}' AND date '{last}'
GROUP BY f.account_sk, f.campaign_id, f.report_date, f.currency, f.load_sk;
INSERT INTO core.ad_conversions_campaign_daily (account_sk, campaign_id, conversion_action_sk,
    report_date, conversions, all_conversions, conversion_value, all_conversion_value,
    attribution_setting, currency, load_sk)
SELECT f.account_sk, f.campaign_id, k.conversion_action_sk, f.report_date,
    round(coalesce(f.conversions, 0) * s.share, 6), round(f.all_conversions * s.share, 6),
    round(f.conversion_value * s.share, 6), round(f.all_conversion_value * s.share, 6),
    'DATA_DRIVEN', f.currency, f.load_sk
FROM core.ad_stats_campaign_daily AS f
JOIN core.dim_account AS a ON a.account_sk = f.account_sk
JOIN core.dim_conversion_action AS k
    ON k.platform = a.platform AND k.conversion_action_id LIKE 'synthetic-%'
CROSS JOIN LATERAL (SELECT CASE k.conversion_action_id {shares} END AS share) AS s
WHERE {ACCOUNTS} AND f.report_date BETWEEN date '{first}' AND date '{last}';
COMMIT;
"""  # noqa: S608 (numbers, dates and constants of this file)


# A load of the history of the journal of loads (history_sql) and a whole load with the lists of
# the entities (whole_load_sql), for the measurement of the lists at the density of a profile
# (test_profile.py). The rows that they write in raw carry this name of the schema of dlt in
# _dlt_loads, by which remove() finds them, and a table of raw that whole_load_sql creates carries
# RAW_MARK.
SCHEMA_NAME = "synthetic"
RAW_MARK = "agoranomos synthetic data: a table of raw made by tests/stack/synthetic.py"
# The accepted loads of the kind window that the maintenance keeps (ADR-0046), and those of the
# kind intraday, with the hours between two of them (ADR-0051): the short cycle of Google Ads and
# that of the demo, every AGORANOMOS_INTRADAY_HOURS hours, 3 by default.
HISTORY_MONTHS = 13
INTRADAY_DAYS = 14
INTRADAY_HOURS = 3
INTRADAY_PLATFORMS = ("google_ads", "demo_search")
VARCHAR = "character varying"
# The journal of the loads of a source as dlt writes it (newest_load_columns of
# dbt/macros/newest_load.sql), and _dlt_loads, where dlt writes the end of a load.
JOURNAL = {
    "account_id": VARCHAR,
    "as_of": "date",
    "window_from": "date",
    "window_to": "date",
    "now": "timestamp with time zone",
    "loaded_at": "timestamp with time zone",
    "status": VARCHAR,
    "error": VARCHAR,
    "kind": VARCHAR,
}
DLT_LOADS = (
    "load_id character varying NOT NULL, schema_name character varying, "
    "status bigint NOT NULL, inserted_at timestamp with time zone NOT NULL, "
    "schema_version_hash character varying"
)
# The rows that each kind of list reads: a the account, c the campaign, g the ad group and e the
# ad, of the synthetic accounts of one platform. Every account of the registry is listed, and the
# entities of the accounts with statistics.
LIST_FROM = {
    "accounts": "core.dim_account AS a",
    "campaigns": "core.dim_campaign AS c JOIN core.dim_account AS a ON a.account_sk = c.account_sk",
    "ad_groups": "core.dim_ad_group AS g JOIN core.dim_account AS a ON a.account_sk = g.account_sk",
    "ads": "core.dim_ad AS e JOIN core.dim_account AS a ON a.account_sk = e.account_sk",
}


# The lists of one platform: for each kind, its table and its columns, each with its type and its
# SQL.
Lists = dict[str, tuple[str, dict[str, tuple[str, str]]]]


def google_lists(accounts: str) -> Lists:
    """The lists of the shape of Google Ads (dbt/macros/shapes/google_ads_v25.sql): the table of
    each kind, and its columns with their type and their SQL. The status comes from the common
    state of the core, so that a second load gives what the first one left."""

    def status(state: str) -> str:
        return (
            f"CASE {state} WHEN 'paused' THEN 'PAUSED' WHEN 'removed' THEN 'REMOVED' "
            "ELSE 'ENABLED' END"
        )

    campaign = "'customers/' || a.account_id || '/campaigns/' || {}"
    group = "'customers/' || a.account_id || '/adGroups/' || {}"
    if accounts == "customers":
        account_columns = {
            "customer__resource_name": (VARCHAR, "'customers/' || a.account_id"),
            "customer__id": (VARCHAR, "a.account_id"),
            "customer__descriptive_name": (VARCHAR, "a.account_name"),
            "customer__currency_code": (VARCHAR, "a.currency"),
            "customer__time_zone": (VARCHAR, "a.time_zone"),
            "customer__manager": ("boolean", "false"),
            "customer__status": (VARCHAR, "'ENABLED'"),
        }
    else:
        account_columns = {
            "customer_client__id": (VARCHAR, "a.account_id"),
            "customer_client__resource_name": (
                VARCHAR,
                "'customers/0/customerClients/' || a.account_id",
            ),
            "customer_client__descriptive_name": (VARCHAR, "a.account_name"),
            "customer_client__currency_code": (VARCHAR, "a.currency"),
            "customer_client__time_zone": (VARCHAR, "a.time_zone"),
            "customer_client__manager": ("boolean", "false"),
            "customer_client__status": (VARCHAR, "'ENABLED'"),
        }
    return {
        "accounts": (accounts, account_columns),
        "campaigns": (
            "campaigns",
            {
                "customer__id": (VARCHAR, "a.account_id"),
                "campaign__resource_name": (VARCHAR, campaign.format("c.campaign_id_text")),
                "campaign__id": (VARCHAR, "c.campaign_id_text"),
                "campaign__name": (VARCHAR, "c.campaign_name"),
                "campaign__status": (VARCHAR, status("c.campaign_state")),
                "campaign__advertising_channel_type": (VARCHAR, "c.campaign_type"),
            },
        ),
        "ad_groups": (
            "ad_groups",
            {
                "customer__id": (VARCHAR, "a.account_id"),
                "campaign__resource_name": (VARCHAR, campaign.format("g.campaign_id::text")),
                "campaign__id": (VARCHAR, "g.campaign_id::text"),
                "ad_group__resource_name": (VARCHAR, group.format("g.ad_group_id_text")),
                "ad_group__id": (VARCHAR, "g.ad_group_id_text"),
                "ad_group__name": (VARCHAR, "g.ad_group_name"),
                "ad_group__status": (VARCHAR, status("g.ad_group_state")),
                "ad_group__type": (VARCHAR, "g.ad_group_type"),
            },
        ),
        "ads": (
            "ads",
            {
                "customer__id": (VARCHAR, "a.account_id"),
                "campaign__resource_name": (VARCHAR, campaign.format("e.campaign_id::text")),
                "campaign__id": (VARCHAR, "e.campaign_id::text"),
                "ad_group__resource_name": (VARCHAR, group.format("e.ad_group_id::text")),
                "ad_group__id": (VARCHAR, "e.ad_group_id::text"),
                "ad_group_ad__resource_name": (
                    VARCHAR,
                    "'customers/' || a.account_id || '/adGroupAds/' || e.ad_group_id || '~' "
                    "|| e.ad_id_text",
                ),
                "ad_group_ad__status": (VARCHAR, status("e.ad_state")),
                "ad_group_ad__ad__resource_name": (
                    VARCHAR,
                    "'customers/' || a.account_id || '/ads/' || e.ad_id_text",
                ),
                "ad_group_ad__ad__id": (VARCHAR, "e.ad_id_text"),
                "ad_group_ad__ad__name": (VARCHAR, "e.ad_name"),
                "ad_group_ad__ad__type": (VARCHAR, "e.ad_type"),
            },
        ),
    }


def meta_lists(demo: bool) -> Lists:
    """The lists of the shape of Meta (dbt/macros/shapes/meta_v26.sql), with the columns of the
    connector of Meta or, for demo_social, those of the demo (dbt/macros/demo_source.sql)."""

    def status(state: str) -> dict[str, tuple[str, str]]:
        value = (
            f"CASE {state} WHEN 'paused' THEN 'PAUSED' WHEN 'removed' THEN 'DELETED' "
            "ELSE 'ACTIVE' END"
        )
        found = {"status": (VARCHAR, value)}
        if not demo:
            found |= {"configured_status": (VARCHAR, value), "effective_status": (VARCHAR, value)}
        return found

    return {
        "accounts": (
            "ad_accounts",
            {
                "id": (VARCHAR, "'act_' || a.account_id"),
                "account_id": (VARCHAR, "a.account_id"),
                "name": (VARCHAR, "a.account_name"),
                "currency": (VARCHAR, "a.currency"),
                "timezone_name": (VARCHAR, "a.time_zone"),
                "account_status": ("bigint", "1") if demo else (VARCHAR, "'1'"),
            },
        ),
        "campaigns": (
            "campaigns",
            {
                "id": (VARCHAR, "c.campaign_id_text"),
                "account_id": (VARCHAR, "a.account_id"),
                "name": (VARCHAR, "c.campaign_name"),
                **status("c.campaign_state"),
                "objective": (VARCHAR, "c.campaign_type"),
            },
        ),
        "ad_groups": (
            "adsets",
            {
                "id": (VARCHAR, "g.ad_group_id_text"),
                "account_id": (VARCHAR, "a.account_id"),
                "campaign_id": (VARCHAR, "g.campaign_id::text"),
                "name": (VARCHAR, "g.ad_group_name"),
                **status("g.ad_group_state"),
                "optimization_goal": (VARCHAR, "g.ad_group_type"),
            },
        ),
        "ads": (
            "ads",
            {
                "id": (VARCHAR, "e.ad_id_text"),
                "account_id": (VARCHAR, "a.account_id"),
                "campaign_id": (VARCHAR, "e.campaign_id::text"),
                "adset_id": (VARCHAR, "e.ad_group_id::text"),
                "name": (VARCHAR, "e.ad_name"),
                **status("e.ad_state"),
            },
        ),
    }


def tiktok_lists(demo: bool) -> Lists:
    """The lists of the shape of TikTok (dbt/macros/shapes/tiktok_v1_3.sql), with the columns of
    the connector of TikTok or, for demo_video, those of the demo."""

    def status(state: str, deleted: str) -> dict[str, tuple[str, str]]:
        return {
            "operation_status": (
                VARCHAR,
                f"CASE {state} WHEN 'paused' THEN 'DISABLE' ELSE 'ENABLE' END",
            ),
            "secondary_status": (VARCHAR, f"CASE {state} WHEN 'removed' THEN '{deleted}' END"),
        }

    advertiser = {
        "advertiser_id": (VARCHAR, "a.account_id"),
        "name": (VARCHAR, "a.account_name"),
        "currency": (VARCHAR, "a.currency"),
        "timezone": (VARCHAR, "a.time_zone"),
        "status": (VARCHAR, "'STATUS_ENABLE'"),
    }
    if not demo:
        advertiser["display_timezone"] = (VARCHAR, "a.time_zone")
    return {
        "accounts": ("advertisers", advertiser),
        "campaigns": (
            "campaigns",
            {
                "advertiser_id": (VARCHAR, "a.account_id"),
                "campaign_id": (VARCHAR, "c.campaign_id_text"),
                "campaign_name": (VARCHAR, "c.campaign_name"),
                "objective_type": (VARCHAR, "c.campaign_type"),
                **status("c.campaign_state", "CAMPAIGN_STATUS_DELETE"),
            },
        ),
        "ad_groups": (
            "adgroups",
            {
                "advertiser_id": (VARCHAR, "a.account_id"),
                "campaign_id": (VARCHAR, "g.campaign_id::text"),
                "adgroup_id": (VARCHAR, "g.ad_group_id_text"),
                "adgroup_name": (VARCHAR, "g.ad_group_name"),
                "optimization_goal": (VARCHAR, "g.ad_group_type"),
                **status("g.ad_group_state", "ADGROUP_STATUS_DELETE"),
            },
        ),
        "ads": (
            "ads",
            {
                "advertiser_id": (VARCHAR, "a.account_id"),
                "campaign_id": (VARCHAR, "e.campaign_id::text"),
                "adgroup_id": (VARCHAR, "e.ad_group_id::text"),
                "ad_id": (VARCHAR, "e.ad_id_text"),
                "ad_name": (VARCHAR, "e.ad_name"),
                "ad_format": (VARCHAR, "e.ad_type"),
                **status("e.ad_state", "AD_STATUS_DELETE"),
            },
        ),
    }


# The lists of each platform of PLATFORMS in its schema of raw, as its connector or the demo
# writes them: the tables of its staging models of the accounts, campaigns, ad groups and ads.
LISTS = {
    "google_ads": google_lists("customers"),
    "meta_ads": meta_lists(demo=False),
    "tiktok_ads": tiktok_lists(demo=False),
    "demo_search": google_lists("customer_clients"),
    "demo_social": meta_lists(demo=True),
    "demo_video": tiktok_lists(demo=True),
}


def history_sql(first: date, last: date) -> str:
    """The journal of the loads that the accounts of the registry would hold after the days of the
    period, as the maintenance keeps it (ADR-0046, ADR-0051): an accepted load of the kind window of
    each account on each day of the period, back at most HISTORY_MONTHS months, at five in the
    morning of the next day in UTC, and an accepted load of the kind intraday of each account of
    INTRADAY_PLATFORMS every INTRADAY_HOURS hours over the last INTRADAY_DAYS days. No fact reads
    them, and no run reads an accepted load older than a pinned one (macros/newest_load.sql)."""
    platforms = ", ".join(f"'{p}'" for p in INTRADAY_PLATFORMS)
    return f"""
BEGIN;
INSERT INTO core.loads (source, load_id, account_id, window_from, window_to, outcome, finished_at,
    invocation_id, status, accepted_at, kind)
SELECT a.platform,
    to_char(extract(epoch FROM t.at), 'FM9999999999') || '.' || substr(a.account_id, 11),
    a.account_id, d::date, d::date, 'loaded', t.at, gen_random_uuid(), 'accepted', t.at, 'window'
FROM core.dim_account AS a
CROSS JOIN generate_series(date '{first}', date '{last}', interval '1 day') AS d
CROSS JOIN LATERAL (SELECT (d::date + 1 + time '05:00') AT TIME ZONE 'UTC' AS at) AS t
WHERE {ACCOUNTS} AND d::date >= (now() - interval '{HISTORY_MONTHS} months')::date
    AND t.at < now() - interval '1 hour'
ON CONFLICT DO NOTHING;
INSERT INTO core.loads (source, load_id, account_id, window_from, window_to, outcome, finished_at,
    invocation_id, status, accepted_at, kind)
SELECT a.platform,
    to_char(extract(epoch FROM t), 'FM9999999999') || '.5' || substr(a.account_id, 11),
    a.account_id, (t AT TIME ZONE 'UTC')::date, (t AT TIME ZONE 'UTC')::date, 'loaded', t,
    gen_random_uuid(), 'accepted', t, 'intraday'
FROM core.dim_account AS a
CROSS JOIN generate_series(date_trunc('hour', now()) - interval '{INTRADAY_DAYS} days',
    now() - interval '1 hour', interval '{INTRADAY_HOURS} hours') AS t
WHERE {ACCOUNTS} AND a.platform IN ({platforms})
ON CONFLICT DO NOTHING;
COMMIT;
ANALYZE core.loads;
"""  # noqa: S608 (numbers, dates and constants of this file)


def whole_load_sql(load_id: str, day: date, pin: bool = False) -> str:
    """A whole load of every platform of PLATFORMS, with the load id given, as its connector or the
    demo writes it in raw: the lists of the accounts, campaigns, ad groups and ads of LISTS, of
    every synthetic account of the platform, its row in the journal with the window of one day,
    and its end in _dlt_loads, in one transaction. A table that raw lacks is created, with
    RAW_MARK, and a column that a table lacks is added, as loader, which owns the raw schemas and
    whose tables transformer may read (postgres/warehouse.sql). The day is one on which the core
    holds no statistics of the accounts, the day before the period: so the run that reads the load
    writes no fact and takes nothing out of the core, as the load brings no statistics, and G1a
    finds no spend that fell. With pin, the rows of the load also go into core.loads as the pin of
    a run writes them (dbt/macros/pin_loads.sql), so that the views of staging read them before
    the run; its pin then passes them over."""
    ddl = []
    rows = []
    for platform, lists in LISTS.items():
        schema = f"raw_{platform}"
        listed = f"{ACCOUNTS} AND a.platform = '{platform}'"
        tables = [("_agoranomos_loads", JOURNAL)]
        tables += [
            (table, {n: t for n, (t, _) in columns.items()}) for table, columns in lists.values()
        ]
        for table, columns in tables:
            name = f"{schema}.{table}"
            declared = ", ".join(f"{column} {kind}" for column, kind in columns.items())
            ddl.append(
                f"DO $$ BEGIN IF to_regclass('{name}') IS NULL THEN "
                f"CREATE TABLE {name} ({declared}, _dlt_load_id character varying NOT NULL, "
                f"_dlt_id character varying NOT NULL UNIQUE); "
                f"COMMENT ON TABLE {name} IS '{RAW_MARK}'; END IF; END $$;"
            )
            added = ", ".join(f"ADD COLUMN IF NOT EXISTS {c} {k}" for c, k in columns.items())
            ddl.append(f"ALTER TABLE {name} {added};")
        ddl.append(
            f"DO $$ BEGIN IF to_regclass('{schema}._dlt_loads') IS NULL THEN "
            f"CREATE TABLE {schema}._dlt_loads ({DLT_LOADS}); "
            f"COMMENT ON TABLE {schema}._dlt_loads IS '{RAW_MARK}'; END IF; END $$;"
        )
        for kind, (table, columns) in lists.items():
            names = ", ".join(columns)
            values = ", ".join(sql for _, sql in columns.values())
            rows.append(
                f"INSERT INTO {schema}.{table} ({names}, _dlt_load_id, _dlt_id) "  # noqa: S608 (constants of this file)
                f"SELECT {values}, '{load_id}', md5(random()::text) "
                f"FROM {LIST_FROM[kind]} WHERE {listed};"
            )
        rows.append(
            f"INSERT INTO {schema}._agoranomos_loads (account_id, as_of, window_from, window_to, "  # noqa: S608 (constants of this file)
            "now, loaded_at, status, kind, _dlt_load_id, _dlt_id) "
            f"SELECT a.account_id, date '{day}', date '{day}', date '{day}', now(), now(), "
            f"'loaded', 'window', '{load_id}', md5(random()::text) "
            f"FROM core.dim_account AS a WHERE {listed};"
        )
        rows.append(
            f"INSERT INTO {schema}._dlt_loads (load_id, schema_name, status, inserted_at) "  # noqa: S608 (constants of this file)
            f"VALUES ('{load_id}', '{SCHEMA_NAME}', 0, now());"
        )
        if pin:
            rows.append(
                "INSERT INTO core.loads (source, load_id, account_id, window_from, window_to, "  # noqa: S608 (constants of this file)
                "outcome, finished_at, invocation_id, kind) "
                f"SELECT '{platform}', '{load_id}', a.account_id, date '{day}', date '{day}', "
                "'loaded', now(), gen_random_uuid(), 'window' "
                f"FROM core.dim_account AS a WHERE {listed} ON CONFLICT DO NOTHING;"
            )
    analyze = ", ".join(
        f"raw_{platform}.{table}"
        for platform, lists in LISTS.items()
        for table, _ in lists.values()
    )
    return "\n".join(
        [
            "BEGIN;",
            "SET LOCAL ROLE loader;",
            *ddl,
            "RESET ROLE;",
            *rows,
            "COMMIT;",
            f"ANALYZE core.loads, {analyze};",
        ]
    )


def list_rows(env: dict[str, str]) -> dict[str, int]:
    """The rows of each kind of list that a whole load writes, of all the platforms."""
    counts = {}
    for kind, source in LIST_FROM.items():
        counts[kind] = int(
            warehouse_output(env, f"SELECT count(*) FROM {source} WHERE {ACCOUNTS}")  # noqa: S608 (constants of this file)
        )
    return counts


ANALYZE_SQL = (
    "ANALYZE core.dim_client, core.dim_account, core.loads, core.dim_conversion_action, "
    "core.dim_campaign, core.dim_ad_group, core.dim_ad, core.fx_rates_daily, "
    + ", ".join(f"core.{fact}" for fact in FACTS)
)

# What the warehouse holds of the generator: the accounts with statistics, the rows of ads, the
# first and the last day, and the rows of ads of the last day.
STATUS_SQL = f"""
SELECT count(DISTINCT a.account_sk) FILTER (WHERE a.first_report_date IS NOT NULL),
    (SELECT count(*) FROM core.ad_stats_ad_daily AS f JOIN core.dim_account AS a
        ON a.account_sk = f.account_sk WHERE {ACCOUNTS}),
    min(a.first_report_date), max(a.last_report_date),
    (SELECT count(*) FROM core.ad_stats_ad_daily AS f JOIN core.dim_account AS a
        ON a.account_sk = f.account_sk WHERE {ACCOUNTS}
        AND f.report_date = (SELECT max(last_report_date) FROM core.dim_account AS a
            WHERE {ACCOUNTS}))
FROM core.dim_account AS a
WHERE {ACCOUNTS}
"""  # noqa: S608 (numbers, dates and constants of this file)


@dataclass(frozen=True)
class Present:
    """What the warehouse holds of the generator."""

    accounts: int
    ad_rows: int
    first: date | None
    last: date | None
    ad_rows_last_day: int


def status(env: dict[str, str]) -> Present:
    accounts, rows, first, last, last_day = warehouse_output(env, STATUS_SQL).split("|")
    return Present(
        int(accounts),
        int(rows),
        date.fromisoformat(first) if first else None,
        date.fromisoformat(last) if last else None,
        int(last_day),
    )


def generate(
    env: dict[str, str],
    profile: Profile,
    days: int | None = None,
    log: Callable[[str], None] = print,
    today: date | None = None,
) -> tuple[date, date]:
    """Write the data of a profile over a period of days that ends yesterday, and return its first
    and last day. The warehouse must hold none of it yet."""
    found = status(env)
    if found.accounts or found.ad_rows:
        raise RuntimeError("the warehouse already holds synthetic data: remove it first")
    first, last = period(days or profile.days, today)
    reporting = env.get("AGORANOMOS_REPORTING_CURRENCY") or "USD"
    log(
        f"{profile.name}: {profile.ad_rows_a_day:,} rows of ads a day from {first} to {last}, "
        f"{(last - first).days + 1} days"
    )
    started = time.monotonic()
    warehouse_output(env, registry_sql(profile, first, last, reporting))
    warehouse_output(env, partitions_sql(first, last))
    log(f"the registries, the entities and the rates in {time.monotonic() - started:.0f} s")
    for start, end in months(first, last):
        at = time.monotonic()
        warehouse_output(env, month_sql(start, end))
        log(f"{start:%Y-%m}: {(end - start).days + 1} days in {time.monotonic() - at:.0f} s")
    at = time.monotonic()
    warehouse_output(env, ANALYZE_SQL)
    log(f"ANALYZE in {time.monotonic() - at:.0f} s; {time.monotonic() - started:.0f} s in all")
    return first, last


def remove_sql() -> list[str]:
    """The statements that remove every row that the generator wrote, each in a transaction of its
    own, the deletes of the facts with the thaw of their transaction (ADR-0035), then the empty
    partitions that it made."""
    thaw = "SELECT set_config('agoranomos.thaw', pg_current_xact_id()::text, true) <> ''"
    synthetic = f"(SELECT a.account_sk FROM core.dim_account AS a WHERE {ACCOUNTS})"  # noqa: S608 (numbers, dates and constants of this file)
    facts = [
        f"BEGIN; {thaw}; DELETE FROM core.{fact} WHERE account_sk IN {synthetic}; COMMIT;"  # noqa: S608 (constants of this file)
        for fact in FACTS
    ]
    dimensions = [
        f"DELETE FROM core.{table} WHERE account_sk IN {synthetic};"  # noqa: S608 (constants of this file)
        for table in ("dim_ad", "dim_ad_group", "dim_campaign")
    ]
    registry = (
        "BEGIN; "  # noqa: S608 (numbers, dates and constants of this file)
        + " ".join(dimensions)
        + " DELETE FROM core.loads AS l USING core.dim_account AS a "  # noqa: S608 (numbers, dates and constants of this file)
        f"WHERE l.source = a.platform AND l.account_id = a.account_id AND {ACCOUNTS}; "
        f"DELETE FROM core.dim_account AS a WHERE {ACCOUNTS}; "
        "DELETE FROM core.dim_client WHERE client_id LIKE 'synthetic\\_%'; "
        "DELETE FROM core.dim_conversion_action WHERE conversion_action_id LIKE 'synthetic-%'; "
        "DELETE FROM core.fx_rates_daily WHERE rate_source = 'synthetic'; "
        "COMMIT;"
    )
    partitions = (
        "DO $$ DECLARE p regclass; empty boolean; BEGIN "  # noqa: S608 (numbers, dates and constants of this file)
        "FOR p IN SELECT c.oid::regclass FROM pg_class AS c "
        "JOIN pg_inherits AS i ON i.inhrelid = c.oid "
        "WHERE c.relnamespace = 'core'::regnamespace "
        f"AND obj_description(c.oid, 'pg_class') = '{MARK}' LOOP "
        "EXECUTE format('SELECT NOT EXISTS (SELECT FROM %s)', p) INTO empty; "
        "IF empty THEN EXECUTE format('DROP TABLE %s', p); END IF; "
        "END LOOP; END $$;"
    )
    schemas = ", ".join(f"'raw_{platform}'" for platform in LISTS)
    # The rows of the whole loads in raw, which a run that accepts them deletes, and their ends in
    # _dlt_loads; a table that whole_load_sql made stays, empty, as dlt adds what it lacks to a
    # table that exists, and a view of staging may read it.
    raw = (
        "DO $$ DECLARE s text; t text; BEGIN "  # noqa: S608 (constants of this file)
        f"FOREACH s IN ARRAY ARRAY[{schemas}] LOOP "
        "IF to_regclass(s || '._dlt_loads') IS NOT NULL THEN "
        "FOR t IN SELECT table_name FROM information_schema.columns "
        "WHERE table_schema = s AND column_name = '_dlt_load_id' LOOP "
        "EXECUTE format('DELETE FROM %I.%I WHERE _dlt_load_id IN "
        "(SELECT load_id FROM %I._dlt_loads WHERE schema_name = %L)', "
        f"s, t, s, '{SCHEMA_NAME}'); "
        "END LOOP; "
        f"EXECUTE format('DELETE FROM %I._dlt_loads WHERE schema_name = %L', s, '{SCHEMA_NAME}'); "
        "END IF; END LOOP; END $$;"
    )
    return [*facts, raw, registry, partitions]


def remove(env: dict[str, str], log: Callable[[str], None] = print) -> None:
    started = time.monotonic()
    for statement in remove_sql():
        warehouse_output(env, statement)
    log(f"removed the synthetic data in {time.monotonic() - started:.0f} s")


def restore_accounts(env: dict[str, str]) -> None:
    """Give the synthetic accounts back their clients and their activity, which a run of the
    transform takes away, as the settings of the model name none of them (dim_account.sql)."""
    warehouse_output(
        env,
        "UPDATE core.dim_account AS a SET client_id = 'synthetic_' || "  # noqa: S608 (numbers, dates and constants of this file)
        f"lpad(((substr(a.account_id, 11)::integer - 1) % {CLIENTS} + 1)::text, 2, '0'), "
        f"is_active = true WHERE {ACCOUNTS}",
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    make = commands.add_parser("generate", help="write the data of a profile")
    make.add_argument("profile", choices=sorted(PROFILES))
    make.add_argument("--days", type=int, help="the days of the period, which ends yesterday")
    commands.add_parser("status", help="what the warehouse holds of the generator")
    commands.add_parser("remove", help="remove everything that the generator wrote")
    args = parser.parse_args(argv)
    env = load_env(REPO_ROOT / ".env")
    if args.command == "generate":
        generate(env, PROFILES[args.profile], args.days)
    elif args.command == "remove":
        remove(env)
    found = status(env)
    print(
        f"{found.accounts} accounts with statistics, {found.ad_rows:,} rows of ads from "
        f"{found.first} to {found.last}, {found.ad_rows_last_day:,} on the last day"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
