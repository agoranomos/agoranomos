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
four ad platforms of the seed of the base package, and over 40 clients; one in five reports in EUR,
the others in USD, with a rate for each day into the reporting currency of .env. The numbers come
from the IDs and the day, so the same profile and period give the same rows.

Everything that it writes is marked: the IDs of the clients start with synthetic_, those of the
accounts and of the conversion actions with synthetic-, the rates have the source synthetic, and a
partition that it creates carries the comment MARK. remove() deletes all of it, in a transaction
that thaws the frozen months for itself (ADR-0035), and drops the partitions that it created and
that are then empty. The loads of the accounts are accepted, so that no run of the transform
replaces their days: a run reads the pinned loads only (dbt/macros/newest_load.sql). A run of the
transform still gives every account the client that the settings of the model name, which none of
these accounts has, so restore_accounts() puts their clients back after one.

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
PLATFORMS = ("google_ads", "demo_search", "demo_social", "demo_video")
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
    CASE WHEN n % 5 = 0 THEN 'EUR' ELSE 'USD' END, 'UTC', 'ENABLED', true, false,
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
-- A rate for each day of the period and each currency of the accounts into the reporting currency,
-- where none is there.
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
COMMIT;
"""  # noqa: S608 (numbers, dates and constants of this file)


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
    return [*facts, registry, partitions]


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
