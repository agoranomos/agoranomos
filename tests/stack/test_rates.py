"""The rates into the reporting currency.

The core holds core.fx_rates_daily (ADR-0042): a row for each calendar day, each currency of the
accounts and the reporting currency, through the euro with no rounding, with the last published
rate carried over a day without one and flagged; the view ops.ops_rates says how old the last rate
is, and the views of the marts convert an amount by a join with it at the rate of the day of its
row (ADR-0037). The checks of a run that reads only the last window of the rates, of one that
brings a later day of the facts, and of another reporting currency compute the rows that the model
would write from the SQL that dbt compiles, in a transaction that ends in ROLLBACK, so the core is
as before. They read the rates that staging reads: a run that succeeds accepts its loads and
deletes their rows from raw (ADR-0046), so the module first loads the last 60 days of the rates of
the demo again, as of their newest load, which gives the same rates, and runs the transform with
its acceptance skipped, which leaves the load pinned and its rows in raw. The next run accepts it.

The last test changes the reporting currency to GBP, which no account of the demo has, after a run
that accepted every load of the rates and cleaned raw: marts-init in GBP, the load of the rates of
the demo as its task makes it in GBP, a run of the transform that pins that load and keeps it, and
dbt in GBP by hand as a run in GBP would. It then deletes the rows of GBP from core.fx_rates_daily
and builds ops.ops_rates and the views of the marts again in the reporting currency of the stack.
If it is killed before that, run DELETE FROM core.fx_rates_daily WHERE reporting_currency = 'GBP'
as postgres, dbt run --select ops_rates in the container of the tasks, and marts-init.

The order of the sources of the rates (ADR-0042, C2c of #222) is checked on the recorded answers of
the Bank of Italy and of the National Bank of Kazakhstan that the unit tests of their loads read:
the code of each load parses them in the container of the tasks, with the HTTP of its client
replaced, and one transaction as postgres, which ends in ROLLBACK, puts them in stand-ins of the raw
tables under the staging views of the two banks and of the ECB, as the compiled staging models
read them, with their days in stg_loads__newest_days and their loads in core.loads, and reads the
rows that the model of the rates would merge in EUR and in KZT. No request reaches a bank, no load
of dlt writes a table, and the core is as before.

The assertions print counts, days and codes of currencies, never a secret.
"""

import json
import re
import subprocess
from datetime import date, timedelta
from decimal import Decimal

import pytest
from agoranomos_pipelines import rates
from agoranomos_pipelines.demo import platforms
from airflow_api import is_busy, trigger, unpause, wait_for_dag, wait_for_run
from raw_loads import last_moment, load_platform, run_in_tasks, transform_keeping_raw
from stack_helpers import (
    REPO_ROOT,
    active_variant,
    compose,
    job_failed,
    wait_for,
    warehouse_output,
)

pytestmark = [
    pytest.mark.demo,
    pytest.mark.usefixtures("daily_load_paused"),
    pytest.mark.transform,
]

TRANSFORM = "agoranomos_demo_transform"
# dbt in the container of the tasks, which has the password of transformer, with its target and
# logs folders outside the project, which is mounted read-only (CONTRIBUTING.md).
DBT = "/opt/airflow/dbt_venv/bin/dbt"
PROJECT = "/opt/agoranomos/dbt"
WORK = "/tmp/agoranomos-stack-test-rates"  # noqa: S108 (a folder in the container, not on this host)


@pytest.fixture(scope="module")
def built(env, airflow_url, token):
    """The rates of the core and the views of the marts as a run of the transform left them that
    read a new load of the last 60 days of the rates of the demo, and kept it pinned, with its rows
    in raw."""
    wait_for_dag(airflow_url, token, TRANSFORM)
    wait_for(
        lambda: not is_busy(airflow_url, token, TRANSFORM, assets=len(platforms.PLATFORMS)),
        600,
        interval=5,
        message="the runs of the transform that earlier tests started did not end",
    )
    found = warehouse_output(env, "SELECT to_regclass('marts.fx_rates_daily') IS NOT NULL")
    assert found == "t", "marts.fx_rates_daily does not exist: the transform has not run here"
    now = last_moment(env, "demo_fx")
    load_platform(env, "demo_fx", now, (now - timedelta(days=60)).date())
    transform_keeping_raw(airflow_url, token)


def test_the_view_of_the_rates_shows_the_rates_of_the_core_into_the_reporting_currency(env, built):
    # The view holds the reporting currency of the deployment alone, the core those of before too,
    # and the rate to eight decimal places, as the view of v0.4 had it.
    currency = env.get("AGORANOMOS_REPORTING_CURRENCY") or "USD"
    differ = warehouse_output(
        env,
        "SELECT (SELECT count(*) FROM (SELECT rate_date, currency::text, reporting_currency::text, "  # noqa: S608 (a code of .env)
        "round(rate_into_reporting_currency, 8), rate_source, cross_source, is_carried_forward "
        f"FROM core.fx_rates_daily WHERE reporting_currency = '{currency}' EXCEPT SELECT * "
        "FROM marts.fx_rates_daily) AS c), (SELECT count(*) FROM (SELECT * FROM "
        "marts.fx_rates_daily EXCEPT SELECT rate_date, currency::text, reporting_currency::text, "
        "round(rate_into_reporting_currency, 8), rate_source, cross_source, is_carried_forward "
        f"FROM core.fx_rates_daily WHERE reporting_currency = '{currency}') AS m), "
        "(SELECT count(DISTINCT reporting_currency) FROM marts.fx_rates_daily)",
    )
    assert differ == "0|0|1", differ


# The rates of the core (ADR-0042), the model that writes them, and the file of its compiled SQL
# under the target folder of a call of dbt.
CORE_RATES = "core.fx_rates_daily"
CORE_MODEL = "core_fx_rates_daily"
CORE_COMPILED = "target/compiled/agoranomos/models/core/core_fx_rates_daily.sql"
# A day on which the demo publishes no rate: a weekend or one of four closing days of TARGET
# (agoranomos_pipelines/demo/fx.py).
UNPUBLISHED = (
    "(extract(isodow FROM rate_date) >= 6 "
    "OR to_char(rate_date, 'MM-DD') IN ('01-01', '05-01', '12-25', '12-26'))"
)
# The rates of the demo as staging reads them, with the euro at 1 on each day of publication.
PUBLISHED_RATES = (
    "SELECT rate_date, quote_currency AS currency, rate FROM staging.stg_demo_fx__rates_daily "
    "UNION ALL SELECT DISTINCT rate_date, base_currency, 1 FROM staging.stg_demo_fx__rates_daily"
)
# The days of the demo for which a run reads the loads of its rates, which a check takes from the
# table of the run, in a transaction, as a run reads when the core has accepted the older loads.
RATE_DAYS = "staging.stg_loads__newest_days WHERE source = 'demo_fx'"


def reporting(env):
    """The reporting currency of the stack, USD unless .env says otherwise."""
    return env.get("AGORANOMOS_REPORTING_CURRENCY") or "USD"


def other_currency(env):
    """The currency of the demo that is not the reporting currency: the demo has accounts in US
    dollars and in euros."""
    return "EUR" if reporting(env) == "USD" else "USD"


def task_service():
    return "airflow-worker" if active_variant() == "celery" else "airflow-scheduler"


def table(env, sql, role="postgres"):
    """The rows of a query, each as a list of texts, a NULL as an empty text."""
    return [line.split("|") for line in warehouse_output(env, sql, role).splitlines() if line]


def written(output):
    """The rows of the check that a transaction prints between its tags, such as BEGIN, DELETE 3
    and ROLLBACK: each starts with written|, which no tag does."""
    return [line.split("|")[1:] for line in output.splitlines() if line.startswith("written|")]


def compiled_rates(name, currency=None, dbt_vars=None):
    """The SQL of the model of the rates of the core, as dbt compiles it in the container of the
    tasks, with another reporting currency or vars of dbt when they are given: the query of the
    rows that the model would merge, which reads the core and writes nothing."""
    work = f"{WORK}-{name}"
    service = task_service()
    command = ["exec", "-T"]
    if currency:
        command += ["-e", f"AGORANOMOS_REPORTING_CURRENCY={currency}"]
    command += [service, DBT, "--quiet", "--no-use-colors", "compile", "--select", CORE_MODEL]
    command += ["--project-dir", PROJECT, "--profiles-dir", PROJECT]
    command += ["--target-path", f"{work}/target", "--log-path", f"{work}/logs"]
    if dbt_vars:
        command += ["--vars", dbt_vars]
    compose(*command)
    return compose("exec", "-T", service, "cat", f"{work}/{CORE_COMPILED}").stdout


@pytest.fixture(scope="module")
def core_rates(env, built):
    """The rates of the core as the last run of the transform wrote them."""
    held = warehouse_output(
        env,
        f"SELECT count(*) FROM {CORE_RATES} WHERE reporting_currency = '{reporting(env)}'",  # noqa: S608 (constants of this file and a code of .env)
    )
    assert int(held) > 0, f"{CORE_RATES} holds no rate into the reporting currency"


@pytest.fixture(scope="module")
def core_rates_sql(core_rates):
    return compiled_rates("core")


def test_the_core_has_a_rate_for_every_day_of_each_currency_of_the_accounts(env, core_rates):
    # ADR-0042: every calendar day from the first rate of a currency to the last day of the table,
    # with no gap, and each day of the facts of the core with a rate of the currency of its
    # account, as the test of G3 checks for the days of the windows of a run: once the run has
    # accepted its loads, it has no window left (ADR-0046).
    currency = reporting(env)
    rates = f"SELECT * FROM {CORE_RATES} WHERE reporting_currency = '{currency}'"  # noqa: S608 (constants of this file and a code of .env)
    spans = table(
        env,
        f"WITH r AS ({rates}) "  # noqa: S608 (constants of this file)
        "SELECT currency, count(*), max(rate_date) - min(rate_date) + 1, "
        "max(rate_date) = (SELECT max(rate_date) FROM r) FROM r GROUP BY currency ORDER BY 1",
    )
    accounts = warehouse_output(
        env, "SELECT string_agg(DISTINCT currency, ',') FROM core.dim_account"
    )
    assert {row[0] for row in spans} == {*accounts.split(","), currency}, spans
    assert all(row[1] == row[2] and row[3] == "t" for row in spans), spans
    missing = warehouse_output(
        env,
        f"WITH r AS ({rates}) SELECT count(*) FROM core.ad_stats_campaign_daily AS f "  # noqa: S608 (constants of this file)
        "JOIN core.dim_account AS a USING (account_sk) "
        "WHERE NOT EXISTS (SELECT FROM r WHERE r.rate_date = f.report_date "
        "AND r.currency = a.currency)",
    )
    assert int(missing) == 0, missing


def test_the_reporting_currency_has_the_rate_1_on_every_day_with_no_source(env, core_rates):
    currency = reporting(env)
    [[days, other]] = table(
        env,
        "SELECT count(*), count(*) FILTER (WHERE rate_into_reporting_currency <> 1 "  # noqa: S608 (constants of this file and a code of .env)
        "OR rate_source <> 'identity' OR base_currency IS NOT NULL OR base_rate IS NOT NULL "
        f"OR published_on IS NOT NULL OR is_carried_forward) FROM {CORE_RATES} "
        f"WHERE reporting_currency = '{currency}' AND currency = '{currency}'",
    )
    assert (int(days) > 0, other) == (True, "0"), (days, other)


def test_a_day_without_a_publication_carries_the_last_rate_with_its_flag(env, core_rates):
    # A weekend or a holiday takes the rate of the last day of publication before it, with that
    # day in published_on and is_carried_forward; a day of publication takes its own.
    currency, other = reporting(env), other_currency(env)
    [counts] = table(
        env,
        f"WITH r AS (SELECT * FROM {CORE_RATES} WHERE reporting_currency = '{currency}' "  # noqa: S608 (constants of this file and codes of .env)
        f"AND currency = '{other}'), "
        "published AS (SELECT * FROM r WHERE NOT is_carried_forward) "
        "SELECT "
        f"count(*) FILTER (WHERE NOT {UNPUBLISHED} "
        "AND rate_date <= (SELECT max(rate_date) FROM published) "
        "AND (is_carried_forward OR published_on <> rate_date)), "
        f"count(*) FILTER (WHERE {UNPUBLISHED} AND NOT is_carried_forward), "
        "count(*) FILTER (WHERE is_carried_forward), "
        "count(*) FILTER (WHERE is_carried_forward AND NOT EXISTS (SELECT FROM published AS p "
        "WHERE p.rate_date = r.published_on "
        "AND p.rate_into_reporting_currency = r.rate_into_reporting_currency "
        "AND p.base_rate = r.base_rate AND p.rate_source = r.rate_source)), "
        "count(*) FILTER (WHERE is_carried_forward AND published_on <> (SELECT max(p.rate_date) "
        "FROM published AS p WHERE p.rate_date < r.rate_date)) "
        "FROM r",
    )
    weekdays_carried, holidays_published, carried, unlike, not_last = map(int, counts)
    assert (weekdays_carried, holidays_published) == (0, 0), counts
    assert carried > 0, "the demo has weekends"
    assert (unlike, not_last) == (0, 0), counts


def test_the_rate_goes_through_the_euro_with_no_rounding(env, core_rates):
    # ADR-0042 and Regulation (EC) 1103/97: one unit of a currency is worth the rate of the
    # reporting currency over the rate of the currency, both against the euro as the source
    # published them, with no rounding on the way, and the row keeps the rate of the currency. On
    # the days of the rates that staging reads: raw holds no older load, as the core accepted it.
    currency, other = reporting(env), other_currency(env)
    [counts] = table(
        env,
        f"WITH published AS ({PUBLISHED_RATES}), "  # noqa: S608 (constants of this file and codes of .env)
        f"r AS (SELECT * FROM {CORE_RATES} WHERE reporting_currency = '{currency}' "
        f"AND currency = '{other}' AND NOT is_carried_forward "
        "AND rate_date IN (SELECT rate_date FROM published)) "
        "SELECT count(*), "
        "count(*) FILTER (WHERE r.rate_into_reporting_currency <> pr.rate / pc.rate "
        "OR r.base_rate <> pc.rate OR r.base_currency <> 'EUR' OR r.rate_source <> 'demo_fx'), "
        "count(*) FILTER (WHERE pc.rate IS NULL OR pr.rate IS NULL) "
        "FROM r "
        "LEFT JOIN published AS pc ON pc.rate_date = r.rate_date AND pc.currency = r.currency "
        "LEFT JOIN published AS pr "
        "ON pr.rate_date = r.rate_date AND pr.currency = r.reporting_currency",
    )
    rows, wrong, unread = map(int, counts)
    assert rows > 0, f"no published rate of {other} into {currency}"
    assert (wrong, unread) == (0, 0), counts


def test_ops_says_how_old_the_last_rate_is(env, core_rates):
    # ADR-0042: the business days, Monday to Friday, after the day of the last rate up to the day
    # of the warehouse, and a warning after 3, which metabase_reader may read.
    currency = reporting(env)
    [row] = table(
        env,
        "SELECT reporting_currency, rate_source, last_rate_date, business_days_since_rate, "
        "is_late FROM ops.ops_rates",
        "metabase_reader",
    )
    last = warehouse_output(
        env,
        f"SELECT max(published_on) FROM {CORE_RATES} WHERE reporting_currency = '{currency}' "  # noqa: S608 (constants of this file and a code of .env)
        "AND rate_source <> 'identity'",
    )
    today = date.fromisoformat(warehouse_output(env, "SELECT current_date"))
    day = date.fromisoformat(last) + timedelta(days=1)
    business = 0
    while day <= today:
        business += day.weekday() < 5
        day += timedelta(days=1)
    assert row == [currency, "demo_fx", last, str(business), "t" if business > 3 else "f"], row


def test_status_says_how_old_the_last_rate_is(env, core_rates):
    result = subprocess.run(
        [str(REPO_ROOT / "agoranomos"), "status"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    assert result.returncode == 0, f"status exited with {result.returncode}"
    [row] = table(env, "SELECT rate_source, reporting_currency, last_rate_date FROM ops.ops_rates")
    line = rf"Rates: the last rate of {row[0]} into {row[1]} is of {row[2]}, \d+ business days? ago"
    assert re.search(rf"^{line} \(ADR-0042\)\.$", result.stdout, re.MULTILINE), "no line of rates"


def test_a_run_that_reads_only_the_last_window_of_the_rates_keeps_the_days_before_it(
    env, core_rates_sql
):
    # As once the core accepts the loads and the raw layer keeps only those that it has not
    # (ADR-0035): the run reads the last 14 days of the rates, and carries the rates that the core
    # holds into the first of them, so the model would write no row.
    output = warehouse_output(
        env,
        f"BEGIN; DELETE FROM {RATE_DAYS} AND day < (SELECT max(day) - 13 FROM {RATE_DAYS}); "  # noqa: S608 (constants of this file)
        f"SELECT 'written|' || count(*) FROM ({core_rates_sql}) AS w; ROLLBACK",
    )
    assert written(output) == [["0"]], output.splitlines()[-3:]
    assert int(warehouse_output(env, f"SELECT count(*) FROM {RATE_DAYS}")) > 14  # noqa: S608 (constants of this file)


def test_a_later_day_of_the_facts_takes_the_last_rate_that_the_core_holds(env, core_rates_sql):
    # A run that reads no load of the rates and writes a day of the facts after the last day of the
    # rates: the model carries the last rate that the core holds as published to it, with the flag,
    # and gives the reporting currency the rate 1, as the raw layer may have no rate left.
    currency, other = reporting(env), other_currency(env)
    [[last_day, last_published, rate]] = table(
        env,
        f"SELECT (SELECT max(rate_date) FROM {CORE_RATES} WHERE reporting_currency = "  # noqa: S608 (constants of this file and codes of .env)
        f"'{currency}'), r.rate_date, r.rate_into_reporting_currency FROM {CORE_RATES} AS r "
        f"WHERE r.reporting_currency = '{currency}' AND r.currency = '{other}' "
        "AND NOT r.is_carried_forward ORDER BY r.rate_date DESC LIMIT 1",
    )
    days = [date.fromisoformat(last_day) + timedelta(days=n) for n in (1, 2, 3)]
    output = warehouse_output(
        env,
        f"BEGIN; DELETE FROM {RATE_DAYS}; "  # noqa: S608 (constants of this file and codes of .env)
        "INSERT INTO staging.int_window_days "
        "(source, account_id, account_sk, currency, report_date, load_sk, day_key) "
        f"SELECT platform, account_id, account_sk, currency, date '{days[-1]}', 0, "
        f"platform || '|' || account_id || '|' || (date '{days[-1]}' - date '2000-01-01')::text "
        f"FROM core.dim_account WHERE currency = '{other}' LIMIT 1; "
        "SELECT 'written|' || rate_date || '|' || currency || '|' || rate_into_reporting_currency "
        "|| '|' || coalesce(published_on::text, '') || '|' || is_carried_forward || '|' "
        f"|| rate_source FROM ({core_rates_sql}) AS w ORDER BY currency <> '{currency}', "
        "rate_date; ROLLBACK",
    )
    expected = [[str(day), currency, "1", "", "false", "identity"] for day in days] + [
        [str(day), other, rate, last_published, "true", "demo_fx"] for day in days
    ]
    assert written(output) == expected, output.splitlines()[-8:]


def test_another_reporting_currency_takes_a_rate_for_every_day(env, core_rates):
    # After a change of AGORANOMOS_REPORTING_CURRENCY the next run computes every day for the new
    # one, of which the table holds no row: the reporting currency of the stack becomes a currency
    # like another, with the inverse rate, the same days of publication and the same flags.
    currency, other = reporting(env), other_currency(env)
    sql = compiled_rates("other", currency=other)
    [counts] = table(
        env,
        f"WITH w AS ({sql}), "  # noqa: S608 (constants of this file, codes of .env and the SQL of dbt)
        "span AS (SELECT min(rate_date) AS first_day, max(rate_date) AS last_day FROM w), "
        f"r AS (SELECT * FROM {CORE_RATES} WHERE reporting_currency = '{currency}' "
        f"AND currency = '{other}' "
        "AND rate_date BETWEEN (SELECT first_day FROM span) AND (SELECT last_day FROM span)) "
        "SELECT "
        f"(SELECT count(*) FROM w WHERE reporting_currency <> '{other}'), "
        f"(SELECT count(*) FROM w WHERE currency = '{other}' AND rate_source = 'identity' "
        "AND rate_into_reporting_currency = 1), "
        "(SELECT last_day - first_day + 1 FROM span), "
        "(SELECT count(*) FROM r), "
        "(SELECT count(*) FROM r JOIN w "
        f"ON w.rate_date = r.rate_date AND w.currency = '{currency}' "
        "WHERE abs(w.rate_into_reporting_currency * r.rate_into_reporting_currency - 1) < 1e-12 "
        "AND w.published_on = r.published_on AND w.is_carried_forward = r.is_carried_forward)",
    )
    elsewhere, identity, days, rated, inverse = map(int, counts)
    assert elsewhere == 0, counts
    assert identity == days > 0, counts
    assert inverse == rated > 0, counts


def test_a_recompute_of_every_day_writes_what_the_core_holds(env, core_rates):
    # The var recompute_rates computes every day again for the reporting currency of .env, as up
    # is to after a change of it: on a table that the last run wrote, no row differs.
    sql = compiled_rates("recompute", dbt_vars="{recompute_rates: true}")
    assert "when true\n" in sql, "the var did not reach the SQL"
    assert warehouse_output(env, f"SELECT count(*) FROM ({sql}) AS w") == "0"  # noqa: S608 (the SQL of dbt)


# A reporting currency that is neither a currency of an account of the demo nor that of the stack,
# whose rates the demo publishes (agoranomos_pipelines/demo/fx.py).
NEW_CURRENCY = "GBP"
# The daily load of a platform of the demo as its task makes it, in the reporting currency of the
# environment, as of a moment.
LOAD_DAILY = """
import sys
from datetime import datetime

from agoranomos_pipelines.demo import sources

sources.load_daily(sys.argv[2], datetime.fromisoformat(sys.argv[1]))
"""
# The newest whole load of the rates of the demo, as its journal records it.
NEWEST_RATES = (
    "SELECT j._dlt_load_id, j.window_from, j.window_to, j.kind "
    "FROM raw_demo_fx._agoranomos_loads AS j "
    "JOIN raw_demo_fx._dlt_loads AS l ON l.load_id = j._dlt_load_id AND l.status = 0 "
    "ORDER BY j._dlt_load_id::numeric DESC LIMIT 1"
)
# The facts of the migrations, whose days ops.ops_rates reads.
FACTS = (
    "ad_stats_campaign_daily",
    "ad_stats_ad_group_daily",
    "ad_stats_ad_daily",
    "ad_conversions_campaign_daily",
    "web_sessions_daily",
)
# The first day of the facts of the core, of every account.
FIRST_FACT = "SELECT least({})".format(
    ", ".join(f"(SELECT min(report_date) FROM core.{fact})" for fact in FACTS)  # noqa: S608
)
# The rows of the facts of the campaigns whose day has no rate of the currency of their account
# into a reporting currency, {currency}.
UNRATED_FACTS = (
    "SELECT count(*) FROM core.ad_stats_campaign_daily AS f "
    "JOIN core.dim_account AS a USING (account_sk) "
    "WHERE a.currency <> '{currency}' AND NOT EXISTS (SELECT FROM core.fx_rates_daily AS r "
    "WHERE r.rate_date = f.report_date AND r.currency = a.currency "
    "AND r.reporting_currency = '{currency}')"
)
GAPS = (
    "SELECT reporting_currency, coalesce(first_day_without_rate::text, ''), days_without_rate "
    "FROM ops.ops_rates"
)


def dbt_run(name, currency, *nodes):
    """dbt run of some nodes in the container of the tasks, in a reporting currency, as transformer,
    as a run of the transform in that currency would build them."""
    work = f"{WORK}-{name}"
    command = ["exec", "-T", "-e", f"AGORANOMOS_REPORTING_CURRENCY={currency}", task_service()]
    command += [DBT, "--quiet", "--no-use-colors", "run", "--select", *nodes]
    command += ["--project-dir", PROJECT, "--profiles-dir", PROJECT]
    command += ["--target-path", f"{work}/target", "--log-path", f"{work}/logs"]
    result = compose(*command, check=False)
    assert result.returncode == 0, f"dbt run of {nodes} in {currency} exited {result.returncode}"


def marts_init(currency=None):
    """The job marts-init, in another reporting currency when one is given, and what it printed."""
    command = ["run", "--rm", "--no-deps", "-T"]
    if currency:
        command += ["-e", f"AGORANOMOS_REPORTING_CURRENCY={currency}"]
    result = compose(*command, "marts-init", check=False)
    assert result.returncode == 0, job_failed("marts-init", result)
    return result.stdout


@pytest.fixture(scope="module")
def cleaned(env, airflow_url, token, built):
    """The rates after a run of the transform that accepted every load that it read and deleted
    their rows from raw (ADR-0046), as on a deployment some days after its first start: raw holds no
    row of a load of the rates that the core has accepted."""
    unpause(airflow_url, token, TRANSFORM)
    run = trigger(airflow_url, token, TRANSFORM)
    run = wait_for_run(airflow_url, token, TRANSFORM, run["dag_run_id"], timeout=1800)
    assert run["state"] == "success", run["state"]
    left = warehouse_output(
        env,
        "SELECT count(*) FILTER (WHERE l.status = 'accepted') || '|' || count(*) "
        "FROM raw_demo_fx.rates_daily AS r LEFT JOIN core.loads AS l "
        "ON l.source = 'demo_fx' AND l.load_id = r._dlt_load_id",
    )
    assert left.split("|")[0] == "0", left


def test_a_new_reporting_currency_takes_the_history_of_the_rates_again(
    env, airflow_url, token, cleaned
):
    # ADR-0042 and the decision of 12 November 2026: a reporting currency that the core cannot give
    # from its own rows, once the cleanup has deleted the loads of the rates that it accepted, takes
    # the history of the rates again from the source, and a rate for every day of the facts.
    currency = reporting(env)
    accounts = warehouse_output(
        env, "SELECT string_agg(DISTINCT currency, ',') FROM core.dim_account"
    ).split(",")
    assert currency != NEW_CURRENCY, currency
    assert NEW_CURRENCY not in accounts, accounts
    held = warehouse_output(
        env,
        f"SELECT count(*) FROM {CORE_RATES} WHERE reporting_currency = '{NEW_CURRENCY}'",  # noqa: S608 (constants of this file)
    )
    assert held == "0", f"{CORE_RATES} holds rates into {NEW_CURRENCY}: see the docstring"
    first_fact = date.fromisoformat(warehouse_output(env, FIRST_FACT))
    try:
        # The up after the change: marts-init computes the rates in GBP from what the core holds,
        # which gives none of the older days, builds ops.ops_rates in GBP, and warns.
        output = marts_init(NEW_CURRENCY)
        assert f"marts-init: warning: the core holds no rate into {NEW_CURRENCY} on " in output
        [[shown, first_without, days]] = table(env, GAPS)
        assert (shown, first_without) == (NEW_CURRENCY, str(first_fact)), (shown, first_without)
        assert int(days) > 0
        assert int(warehouse_output(env, UNRATED_FACTS.format(currency=NEW_CURRENCY))) > 0
        # The views of the marts go back to the currency of the stack at once, so that the run of
        # the transform below, in that currency, tests them as they are. marts-init builds every
        # view of ops again with them, ops.ops_rates in the currency of the stack too, so a run in
        # GBP builds it again in GBP, as the start in GBP left it for the load below.
        marts_init()
        dbt_run("gaps", NEW_CURRENCY, "ops_rates")
        [[shown, first_without, _]] = table(env, GAPS)
        assert (shown, first_without) == (NEW_CURRENCY, str(first_fact)), (shown, first_without)
        # The next load of the rates of the demo, as its task makes it in GBP, reads ops.ops_rates
        # as transformer and takes the history again, from a week before the first day without a
        # rate or from the first day of the table, with the kind history.
        first_day = date.fromisoformat(
            warehouse_output(env, f"SELECT min(rate_date) FROM {CORE_RATES}")  # noqa: S608 (constants of this file)
        )
        start = min(first_fact - timedelta(days=rates.LEAD_DAYS), first_day)
        now = last_moment(env, "demo_fx")
        run_in_tasks(
            env,
            LOAD_DAILY,
            now.isoformat(),
            "demo_fx",
            environment={"AGORANOMOS_REPORTING_CURRENCY": NEW_CURRENCY},
        )
        [[load_id, window_from, window_to, kind]] = table(env, NEWEST_RATES)
        assert (window_from, kind) == (str(start), "history"), (window_from, kind)
        # The transform pins it, with its kind, and reads it; the acceptance is skipped, so its
        # rows stay in raw for the run in GBP below, as for a run in GBP itself.
        transform_keeping_raw(airflow_url, token)
        [pinned] = table(
            env,
            "SELECT kind, status, window_from, window_to FROM core.loads "  # noqa: S608 (an ID of dlt)
            f"WHERE source = 'demo_fx' AND load_id = '{load_id}'",
        )
        assert pinned == ["history", "pinned", window_from, window_to], pinned
        # A run in GBP: every day of the facts has a rate into GBP, and Ops names none without.
        dbt_run("history", NEW_CURRENCY, CORE_MODEL, "ops_rates")
        assert table(env, GAPS) == [[NEW_CURRENCY, "", "0"]]
        assert warehouse_output(env, UNRATED_FACTS.format(currency=NEW_CURRENCY)) == "0"
        # The rates of the source on the days that it published, through the euro, as for the
        # reporting currency of the stack.
        [[rows, wrong]] = table(
            env,
            f"WITH published AS ({PUBLISHED_RATES}) "  # noqa: S608 (constants of this file)
            "SELECT count(*), count(*) FILTER (WHERE r.rate_into_reporting_currency "
            "<> pr.rate / pc.rate OR r.rate_source <> 'demo_fx') "
            f"FROM {CORE_RATES} AS r JOIN published AS pc "
            "ON pc.rate_date = r.rate_date AND pc.currency = r.currency "
            "JOIN published AS pr ON pr.rate_date = r.rate_date "
            f"AND pr.currency = '{NEW_CURRENCY}' "
            f"WHERE r.reporting_currency = '{NEW_CURRENCY}' AND NOT r.is_carried_forward "
            f"AND r.currency <> '{NEW_CURRENCY}'",
        )
        assert int(rows) > 0, rows
        assert wrong == "0", (rows, wrong)
    finally:
        warehouse_output(
            env,
            f"DELETE FROM {CORE_RATES} WHERE reporting_currency = '{NEW_CURRENCY}'",  # noqa: S608 (constants of this file)
        )
        dbt_run("back", currency, "ops_rates")
        marts_init()
    [[shown, _, _]] = table(env, GAPS)
    assert shown == currency


# The order of the sources of the rates (ADR-0042): the recorded answers of the two banks, the
# days of their loads, and the accounts of the check, whose currencies the model computes. The
# answer of the Bank of Italy of Thursday 12 November 2026 stands for Friday 6 November too, as
# the fixtures hold no other weekday; Monday 9 to Wednesday 11 November it quotes nothing, as on a
# day that its load missed, and the ECB has Tuesday 10 November.
FIXTURES = REPO_ROOT / "tests" / "unit" / "fixtures"
ITALY = FIXTURES / "bank_of_italy" / "daily_rates_2026-11-12.json"
ITALY_EMPTY = FIXTURES / "bank_of_italy" / "daily_rates_2026-11-08.json"
NBK_DAYS = ("2026-11-06", "2026-11-07", "2026-11-08", "2026-11-09", "2026-11-12")
# The currencies of the check: RUB, which the Bank of Italy gives as N.A., ARS, which the NBK
# lacks, KPW, which no source has, and the others that both have.
CHAIN_CURRENCIES = ("RUB", "ARS", "KPW", "KZT", "GBP", "USD", "EUR")
# The script that parses the answers through the code of the loads, in the container of the
# tasks: argv holds pairs of a source and a day, and the answer of that day. It prints the rows
# that each load would give dlt, the fields of the Bank of Italy in the snake_case of dlt, of the
# currencies of the check.
PARSE = """
import json
import re
import sys
from datetime import date

from agoranomos_pipelines import bank_of_italy, nbk

answers = {}
for key, body in zip(sys.argv[2::2], sys.argv[3::2]):
    source, day = key.split(":")
    answers.setdefault(source, {})[day] = body
kept = set(sys.argv[1].split(","))


class Answer:
    def __init__(self, body):
        self.body = body

    def json(self):
        return json.loads(self.body)

    @property
    def content(self):
        return self.body.encode("utf-8")


class Client:
    def __init__(self, **settings):
        pass

    def get(self, url, params=None, headers=None):
        if url == bank_of_italy.URL:
            return Answer(answers["bank_of_italy"][params["referenceDate"]])
        assert url == nbk.URL, url
        day, month, year = params["fdate"].split(".")
        return Answer(answers["nbk"][f"{year}-{month}-{day}"])


bank_of_italy.Client = Client
bank_of_italy.sleep = lambda seconds: None
nbk.Client = Client
nbk.time.sleep = lambda seconds: None


def snake(name):
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


italy = [
    {snake(key): value for key, value in row.items()}
    for row in bank_of_italy.rows(date(2026, 11, 6), date(2026, 11, 12))
]
kazakhstan = [
    *nbk.rows(date(2026, 11, 6), date(2026, 11, 9)),
    *nbk.rows(date(2026, 11, 12), date(2026, 11, 12)),
]
print(json.dumps({
    "bank_of_italy": [row for row in italy if row["iso_code"] in kept],
    "nbk": [row for row in kazakhstan if row["title"] in kept],
}))
"""
# The columns of the stand-ins of the raw tables, every one text, as dlt writes them.
ITALY_COLUMNS = (
    "country",
    "currency",
    "iso_code",
    "uic_code",
    "avg_rate",
    "exchange_convention",
    "exchange_convention_code",
    "reference_date",
)
NBK_COLUMNS = ("date", "fullname", "title", "description", "quant", "index", "change")
ECB_COLUMNS = (
    "key",
    "freq",
    "currency",
    "currency_denom",
    "exr_type",
    "exr_suffix",
    "time_period",
    "obs_value",
)
# The loads of the check: the source, the id of its load, its account, its window and its kind.
# The first load of the Bank of Italy is a load of the history, as after an update from v0.5.
CHAIN_LOADS = (
    ("bank_of_italy", "9000000001.1", "EUR", "2026-11-06", "2026-11-12", "history"),
    ("nbk", "9000000002.1", "KZT", "2026-11-06", "2026-11-09", "window"),
    ("nbk", "9000000003.1", "KZT", "2026-11-12", "2026-11-12", "window"),
    ("ecb", "9000000004.1", "EUR", "2026-11-06", "2026-11-12", "window"),
)


def literal(value):
    """A value as an SQL literal of text: data of a bank, whose quotes are doubled."""
    return "NULL" if value is None else "'" + str(value).replace("'", "''") + "'"


def compiled_node(name, select, path, currency=None):
    """The SQL of a node as dbt compiles it in the container of the tasks, in a reporting currency
    when one is given."""
    work = f"{WORK}-{name}"
    service = task_service()
    command = ["exec", "-T"]
    if currency:
        command += ["-e", f"AGORANOMOS_REPORTING_CURRENCY={currency}"]
    command += ["-e", "AGORANOMOS_RATES_SOURCE=auto"]
    command += [service, DBT, "--quiet", "--no-use-colors", "compile", "--select", select]
    command += ["--project-dir", PROJECT, "--profiles-dir", PROJECT]
    command += ["--target-path", f"{work}/target", "--log-path", f"{work}/logs"]
    compose(*command)
    return compose("exec", "-T", service, "cat", f"{work}/target/compiled/agoranomos/{path}").stdout


def stand_in(sql, first_column, table):
    """The SQL of a compiled staging model of a bank with the stand-in of its raw table in place
    of the empty query that it reads while the table does not exist (relation_or_empty)."""
    pattern = rf"\(select\s+cast\(null as text\) as {first_column}\b.*?where false\) as rates_daily"
    found, count = re.subn(pattern, f"{table} as rates_daily", sql, flags=re.S)
    if count != 1:
        pytest.skip(f"the staging model reads a raw table of its bank: {table} stands in for none")
    return found


@pytest.fixture(scope="module")
def chain(env, core_rates):
    """The rows of the recorded answers as the loads give them, and the SQL that puts them under
    the staging views, in one transaction that the caller ends."""
    italy, empty = ITALY.read_text(encoding="utf-8"), ITALY_EMPTY.read_text(encoding="utf-8")
    arguments = [",".join(CHAIN_CURRENCIES)]
    arguments += ["bank_of_italy:2026-11-06", italy.replace('"2026-11-12"', '"2026-11-06"')]
    for day in ("2026-11-09", "2026-11-10", "2026-11-11"):
        arguments += [f"bank_of_italy:{day}", empty.replace('"2026-11-08"', f'"{day}"')]
    arguments += ["bank_of_italy:2026-11-12", italy]
    for day in NBK_DAYS:
        arguments += [f"nbk:{day}", (FIXTURES / "nbk" / f"{day}.xml").read_text(encoding="utf-8")]
    parsed = json.loads(run_in_tasks(env, PARSE, *arguments).stdout.strip().splitlines()[-1])
    italy_load = CHAIN_LOADS[0][1]
    statements = ["BEGIN"]
    statements.append(
        "CREATE TABLE raw_bank_of_italy.stack_test_rates ("
        + ", ".join(f"{c} text" for c in (*ITALY_COLUMNS, "_dlt_load_id"))
        + ")"
    )
    rows = [
        "(" + ", ".join(literal(row.get(c)) for c in ITALY_COLUMNS) + f", '{italy_load}')"
        for row in parsed["bank_of_italy"]
    ]
    # The values are literals of the recorded answers, their quotes doubled.
    statements.append(f"INSERT INTO raw_bank_of_italy.stack_test_rates VALUES {', '.join(rows)}")  # noqa: S608
    statements.append(
        "CREATE TABLE raw_nbk.stack_test_rates ("
        + ", ".join(f'"{c}" text' for c in (*NBK_COLUMNS, "_dlt_load_id"))
        + ")"
    )
    rows = []
    for row in parsed["nbk"]:
        load = CHAIN_LOADS[2][1] if row["date"] == "12.11.2026" else CHAIN_LOADS[1][1]
        rows.append("(" + ", ".join(literal(row.get(c)) for c in NBK_COLUMNS) + f", '{load}')")
    # The values are literals of the recorded answers, their quotes doubled.
    statements.append(f"INSERT INTO raw_nbk.stack_test_rates VALUES {', '.join(rows)}")  # noqa: S608
    # The ECB publishes the same numbers as the Bank of Italy for its currencies (ADR-0042): those
    # of the answer of 12 November stand for Tuesday 10 November.
    statements.append(
        "CREATE TABLE raw_ecb.stack_test_rates ("
        + ", ".join(f"{c} text" for c in (*ECB_COLUMNS, "_dlt_load_id"))
        + ")"
    )
    ecb_load = CHAIN_LOADS[3][1]
    rows = [
        f"('D.{row['iso_code']}.EUR.SP00.A', 'D', '{row['iso_code']}', 'EUR', 'SP00', 'A', "
        f"'2026-11-10', {literal(row['avg_rate'])}, '{ecb_load}')"
        for row in parsed["bank_of_italy"]
        if row["reference_date"] == "2026-11-12" and row["iso_code"] in ("USD", "GBP")
    ]
    # The values are literals of the recorded answers, their quotes doubled.
    statements.append(f"INSERT INTO raw_ecb.stack_test_rates VALUES {', '.join(rows)}")  # noqa: S608
    # The views of staging that dbt built keep transformer as their owner when they are replaced,
    # and a view reads its tables with the privileges of its owner.
    statements.append(
        "GRANT SELECT ON raw_bank_of_italy.stack_test_rates, raw_nbk.stack_test_rates, "
        "raw_ecb.stack_test_rates TO transformer"
    )
    for code, first_column, path in (
        ("bank_of_italy", "country", "bank_of_italy/stg_bank_of_italy__rates_daily.sql"),
        ("nbk", "date", "nbk/stg_nbk__rates_daily.sql"),
        ("ecb", "key", "ecb/stg_ecb__rates_daily.sql"),
    ):
        model = f"stg_{code}__rates_daily"
        sql = compiled_node(f"chain-{code}", model, f"models/staging/{path}")
        sql = stand_in(sql, first_column, f"raw_{code}.stack_test_rates")
        statements.append(f"CREATE OR REPLACE VIEW staging.{model} AS {sql}")
    statements.append(
        "DELETE FROM staging.stg_loads__newest_days WHERE source IN ('bank_of_italy', 'nbk', 'ecb')"
    )
    for source, load, account, first, last, kind in CHAIN_LOADS:
        statements.append(
            "INSERT INTO staging.stg_loads__newest_days "  # noqa: S608 (constants of this file)
            "(source, account_id, day, load_id, load_key) "
            f"SELECT '{source}', '{account}', d::date, '{load}', "
            f"'{source}|{load}|' || (d::date - date '2000-01-01')::text "
            f"FROM generate_series(date '{first}', date '{last}', interval '1 day') AS d"
        )
        statements.append(
            "INSERT INTO core.loads (source, load_id, account_id, window_from, window_to, outcome, "  # noqa: S608 (constants of this file)
            f"finished_at, invocation_id, kind) VALUES ('{source}', '{load}', '{account}', "
            f"'{first}', '{last}', 'loaded', now(), gen_random_uuid(), '{kind}')"
        )
    # The accounts of the check, so that the model computes their currencies.
    statements.append(
        "INSERT INTO core.dim_account (platform, account_id, client_id, currency, is_active, "  # noqa: S608 (constants of this file)
        "is_stub) SELECT 'stack_test_rates', currency, 'stack-test', currency, true, false "
        f"FROM unnest(ARRAY{list(CHAIN_CURRENCIES)!r}) AS currency"
    )
    return parsed, statements


def chain_rows(env, chain, currency, held=()):
    """The rows that the model of the rates would merge in a reporting currency over the recorded
    answers, with the rows of held in core.fx_rates_daily first, by currency and day: each a dict
    of its columns as text."""
    _, statements = chain
    sql = compiled_node(
        f"chain-core-{currency}", CORE_MODEL, "models/core/core_fx_rates_daily.sql", currency
    )
    columns = (
        "rate_date",
        "currency",
        "rate_into_reporting_currency",
        "base_currency",
        "base_rate",
        "rate_source",
        "published_on",
        "is_carried_forward",
        "cross_source",
        "cross_rate",
        "cross_published_on",
    )
    shown = " || '|' || ".join(f"coalesce(w.{c}::text, '')" for c in columns)
    output = warehouse_output(
        env,
        "; ".join(
            [
                *statements,
                *held,
                f"SELECT 'written|' || {shown} FROM ({sql}) AS w "  # noqa: S608 (the SQL of dbt and constants of this file)
                f"WHERE w.currency IN {tuple(CHAIN_CURRENCIES)!r}",
                "ROLLBACK",
            ]
        ),
    )
    found = {}
    for values in written(output):
        row = dict(zip(columns, values, strict=True))
        found[(row["currency"], row["rate_date"])] = row
    return found


def price(parsed, title, day):
    """The tenge for one unit of a currency of the NBK on a day, as the feed writes it."""
    [row] = [r for r in parsed["nbk"] if r["title"] == title and r["date"] == day]
    return Decimal(row["description"]) / Decimal(row["quant"])


def euros(parsed, code, day):
    """The units of a currency for one euro of the Bank of Italy on a day, as it writes them."""
    [row] = [
        r for r in parsed["bank_of_italy"] if r["iso_code"] == code and r["reference_date"] == day
    ]
    return Decimal(row["avg_rate"])


def test_the_rates_in_euros_take_the_bank_of_italy_then_the_nbk_then_the_ecb(env, chain):
    # ADR-0042: the Bank of Italy first; RUB, which it gives as N.A., from the NBK through the
    # tenge, on a Saturday too, with the tenge of the Bank of Italy of the Friday; the ECB on a day
    # that the Bank of Italy missed; no rate of KPW, which no source has, so that an account in it
    # goes to quarantine (missing_rate) and G3 never meets it.
    parsed, _ = chain
    rows = chain_rows(env, chain, "EUR")
    saturday = rows[("RUB", "2026-11-07")]
    assert (saturday["rate_source"], saturday["base_currency"]) == ("nbk", "KZT")
    assert (saturday["cross_source"], saturday["cross_published_on"]) == (
        "bank_of_italy",
        "2026-11-06",
    )
    assert Decimal(saturday["base_rate"]) == price(parsed, "RUB", "07.11.2026")
    assert Decimal(saturday["cross_rate"]) == euros(parsed, "KZT", "2026-11-06")
    wanted = price(parsed, "RUB", "07.11.2026") / euros(parsed, "KZT", "2026-11-06")
    assert abs(Decimal(saturday["rate_into_reporting_currency"]) / wanted - 1) < Decimal("1e-12")
    assert saturday["is_carried_forward"] == "false"
    # The tenge of the Bank of Italy is carried over the weekend, not taken from the NBK.
    tenge = rows[("KZT", "2026-11-07")]
    assert (tenge["rate_source"], tenge["published_on"], tenge["is_carried_forward"]) == (
        "bank_of_italy",
        "2026-11-06",
        "true",
    )
    # The Bank of Italy quotes nothing on Tuesday 10 November: the ECB covers its currencies.
    tuesday = rows[("USD", "2026-11-10")]
    assert (tuesday["rate_source"], tuesday["published_on"]) == ("ecb", "2026-11-10")
    assert rows[("ARS", "2026-11-10")]["rate_source"] == "bank_of_italy"
    assert rows[("ARS", "2026-11-10")]["is_carried_forward"] == "true"
    assert rows[("USD", "2026-11-12")]["rate_source"] == "bank_of_italy"
    assert not [key for key in rows if key[0] == "KPW"]
    # Every day from the first rate of each other currency, 6 November, to the last day that the
    # run computes has a row: no gap, which G3 would find.
    for code in ("RUB", "ARS", "KZT", "GBP", "USD"):
        days = sorted(date.fromisoformat(day) for (c, day) in rows if c == code)
        assert days[0] == date(2026, 11, 6), code
        assert days == [days[0] + timedelta(days=n) for n in range(len(days))], code
        assert days[-1] >= date(2026, 11, 12), code


def test_the_rates_in_tenge_take_the_nbk_then_the_bank_of_italy(env, chain):
    # ADR-0042: a deployment that reports in KZT takes the official rate of the NBK, the price
    # that it publishes, and a currency that the NBK lacks from the Bank of Italy through the euro
    # of the NBK of the same day.
    parsed, _ = chain
    rows = chain_rows(env, chain, "KZT")
    dollar = rows[("USD", "2026-11-12")]
    assert (dollar["rate_source"], dollar["cross_source"]) == ("nbk", "")
    assert Decimal(dollar["rate_into_reporting_currency"]) == price(parsed, "USD", "12.11.2026")
    assert rows[("RUB", "2026-11-07")]["rate_source"] == "nbk"
    peso = rows[("ARS", "2026-11-12")]
    assert (peso["rate_source"], peso["cross_source"], peso["cross_published_on"]) == (
        "bank_of_italy",
        "nbk",
        "2026-11-12",
    )
    assert Decimal(peso["cross_rate"]) == price(parsed, "EUR", "12.11.2026")
    wanted = price(parsed, "EUR", "12.11.2026") / euros(parsed, "ARS", "2026-11-12")
    assert abs(Decimal(peso["rate_into_reporting_currency"]) / wanted - 1) < Decimal("1e-12")
    assert not [key for key in rows if key[0] == "KPW"]


def test_the_rates_held_from_the_ecb_keep_their_source_under_a_history_of_the_bank(env, chain):
    # The decision of 16 November 2026: a row that the core holds from the ECB, as on a deployment
    # of v0.5, keeps its source when the first load of the Bank of Italy, a load of the history,
    # brings the same day, and that load fills only what the core lacks, such as ARS. The load of
    # the ECB that this run reads starts on 10 November: the core accepted the ECB of 6 November in
    # a run before, and the raw layer no longer holds it (ADR-0035). A load that read the day would
    # bring its rates, and a day that it reads without a rate has no publication.
    parsed, _ = chain
    pound = euros(parsed, "GBP", "2026-11-06")
    held = [
        "DELETE FROM staging.stg_loads__newest_days WHERE source = 'ecb' AND day < '2026-11-10'",
        "INSERT INTO core.fx_rates_daily (rate_date, currency, reporting_currency, "  # noqa: S608 (a rate of a recorded answer)
        "rate_into_reporting_currency, base_currency, base_rate, rate_source, published_on, "
        f"is_carried_forward) VALUES ('2026-11-06', 'GBP', 'EUR', 1 / {pound}, 'EUR', {pound}, "
        "'ecb', '2026-11-06', false)",
    ]
    rows = chain_rows(env, chain, "EUR", held)
    kept = rows.get(("GBP", "2026-11-06"))
    assert kept is None or kept["rate_source"] == "ecb", kept
    assert rows[("ARS", "2026-11-06")]["rate_source"] == "bank_of_italy"
    assert Decimal(rows[("ARS", "2026-11-06")]["base_rate"]) == euros(parsed, "ARS", "2026-11-06")
