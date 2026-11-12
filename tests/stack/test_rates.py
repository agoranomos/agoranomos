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

The assertions print counts, days and codes of currencies, never a secret.
"""

import re
import subprocess
from datetime import date, timedelta

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
        "round(rate_into_reporting_currency, 8), rate_source, is_carried_forward "
        f"FROM core.fx_rates_daily WHERE reporting_currency = '{currency}' EXCEPT SELECT * "
        "FROM marts.fx_rates_daily) AS c), (SELECT count(*) FROM (SELECT * FROM "
        "marts.fx_rates_daily EXCEPT SELECT rate_date, currency::text, reporting_currency::text, "
        "round(rate_into_reporting_currency, 8), rate_source, is_carried_forward "
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
        # the transform below, in that currency, tests them as they are; ops.ops_rates stays in GBP.
        marts_init()
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
