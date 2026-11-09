"""The rates into the reporting currency.

The marts convert through the table staging.int_fx_rates__filled until they become views over the
core: the reporting currency has the rate 1 on every day of the data, whether or not a source
publishes rates, so that the data of a platform in the reporting currency needs no source of rates.

The core holds core.fx_rates_daily (ADR-0042): a row for each calendar day, each currency of the
accounts and the reporting currency, through the euro with no rounding, with the last published
rate carried over a day without one and flagged; the view ops.ops_rates says how old the last rate
is. The checks of a run that reads only the last window of the rates, of one that brings a later
day of the facts, and of another reporting currency compute the rows that the model would write
from the SQL that dbt compiles, in a transaction that ends in ROLLBACK, so the core is as before.

The assertions print counts, days and codes of currencies, never a secret.
"""

import re
import subprocess
from datetime import date, timedelta

import pytest
from agoranomos_pipelines.demo import platforms
from airflow_api import is_busy, wait_for_dag
from stack_helpers import REPO_ROOT, active_variant, compose, wait_for, warehouse_output

pytestmark = [
    pytest.mark.demo,
    pytest.mark.usefixtures("daily_load_paused"),
]

TRANSFORM = "agoranomos_demo_transform"
RATES = "staging.int_fx_rates__filled"
# dbt in the container of the tasks, which has the password of transformer, with its target and
# logs folders outside the project, which is mounted read-only (CONTRIBUTING.md).
DBT = "/opt/airflow/dbt_venv/bin/dbt"
PROJECT = "/opt/agoranomos/dbt"
WORK = "/tmp/agoranomos-stack-test-rates"  # noqa: S108 (a folder in the container, not on this host)
COMPILED = f"{WORK}/target/compiled/agoranomos/models/intermediate/int_fx_rates__filled.sql"
# The days and currencies of the data of every platform, as the rates take them.
FACTS = (
    "SELECT report_date, currency FROM staging.int_ad_stats__unioned "
    "UNION SELECT report_date, currency FROM staging.int_ad_conversions__unioned "
    "UNION SELECT report_date, currency FROM staging.int_web_sessions__unioned"
)
# What the rates hold for the data: the reporting currency; the days from the first to the last day
# of the data; the rows of the reporting currency with the rate 1 of identity, and those of other
# currencies; and the days and currencies of the data with a rate and without one, in the reporting
# currency and in another. The views compute from the tables when they are read; the rates are a
# table, which rates names, or the SQL of their model as a subquery.
CHECK = f"""
WITH rates AS (SELECT * FROM {{rates}}),
reporting AS (SELECT DISTINCT reporting_currency AS currency FROM rates),
facts AS ({FACTS}),
checked AS (
    SELECT
        f.currency = (SELECT currency FROM reporting) AS in_reporting,
        EXISTS (
            SELECT FROM rates AS r WHERE r.rate_date = f.report_date AND r.currency = f.currency
        ) AS has_rate
    FROM facts AS f
)
SELECT
    (SELECT string_agg(currency, ',') FROM reporting),
    (SELECT max(report_date) - min(report_date) + 1 FROM facts),
    (SELECT count(*) FROM rates WHERE currency = (SELECT currency FROM reporting)
        AND rate = 1 AND rate_source = 'identity' AND NOT is_carried_forward),
    (SELECT count(*) FROM rates WHERE currency <> (SELECT currency FROM reporting)),
    (SELECT count(*) FILTER (WHERE in_reporting AND has_rate) FROM checked),
    (SELECT count(*) FILTER (WHERE in_reporting AND NOT has_rate) FROM checked),
    (SELECT count(*) FILTER (WHERE NOT in_reporting AND has_rate) FROM checked),
    (SELECT count(*) FILTER (WHERE NOT in_reporting AND NOT has_rate) FROM checked)
"""  # noqa: S608 (constants of this file)
# A warehouse without published rates, in a transaction that ends in ROLLBACK, or with the session
# if the check fails: the check computes the rates from the SQL of their model, sees no rate of the
# demo, and the rates are back afterwards.
WITHOUT_RATES = "BEGIN; DELETE FROM raw_demo_fx.rates_daily; {check}; ROLLBACK"
PUBLISHED = "SELECT count(*) FROM raw_demo_fx.rates_daily"


@pytest.fixture(scope="module")
def built(env, airflow_url, token):
    """The views of the model as the last run of the transform built them: an intermediate view is
    missing while a run rebuilds the staging view under it."""
    wait_for_dag(airflow_url, token, TRANSFORM)
    wait_for(
        lambda: not is_busy(airflow_url, token, TRANSFORM, assets=len(platforms.PLATFORMS)),
        600,
        interval=5,
        message="the runs of the transform that earlier tests started did not end",
    )
    found = warehouse_output(env, f"SELECT to_regclass('{RATES}') IS NOT NULL")
    assert found == "t", f"{RATES} does not exist: the transform has not run on this warehouse"


@pytest.fixture(scope="module")
def rates_sql(built):
    """The SQL of the model of the rates, as dbt compiles it in the container of the tasks: the
    model is a table, so a check of other data than that of its last build computes the rates
    from it."""
    service = "airflow-worker" if active_variant() == "celery" else "airflow-scheduler"
    compose(
        "exec",
        "-T",
        service,
        DBT,
        "--quiet",
        "--no-use-colors",
        "compile",
        "--select",
        "int_fx_rates__filled",
        "--project-dir",
        PROJECT,
        "--profiles-dir",
        PROJECT,
        "--target-path",
        f"{WORK}/target",
        "--log-path",
        f"{WORK}/logs",
    )
    return compose("exec", "-T", service, "cat", COMPILED).stdout


def counts(output: str) -> tuple[str, list[int]]:
    """The reporting currency and the counts of the row of CHECK. psql also prints the tag of each
    statement around it, such as BEGIN or ROLLBACK, and no tag holds a |."""
    [row] = [line for line in output.splitlines() if "|" in line]
    currency, *numbers = row.split("|")
    return currency, [int(number) for number in numbers]


def test_every_day_and_currency_of_the_data_has_a_rate(env, built):
    currency, (span, identity, others, rated, unrated, other_rated, other_unrated) = counts(
        warehouse_output(env, CHECK.format(rates=RATES))
    )
    assert len(currency) == 3, currency
    # The days of the rates start at the first published rate, a week before the history.
    assert identity > span, (currency, identity, span)
    assert others > 0, "the demo has accounts in two currencies"
    assert (unrated, other_unrated) == (0, 0), (currency, unrated, other_unrated)
    assert rated > 0, (currency, rated)
    assert other_rated > 0, (currency, other_rated)


def test_the_reporting_currency_has_a_rate_without_published_rates(env, built, rates_sql):
    # As for a platform whose accounts use the reporting currency on a warehouse without a source
    # of rates: each of its days has the rate 1; a day of another currency has none, and the
    # critical tests of the rates fail for it, as they should.
    published = int(warehouse_output(env, PUBLISHED))
    assert published > 0, "the demo has published rates"
    currency, (span, identity, others, rated, unrated, other_rated, other_unrated) = counts(
        warehouse_output(
            env, WITHOUT_RATES.format(check=CHECK.format(rates=f"({rates_sql}) AS compiled"))
        )
    )
    assert identity == span, (currency, identity, span)
    assert others == 0, (currency, others)
    assert (rated > 0, unrated) == (True, 0), (currency, rated, unrated)
    assert (other_rated, other_unrated > 0) == (0, True), (currency, other_rated, other_unrated)
    assert int(warehouse_output(env, PUBLISHED)) == published, "the rates were not put back"


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
    # with no gap, and each day of the windows of the facts with a rate of the currency of its
    # account, as the test of G3 checks.
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
        f"WITH r AS ({rates}) SELECT count(*) FROM staging.int_window_days AS w "  # noqa: S608 (constants of this file)
        "WHERE NOT EXISTS (SELECT FROM r WHERE r.rate_date = w.report_date "
        "AND r.currency = w.currency)",
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
    # published them, with no rounding on the way, and the row keeps the rate of the currency.
    currency, other = reporting(env), other_currency(env)
    [counts] = table(
        env,
        f"WITH published AS ({PUBLISHED_RATES}), "  # noqa: S608 (constants of this file and codes of .env)
        f"r AS (SELECT * FROM {CORE_RATES} WHERE reporting_currency = '{currency}' "
        f"AND currency = '{other}' AND NOT is_carried_forward) "
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
        f"SELECT source, account_id, account_sk, currency, date '{days[-1]}', load_sk, day_key "
        f"FROM staging.int_window_days WHERE currency = '{other}' LIMIT 1; "
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
