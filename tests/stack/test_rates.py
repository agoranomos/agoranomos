"""The rates of the model into the reporting currency, the table staging.int_fx_rates__filled: the
reporting currency has the rate 1 on every day of the data, whether or not a source publishes
rates, so that the data of a platform in the reporting currency needs no source of rates.

The assertions print counts and codes of currencies, never a secret.
"""

import pytest
from agoranomos_pipelines.demo import platforms
from airflow_api import is_busy, wait_for_dag
from stack_helpers import active_variant, compose, wait_for, warehouse_output

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
