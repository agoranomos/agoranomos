"""An account in a currency new to the core whose window starts before the window of the last load
of the rates, in real runs of the transform (ADR-0042, ADR-0035).

The core accepts each load of the rates and the cleanup deletes its rows from raw (ADR-0046), and
the core keeps the rates of the currencies of its accounts only, so the days of a currency new to
the core before the window of the last load of the rates have no rate that a run can read. An
account of demo_social takes such a currency, GBP, which the rates of the demo publish and no
account of the demo has, with a load of its own of the last days, and the rates of the demo a load
of their own of a later part of those days. The run that reads both stays green: the check of the
rates counts the days as the model of the rates writes them (dbt/macros/quarantine.sql), so the
account goes to quarantine with missing_rate for the days before the first rate of GBP that the run
reads, where before it passed the check and failed the test of G3 and the whole run, and the other
accounts go on. The core holds the rates of GBP from that day on, as it keeps the currencies of the
accounts that a run reads, those in quarantine too, and ops.ops_rates names the days without a rate
among those of demo_fx. The next daily load of the rates of the demo, as its task makes it, then
takes their history again, with the kind history (agoranomos_pipelines/rates.py), and the run that
reads it takes the account into the core with a rate on every day of its window.

The test changes the currency of the account in raw as loader, the owner of raw. Its finally puts
the currency back, deletes the rows of GBP from core.fx_rates_daily, and makes a new load of
demo_social and a run of the transform, so the demo is as before. If it is killed before that, do
the same by hand, as the superuser of the warehouse: UPDATE the currency of 802000000000002 in
raw_demo_social.ad_accounts to EUR, DELETE FROM core.fx_rates_daily WHERE currency = 'GBP', and run
the transform after a daily load of demo_social.

The assertions print codes, IDs, days, counts and states, never a secret.
"""

from datetime import date, timedelta

import pytest
from agoranomos_pipelines import rates
from agoranomos_pipelines.demo import fx, world
from airflow_api import is_busy, task_instances, trigger, unpause, wait_for_dag, wait_for_run
from raw_loads import last_moment, load_platform, run_in_tasks
from stack_helpers import wait_for, warehouse_output

pytestmark = [
    pytest.mark.demo,
    pytest.mark.usefixtures("daily_load_paused"),
    pytest.mark.transform,
]

TRANSFORM = "agoranomos_demo_transform"
ASSETS = 5
# The account of demo_social whose currency the test changes, and the currency, which the rates of
# the demo publish and no account of the demo has (agoranomos_pipelines/demo/fx.py).
[ACCOUNT] = [a for a in world.ACCOUNTS if a.account_id == "802000000000002"]
CURRENCY = "GBP"
LISTED = (
    "SELECT currency FROM raw_demo_social.ad_accounts WHERE account_id = '{id}' "
    "ORDER BY _dlt_load_id::numeric DESC LIMIT 1"
)
SET = "UPDATE raw_demo_social.ad_accounts SET currency = '{value}' WHERE account_id = '{id}'"
# The window of a load of a platform of the demo for an account, as its journal records it.
WINDOW = (
    "SELECT window_from || '|' || window_to FROM {dataset}._agoranomos_loads "
    "WHERE _dlt_load_id = '{load}' AND account_id = '{id}'"
)
# The accounts that the last run held, by reason, as Ops shows them to metabase_reader.
QUARANTINED = (
    "SELECT platform || '|' || account_id || '|' || reason || '|' || detail "
    "FROM ops.ops_quarantine ORDER BY 1"
)
# The state of a load in the journal of the core.
LOAD_STATUS = (
    "SELECT status FROM core.loads WHERE source = '{source}' AND load_id = '{load}' "
    "AND account_id = '{id}'"
)
# The rates of the currency of the test that the core holds in a reporting currency.
HELD = (
    "SELECT count(*) || '|' || coalesce(min(rate_date)::text, '') || '|' "  # noqa: S608 (a constant of this file)
    "|| coalesce(string_agg(DISTINCT rate_source, ','), '') FROM core.fx_rates_daily "
    f"WHERE currency = '{CURRENCY}' AND reporting_currency = '{{currency}}'"
)
# The days without a rate that ops.ops_rates names, of the facts and of the rates of the demo.
OPS = (
    "SELECT coalesce(first_day_without_rate::text, '') || '|' || days_without_rate || '|' "
    "|| coalesce(first_day_without_rate_of_source::text, '') || '|' "
    "|| days_without_rate_of_source FROM ops.ops_rates WHERE rate_source = 'demo_fx'"
)
# The daily load of the rates of the demo as its task makes it, as of a moment.
LOAD_DAILY = """
import sys
from datetime import datetime

from agoranomos_pipelines.demo import sources

sources.load_daily("demo_fx", datetime.fromisoformat(sys.argv[1]))
"""
# The newest whole load of the rates of the demo, as its journal records it.
NEWEST_RATES = (
    "SELECT j._dlt_load_id || '|' || j.window_from || '|' || j.window_to || '|' || j.kind "
    "FROM raw_demo_fx._agoranomos_loads AS j "
    "JOIN raw_demo_fx._dlt_loads AS l ON l.load_id = j._dlt_load_id AND l.status = 0 "
    "ORDER BY j._dlt_load_id::numeric DESC LIMIT 1"
)


def rows(env, sql, role="postgres"):
    return warehouse_output(env, sql, role).splitlines()


def reporting(env):
    """The reporting currency of the stack, USD unless .env says otherwise."""
    return env.get("AGORANOMOS_REPORTING_CURRENCY") or "USD"


def window(env, dataset, load, account_id):
    found = warehouse_output(env, WINDOW.format(dataset=dataset, load=load, id=account_id))
    first, last = (date.fromisoformat(day) for day in found.split("|"))
    return first, last


def days(first, last):
    return [first + timedelta(days=n) for n in range((last - first).days + 1)]


def named(found):
    """The days of a detail of the quarantine (quarantine_days of dbt/macros/quarantine.sql)."""
    if len(found) == 1:
        return f"on {found[0]}"
    return f"on {len(found)} days from {min(found)} to {max(found)}"


def transform(airflow_url, token):
    """A run of the transform and its states by task."""
    unpause(airflow_url, token, TRANSFORM)
    run = trigger(airflow_url, token, TRANSFORM)
    run = wait_for_run(airflow_url, token, TRANSFORM, run["dag_run_id"], timeout=1800)
    states = {i["task_id"]: i["state"] for i in task_instances(airflow_url, token, run)}
    return run, states


def green(run, states):
    failed = sorted(task for task, state in states.items() if state != "success")
    assert run["state"] == "success", (run["state"], failed)
    assert failed == [], failed


@pytest.fixture(scope="module")
def idle(env, airflow_url, token):
    """No run of the transform is queued or running, the core is built, no account is in
    quarantine, and the core holds no rate of the currency of the test, in any reporting
    currency."""
    if reporting(env) == CURRENCY:
        pytest.skip(f"the reporting currency of the stack is {CURRENCY}, the currency of the test")
    wait_for_dag(airflow_url, token, TRANSFORM)
    wait_for(
        lambda: not is_busy(airflow_url, token, TRANSFORM, assets=ASSETS),
        1200,
        interval=5,
        message="the runs of the transform that earlier tests started did not end",
    )
    found = warehouse_output(env, "SELECT to_regclass('staging.int_g1_failures') IS NOT NULL")
    assert found == "t", "staging.int_g1_failures does not exist: no transform has run here"
    assert rows(env, QUARANTINED, "metabase_reader") == [], "an account is in quarantine already"
    held = warehouse_output(
        env,
        "SELECT count(*) FROM core.fx_rates_daily "  # noqa: S608 (a constant)
        f"WHERE currency = '{CURRENCY}' OR reporting_currency = '{CURRENCY}'",
    )
    assert held == "0", f"core.fx_rates_daily holds rates of {CURRENCY}: see the docstring"


def test_a_currency_new_to_the_core_waits_in_quarantine_for_the_history_of_its_rates(
    env, airflow_url, token, idle
):
    currency = reporting(env)
    # The load of the account: its last week, with the list of the accounts, which the run before
    # deleted from raw with the loads that it accepted.
    now = last_moment(env, "demo_social")
    load = load_platform(env, "demo_social", now, (now - timedelta(days=6)).date())
    listed = warehouse_output(env, LISTED.format(id=ACCOUNT.account_id))
    assert listed == ACCOUNT.currency, listed
    first, last = window(env, "raw_demo_social", load, ACCOUNT.account_id)
    assert (last - first).days >= 5, (first, last)
    try:
        warehouse_output(env, SET.format(value=CURRENCY, id=ACCOUNT.account_id), "loader")
        # A load of the rates whose window starts two days after that of the account, as the
        # usual window of a load of the rates after the core has accepted the loads before it.
        rates_now = last_moment(env, "demo_fx")
        rates_load = load_platform(env, "demo_fx", rates_now, first + timedelta(days=2), skip=False)
        rates_first, rates_last = window(env, "raw_demo_fx", rates_load, world.RATES.account_id)
        assert rates_first == first + timedelta(days=2), (rates_first, first)
        published = min(day for day in days(rates_first, rates_last) if fx.published(day))
        assert published <= last, (published, last)
        uncovered = [day for day in days(first, last) if day < published]
        # The run stays green, every task of dbt with it, G3 among them: the account goes to
        # quarantine for the days before the first rate of its currency, which the detail names,
        # and the run accepts the other loads, those of the rates too.
        green(*transform(airflow_url, token))
        assert rows(env, QUARANTINED, "metabase_reader") == [
            f"demo_social|{ACCOUNT.account_id}|missing_rate|rates: no rate of {CURRENCY} into "
            f"{currency} from the source of the rates, demo_fx {named(uncovered)}",
        ]
        status = warehouse_output(
            env, LOAD_STATUS.format(source="demo_social", load=load, id=ACCOUNT.account_id)
        )
        assert status == "pinned", status
        status = warehouse_output(
            env, LOAD_STATUS.format(source="demo_fx", load=rates_load, id=world.RATES.account_id)
        )
        assert status == "accepted", status
        # The core keeps the rates of the currency of the account in quarantine that the run read,
        # from the first that the demo published, so ops.ops_rates knows its source and names the
        # days without a rate among those of demo_fx, and not among those of the facts.
        count, held_from, sources = warehouse_output(env, HELD.format(currency=currency)).split("|")
        assert (held_from, sources) == (str(published), "demo_fx"), (held_from, sources)
        assert int(count) > 0, count
        [gaps] = rows(env, OPS)
        assert gaps.split("|") == ["", "0", str(first), str(len(uncovered))], gaps
        # The next daily load of the rates of the demo, as its task makes it, reads ops.ops_rates
        # as transformer and takes the history again, from a week before the first day without a
        # rate or from the first day of the table, with the kind history.
        table_first = date.fromisoformat(
            warehouse_output(env, "SELECT min(rate_date) FROM core.fx_rates_daily")
        )
        start = min(first - timedelta(days=rates.LEAD_DAYS), table_first)
        run_in_tasks(env, LOAD_DAILY, rates_now.isoformat())
        history, window_from, _, kind = warehouse_output(env, NEWEST_RATES).split("|")
        assert (window_from, kind) == (str(start), "history"), (window_from, kind)
        # The run that reads it takes the account into the core, in its currency, with a rate of
        # it on every day of its window, and accepts both loads; Ops names no day without a rate.
        green(*transform(airflow_url, token))
        assert rows(env, QUARANTINED, "metabase_reader") == []
        held = warehouse_output(
            env,
            "SELECT currency FROM core.dim_account "  # noqa: S608 (an ID of the demo)
            f"WHERE platform = 'demo_social' AND account_id = '{ACCOUNT.account_id}'",
        )
        assert held == CURRENCY, held
        rated = warehouse_output(
            env,
            "SELECT count(DISTINCT rate_date) FROM core.fx_rates_daily "  # noqa: S608 (constants of this file and a code of .env)
            f"WHERE currency = '{CURRENCY}' AND reporting_currency = '{currency}' "
            f"AND rate_date BETWEEN date '{first}' AND date '{last}'",
        )
        assert rated == str(len(days(first, last))), rated
        for source, load_id, account_id in (
            ("demo_social", load, ACCOUNT.account_id),
            ("demo_fx", history, world.RATES.account_id),
        ):
            status = warehouse_output(
                env, LOAD_STATUS.format(source=source, load=load_id, id=account_id)
            )
            assert status == "accepted", (source, status)
        [gaps] = rows(env, OPS)
        assert gaps.split("|") == ["", "0", "", "0"], gaps
    finally:
        warehouse_output(env, SET.format(value=listed, id=ACCOUNT.account_id), "loader")
        # No account has the currency once the next run takes the list again: its rates go, so
        # that the other tests find none of them.
        warehouse_output(
            env,
            f"DELETE FROM core.fx_rates_daily WHERE currency = '{CURRENCY}'",  # noqa: S608 (a constant)
        )
        now = last_moment(env, "demo_social")
        load_platform(env, "demo_social", now, (now - timedelta(days=6)).date())
        again, _ = transform(airflow_url, token)
    assert again["state"] == "success", again["state"]
    assert rows(env, QUARANTINED, "metabase_reader") == []
    held = warehouse_output(
        env,
        "SELECT currency FROM core.dim_account "  # noqa: S608 (an ID of the demo)
        f"WHERE platform = 'demo_social' AND account_id = '{ACCOUNT.account_id}'",
    )
    assert held == ACCOUNT.currency, held
