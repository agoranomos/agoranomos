"""The rates of the file of the deployment in real runs of the transform (ADR-0042, C7 of #227).

An account of demo_social takes a currency that no source of the rates has, KPW, which the rates
of the demo lack too, and extensions/model/rates.toml gives a rate of it into the reporting
currency. The checks of the quarantine count that currency as covered only on the days of the
entries of the file (dbt/macros/quarantine.sql): with an entry shorter than the window of the
load, the account goes to quarantine with missing_rate for the days that the entry leaves out, and
the run goes on, green, for the other accounts, where the test of G3 would otherwise fail the
whole run, as it did before the check counted the days; with an entry that covers the window, the
run takes the account into the core with the rate of the file, rate_source deployment, on every
day of the window.

The test makes a load of its own of the last days of demo_social, as of its newest load, which the
two runs read: the first with its acceptance skipped, so that the load stays pinned for the second.
It changes the currency of the account in raw as loader, the owner of raw, and writes the file into
the extensions folder of the stack. Its finally puts the currency back, removes the file and loads
the settings again without it, deletes the rows of KPW from core.fx_rates_daily, and makes a new
load of demo_social and a run of the transform, so the demo is as before. If it is killed before
that, do the same by hand, as the superuser of the warehouse: UPDATE the currency of
802000000000002 in raw_demo_social.ad_accounts to EUR, delete extensions/model/rates.toml, DELETE
FROM core.fx_rates_daily WHERE currency = 'KPW', and run the transform after a daily load of
demo_social.

The assertions print codes, IDs, days, counts and states, never a secret.
"""

from datetime import date, timedelta

import pytest
from agoranomos_pipelines.demo import world
from airflow_api import (
    is_busy,
    skip,
    task_instances,
    trigger,
    unpause,
    wait_for_dag,
    wait_for_run,
)
from raw_loads import last_moment, load_platform, run_in_tasks
from stack_helpers import REPO_ROOT, wait_for, warehouse_output

pytestmark = [
    pytest.mark.demo,
    pytest.mark.usefixtures("daily_load_paused"),
    pytest.mark.transform,
]

TRANSFORM = "agoranomos_demo_transform"
ASSETS = 5
# The account of demo_social whose currency the test changes, and the currency, which neither the
# real sources of the rates nor those of the demo have.
[ACCOUNT] = [a for a in world.ACCOUNTS if a.account_id == "802000000000002"]
[OTHER] = [a for a in world.ACCOUNTS if a.platform == "demo_social" and a is not ACCOUNT]
CURRENCY = "KPW"
RATE = "0.0011"
LISTED = (
    "SELECT currency FROM raw_demo_social.ad_accounts WHERE account_id = '{id}' "
    "ORDER BY _dlt_load_id::numeric DESC LIMIT 1"
)
SET = "UPDATE raw_demo_social.ad_accounts SET currency = '{value}' WHERE account_id = '{id}'"
# The window of the load of the test for the account, as its journal records it.
WINDOW = (
    "SELECT window_from || '|' || window_to FROM raw_demo_social._agoranomos_loads "
    "WHERE _dlt_load_id = '{load}' AND account_id = '{id}'"
)
# The accounts that the last run held, by reason, as Ops shows them to metabase_reader.
QUARANTINED = (
    "SELECT platform || '|' || account_id || '|' || reason || '|' || detail "
    "FROM ops.ops_quarantine ORDER BY 1"
)
# The days of the windows that the last run wrote into the facts of the core, of an account, while
# the run has not accepted its loads: the acceptance takes their days out of the table.
WRITTEN_DAYS = (
    "SELECT count(*) FROM staging.int_window_days "
    "WHERE source = 'demo_social' AND account_id = '{id}'"
)
# The state of the load of the test of an account in the journal of the core: a run accepts the
# load of each account that it took into the core, and none of an account in quarantine.
LOAD_STATUS = (
    "SELECT status FROM core.loads "
    "WHERE source = 'demo_social' AND load_id = '{load}' AND account_id = '{id}'"
)
# The task that loads the settings of the model into raw_settings, as its task does.
LOAD_SETTINGS = """
from agoranomos_pipelines.settings import source

print(source.load()["rates"])
"""


def rows(env, sql, role="postgres"):
    return warehouse_output(env, sql, role).splitlines()


def reporting(env):
    """The reporting currency of the stack, USD unless .env says otherwise."""
    return env.get("AGORANOMOS_REPORTING_CURRENCY") or "USD"


def rates_file(env):
    """extensions/model/rates.toml in the extensions folder that .env names."""
    folder = env.get("AGORANOMOS_EXTENSIONS_DIR") or "./extensions"
    return (REPO_ROOT / folder).resolve() / "model" / "rates.toml"


def write_entry(path, quote, first, last):
    path.write_text(
        f'[[rates]]\ncurrency = "{CURRENCY}"\nquote = "{quote}"\nrate = {RATE}\n'
        f"from = {first}\nto = {last}\n",
        encoding="utf-8",
    )


def transform(airflow_url, token, skipped=None):
    """A run of the transform, with a task skipped when one is named, and its states by task."""
    unpause(airflow_url, token, TRANSFORM)
    run = trigger(airflow_url, token, TRANSFORM)
    if skipped:
        skip(airflow_url, token, run, skipped)
    run = wait_for_run(airflow_url, token, TRANSFORM, run["dag_run_id"], timeout=1800)
    states = {i["task_id"]: i["state"] for i in task_instances(airflow_url, token, run)}
    return run, states


@pytest.fixture(scope="module")
def idle(env, airflow_url, token):
    """No run of the transform is queued or running, the core is built, no account is in
    quarantine, and neither the file nor the core holds a rate of the currency of the test."""
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
    assert not rates_file(env).exists(), "extensions/model/rates.toml is in the way of this test"
    held = warehouse_output(
        env,
        f"SELECT count(*) FROM core.fx_rates_daily WHERE currency = '{CURRENCY}'",  # noqa: S608 (a constant)
    )
    assert held == "0", f"core.fx_rates_daily holds rates of {CURRENCY}: see the docstring"


def test_a_rate_of_the_file_covers_its_currency_only_on_the_days_of_its_entry(
    env, airflow_url, token, idle
):
    currency = reporting(env)
    path = rates_file(env)
    path.parent.mkdir(parents=True, exist_ok=True)
    # The run before accepted the loads that it read and deleted their rows from raw: the load of
    # the test brings the list of the accounts again.
    now = last_moment(env, "demo_social")
    load = load_platform(env, "demo_social", now, (now - timedelta(days=4)).date())
    listed = warehouse_output(env, LISTED.format(id=ACCOUNT.account_id))
    assert listed == ACCOUNT.currency, listed
    window = warehouse_output(env, WINDOW.format(load=load, id=ACCOUNT.account_id))
    first, last = (date.fromisoformat(day) for day in window.split("|"))
    assert (last - first).days >= 3, window
    try:
        warehouse_output(env, SET.format(value=CURRENCY, id=ACCOUNT.account_id), "loader")
        # A5: an entry that leaves out the first two days of the window. The account goes to
        # quarantine for them, with the days in the detail, and the run stays green: every task
        # of dbt built its node, G3 among them, and the other account of demo_social goes on.
        write_entry(path, currency, first + timedelta(days=2), last)
        run, states = transform(airflow_url, token, skipped="accept")
        failed = sorted(
            task for task, state in states.items() if state not in ("success", "skipped")
        )
        assert run["state"] == "success", (run["state"], failed)
        assert failed == [], failed
        uncovered = f"on 2 days from {first} to {first + timedelta(days=1)}"
        assert rows(env, QUARANTINED, "metabase_reader") == [
            f"demo_social|{ACCOUNT.account_id}|missing_rate|rates: no rate of {CURRENCY} into "
            f"{currency} from the source of the rates, demo_fx, nor from "
            f"extensions/model/rates.toml {uncovered}",
        ]
        assert warehouse_output(env, WRITTEN_DAYS.format(id=ACCOUNT.account_id)) == "0"
        assert int(warehouse_output(env, WRITTEN_DAYS.format(id=OTHER.account_id))) > 0
        # C7: an entry that covers the window. The run takes the account into the core with its
        # currency, and the rate of the file on each day of the window, exact, with the source
        # deployment, the quote as its base, the first day of the entry as its day of
        # publication, and never the flag of a carried rate.
        write_entry(path, currency, first, last)
        run, states = transform(airflow_url, token)
        failed = sorted(task for task, state in states.items() if state != "success")
        assert run["state"] == "success", (run["state"], failed)
        assert rows(env, QUARANTINED, "metabase_reader") == []
        held = warehouse_output(
            env,
            "SELECT currency FROM core.dim_account "  # noqa: S608 (an ID of the demo)
            f"WHERE platform = 'demo_social' AND account_id = '{ACCOUNT.account_id}'",
        )
        assert held == CURRENCY, held
        rated = rows(
            env,
            "SELECT rate_date || '|' || rate_into_reporting_currency || '|' || base_currency "  # noqa: S608 (constants of this file and a code of .env)
            "|| '|' || base_rate || '|' || rate_source || '|' || published_on || '|' "
            "|| is_carried_forward FROM core.fx_rates_daily "
            f"WHERE currency = '{CURRENCY}' AND reporting_currency = '{currency}' "
            "ORDER BY rate_date",
        )
        days = [first + timedelta(days=n) for n in range((last - first).days + 1)]
        assert rated == [
            f"{day}|{RATE}|{currency}|{RATE}000000|deployment|{first}|false" for day in days
        ], rated
        # The run took the load of the account into the core and accepted it.
        status = warehouse_output(env, LOAD_STATUS.format(load=load, id=ACCOUNT.account_id))
        assert status == "accepted", status
    finally:
        warehouse_output(env, SET.format(value=listed, id=ACCOUNT.account_id), "loader")
        path.unlink(missing_ok=True)
        run_in_tasks(env, LOAD_SETTINGS)
        # No account has the currency once the next run takes the list again: its rates go, so
        # that ops.ops_rates names no day of them off the file.
        warehouse_output(
            env,
            f"DELETE FROM core.fx_rates_daily WHERE currency = '{CURRENCY}'",  # noqa: S608 (a constant)
        )
        now = last_moment(env, "demo_social")
        load_platform(env, "demo_social", now, (now - timedelta(days=4)).date())
        again, _ = transform(airflow_url, token)
    assert again["state"] == "success", again["state"]
    assert rows(env, QUARANTINED, "metabase_reader") == []
    held = warehouse_output(
        env,
        "SELECT currency FROM core.dim_account "  # noqa: S608 (an ID of the demo)
        f"WHERE platform = 'demo_social' AND account_id = '{ACCOUNT.account_id}'",
    )
    assert held == ACCOUNT.currency, held
    assert warehouse_output(env, "SELECT count(*) FROM raw_settings.rates") == "0"
