"""The quarantine of an account (ADR-0035): the checks of G1 and G1a hold an account whose data
fails them out of the core and the marts for a run of the transform, the other accounts go on, a
source with more than half of its pinned accounts in quarantine stops, the run stays green, and Ops
says why. The checks write core.account_quarantine, and int_accounts__current and the intermediate
models of the marts leave the accounts of int_g1_failures out (dbt/macros/quarantine.sql).

The test breaks rows of raw as loader, the owner of raw: a currency outside ISO 4217 in the list of
one account of demo_search, a currency of ISO 4217 that the rates of the demo do not have in the
list of one account of demo_social (ADR-0042), and a spend below zero in a row of the one account of
demo_video; it renames the other account of demo_search in its list, which a healthy account takes
into the core. It puts every row back and runs the transform again in its finally, so the demo is
as before. If it is killed before that, put them back by hand as its finally does, as the superuser
of the warehouse: UPDATE the currency of 4170000012 in raw_demo_search.customer_clients and of
802000000000002 in raw_demo_social.ad_accounts to EUR and the name of 4170000011 to its name in
agoranomos_pipelines/demo/world.py, DELETE the rows of raw_demo_video.report_campaign_daily whose
_dlt_id is stack-test-quarantine, and run the transform.

The assertions print codes, IDs, counts and states, never a secret.
"""

import pytest
from agoranomos_pipelines.demo import world
from airflow_api import is_busy, task_instances, trigger, unpause, wait_for_dag, wait_for_run
from metabase_content import item, registry
from raw_loads import DAYS
from stack_helpers import http_json, wait_for, warehouse_output

pytestmark = [
    pytest.mark.demo,
    pytest.mark.usefixtures("daily_load_paused"),
    pytest.mark.transform,
]

TRANSFORM = "agoranomos_demo_transform"
ASSETS = 5
# The two accounts of demo_search, one of which the test breaks, and the one of demo_video.
ACCOUNTS = {account.account_id: account for account in world.ACCOUNTS}
BAD, GOOD = ACCOUNTS["4170000012"], ACCOUNTS["4170000011"]
# The account of demo_social whose currency the test changes to one that the rates of the demo, of
# euros in US dollars, do not have.
UNRATED = ACCOUNTS["802000000000002"]
UNRATED_CURRENCY = "GBP"
LISTED_SOCIAL = "SELECT currency FROM raw_demo_social.ad_accounts WHERE account_id = '{id}'"
SET_SOCIAL = "UPDATE raw_demo_social.ad_accounts SET currency = '{value}' WHERE account_id = '{id}'"
[VIDEO] = [account for account in world.ACCOUNTS if account.platform == "demo_video"]
RENAMED = "Renamed by the test of the quarantine"
# The rows of the lists of demo_search, which each load replaces.
LISTED = "SELECT {column} FROM raw_demo_search.customer_clients WHERE customer_client__id = '{id}'"
SET = (
    "UPDATE raw_demo_search.customer_clients SET {column} = '{value}' "
    "WHERE customer_client__id = '{id}'"
)
CURRENCY = "customer_client__currency_code"
NAME = "customer_client__descriptive_name"
# A row of the statistics of a campaign that the demo does not have, of the one account of
# demo_video, with a spend below zero, on the first day of its newest whole load, so that staging
# reads it.
NEGATIVE = (
    "INSERT INTO raw_demo_video.report_campaign_daily (advertiser_id, dimensions__campaign_id, "  # noqa: S608 (constants of the demo)
    "dimensions__stat_time_day, metrics__campaign_name, metrics__spend, metrics__impressions, "
    "metrics__clicks, metrics__conversion, metrics__complete_payment, "
    "metrics__total_complete_payment_rate, _dlt_load_id, _dlt_id) "
    f"SELECT '{VIDEO.account_id}', '1799999999999999998', d.day::text || ' 00:00:00', "
    "'Broken by a test', '-1.00', '10', '1', '0', '0', '0', d.load_id, 'stack-test-quarantine' "
    f"FROM ({DAYS.format(dataset='raw_demo_video')}) AS d "
    f"WHERE d.account_id = '{VIDEO.account_id}' ORDER BY d.day LIMIT 1"
)
REMOVE_NEGATIVE = (
    "DELETE FROM raw_demo_video.report_campaign_daily WHERE _dlt_id = 'stack-test-quarantine'"
)
# The accounts that the last run held, by reason, as Ops shows them to metabase_reader.
QUARANTINED = (
    "SELECT platform || '|' || account_id || '|' || reason || '|' || detail "
    "FROM ops.ops_quarantine ORDER BY 1"
)
# A row of the registry of accounts, with the transaction that last wrote it.
REGISTERED = (
    "SELECT xmin::text || '|' || account_name || '|' || currency FROM core.dim_account "
    "WHERE platform = 'demo_search' AND account_id = '{id}'"
)


def transform(airflow_url, token):
    unpause(airflow_url, token, TRANSFORM)
    run = trigger(airflow_url, token, TRANSFORM)
    return wait_for_run(airflow_url, token, TRANSFORM, run["dag_run_id"], timeout=1200)


def rows(env, sql, role="postgres"):
    return warehouse_output(env, sql, role).splitlines()


@pytest.fixture(scope="module")
def idle(env, airflow_url, token):
    """No run of the transform is queued or running, the core is built, and no account is in
    quarantine."""
    wait_for_dag(airflow_url, token, TRANSFORM)
    wait_for(
        lambda: not is_busy(airflow_url, token, TRANSFORM, assets=ASSETS),
        1200,
        interval=5,
        message="the runs of the transform that earlier tests started did not end",
    )
    warehouse_output(env, REMOVE_NEGATIVE, "loader")  # left behind by a test that was stopped
    found = warehouse_output(env, "SELECT to_regclass('staging.int_g1_failures') IS NOT NULL")
    assert found == "t", "staging.int_g1_failures does not exist: no transform has run here"
    assert rows(env, QUARANTINED, "metabase_reader") == [], "an account is in quarantine already"


def test_an_account_that_fails_its_checks_stays_out_and_the_others_go_on(
    env, airflow_url, token, idle, metabase_url, session
):
    currency = warehouse_output(env, LISTED.format(column=CURRENCY, id=BAD.account_id))
    name = warehouse_output(env, LISTED.format(column=NAME, id=GOOD.account_id))
    listed = warehouse_output(env, LISTED_SOCIAL.format(id=UNRATED.account_id))
    assert (currency, name, listed) == (BAD.currency, GOOD.name, UNRATED.currency)
    reporting = env.get("AGORANOMOS_REPORTING_CURRENCY") or "USD"
    registered = warehouse_output(env, REGISTERED.format(id=BAD.account_id))
    [load] = rows(
        env,
        "SELECT max(load_id::numeric)::text FROM staging.stg_loads__newest_days "  # noqa: S608 (an ID of the demo)
        f"WHERE source = 'demo_search' AND account_id = '{BAD.account_id}'",
    )
    days = DAYS.format(dataset="raw_demo_video")
    [first] = rows(env, f"SELECT min(day) FROM ({days}) AS d")  # noqa: S608 (constants of the demo)
    try:
        warehouse_output(env, SET.format(column=CURRENCY, value="ZZZ", id=BAD.account_id), "loader")
        warehouse_output(env, SET.format(column=NAME, value=RENAMED, id=GOOD.account_id), "loader")
        warehouse_output(
            env, SET_SOCIAL.format(value=UNRATED_CURRENCY, id=UNRATED.account_id), "loader"
        )
        warehouse_output(env, NEGATIVE, "loader")
        run = transform(airflow_url, token)
        assert run["state"] == "success", run["state"]
        # A green run: every task of dbt built its node, the checks, the core and the marts.
        states = {i["task_id"]: i["state"] for i in task_instances(airflow_url, token, run)}
        assert set(states.values()) == {"success"}, sorted(
            t for t, s in states.items() if s != "success"
        )
        # Ops names the accounts and the reasons: the account of demo_search for its currency,
        # and that of demo_social for a currency without a rate, which stop nothing, as each is
        # one of two; the one account of demo_video for its spend, which stops its source, as one
        # of one is more than half.
        assert rows(env, QUARANTINED, "metabase_reader") == [
            f"demo_search|{BAD.account_id}|unknown_currency|accounts: ZZZ",
            f"demo_social|{UNRATED.account_id}|missing_rate|rates: no rate of "
            f"{UNRATED_CURRENCY} into {reporting} from the source of the rates, demo_fx",
            f"demo_video|{VIDEO.account_id}|negative_measure|campaign_stats: on {first}",
            f"demo_video|{VIDEO.account_id}|source_stopped|source: in quarantine: 1 of the pinned "
            "accounts of the source, 1",
        ]
        # The journal holds the load and the reason, and the window that the core did not take.
        journal = rows(
            env,
            "SELECT reason || '|' || (window_from IS NOT NULL) FROM core.account_quarantine "  # noqa: S608 (IDs of the demo)
            f"WHERE source = 'demo_search' AND account_id = '{BAD.account_id}' "
            f"AND load_id = '{load}'",
        )
        assert journal == ["unknown_currency|true"], journal
        # The core keeps the account in quarantine as it was, and takes the healthy one.
        assert warehouse_output(env, REGISTERED.format(id=BAD.account_id)) == registered
        good = warehouse_output(env, REGISTERED.format(id=GOOD.account_id)).split("|")
        assert good[1:] == [RENAMED, GOOD.currency]
        # The marts leave out the accounts in quarantine and take the healthy one.
        shown = rows(
            env,
            "SELECT platform || '|' || account_id || '|' || account_name FROM marts.ad_accounts",
        )
        assert f"demo_search|{GOOD.account_id}|{RENAMED}" in shown
        assert not [
            line
            for line in shown
            if line.startswith((f"demo_search|{BAD.account_id}|", "demo_video|"))
        ]
        social = warehouse_output(
            env, "SELECT count(*) FROM marts.ad_stats_campaign_daily WHERE platform = 'demo_social'"
        )
        assert int(social) > 0
        # The two warnings, and no failure.
        run_id = run["dag_run_id"].replace("'", "''")
        warned = rows(
            env,
            "SELECT test_type || '|' || status FROM ops.ops_transform_tests "  # noqa: S608 (an ID of Airflow, quoted)
            f"WHERE airflow_run_id = '{run_id}' AND test_type LIKE 'assert_no_%' ORDER BY 1",
        )
        assert warned == [
            "assert_no_account_is_in_quarantine|warn",
            "assert_no_source_is_stopped|warn",
        ]
        # The card of the Ops collection shows them.
        card = item(metabase_url, session, "ops/quarantine", registry())
        status, answer = http_json(
            "POST", f"{metabase_url}/api/card/{card['id']}/query", {}, headers=session
        )
        assert (status in (200, 202), answer.get("status")) == (True, "completed")
        reasons = sorted(row[2] for row in answer["data"]["rows"])
        assert reasons == ["missing_rate", "negative_measure", "source_stopped", "unknown_currency"]
    finally:
        warehouse_output(
            env, SET.format(column=CURRENCY, value=currency, id=BAD.account_id), "loader"
        )
        warehouse_output(env, SET.format(column=NAME, value=name, id=GOOD.account_id), "loader")
        warehouse_output(env, SET_SOCIAL.format(value=listed, id=UNRATED.account_id), "loader")
        warehouse_output(env, REMOVE_NEGATIVE, "loader")
        # The next run, which the tests after this one need, whatever this one found.
        again = transform(airflow_url, token)
    # It finds the data whole: no account is in quarantine, the core takes each again, and the
    # journal keeps what the run before held out.
    assert again["state"] == "success", again["state"]
    assert rows(env, QUARANTINED, "metabase_reader") == []
    assert warehouse_output(env, REGISTERED.format(id=BAD.account_id)) == registered
    assert warehouse_output(env, REGISTERED.format(id=GOOD.account_id)).split("|")[1] == name
    shown = rows(env, "SELECT platform || '|' || account_id FROM marts.ad_accounts")
    assert {f"demo_search|{BAD.account_id}", f"demo_video|{VIDEO.account_id}"} <= set(shown)
    kept = warehouse_output(
        env,
        "SELECT count(*) FROM core.account_quarantine "  # noqa: S608 (IDs of the demo)
        f"WHERE account_id IN ('{BAD.account_id}', '{VIDEO.account_id}')",
    )
    assert int(kept) >= 3
