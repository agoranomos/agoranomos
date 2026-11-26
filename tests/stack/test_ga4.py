"""GA4 in the core and the marts (ADR-0054, task C6c of #226): a load of the connector of GA4,
through the real dlt source of airflow/dags/agoranomos_pipelines/ga4.py with the HTTP of the Admin
API and the Data API faked, reaches core.web_sessions_daily and marts.web_sessions_daily through
the next run of the transform; a session whose campaign ID, the utm_id of its links, is the ID of a
campaign of the demo names that campaign's platform and account in the mart; and a property whose
sessions the core holds and whose next load brings none goes to quarantine with empty_window (G1a
of ADR-0035), while the others go on and the core keeps its sessions.

No load reaches Google, and no answer comes from a real property: the answers below are faked by
hand from the reference of properties.get and runReport v1beta, as tests/unit/test_ga4_source.py
fakes them, the stand-in of the HTTP client of dlt answers each request with them in the container
of the tasks, and the credentials of the service account are a stand-in that signs in at once. The
test names three GA4 properties of its own, with IDs that no property of the demo has, in
extensions/model/clients.toml, loads them twice as of the newest load of demo_web, so that the
rates of the demo cover their days, and runs the transform after each load. In its finally it
deletes every row of the three properties from raw_ga4, from the core, as the superuser with the
thaw of ADR-0035, from the tables of staging that the runs wrote, and the file, so that the demo
is as before and no later test finds a property that left the settings. If it is killed before
that, run the cleanup of REMOVE by hand as the superuser of the warehouse, and delete
extensions/model/clients.toml.

The assertions print IDs, days, counts and sums, never a secret: the access token of the load is a
stand-in of low entropy.
"""

import json
from datetime import date, timedelta
from decimal import Decimal

import pytest
from airflow_api import is_busy, trigger, unpause, wait_for_dag, wait_for_run
from raw_loads import last_moment, run_in_tasks
from stack_helpers import REPO_ROOT, wait_for, warehouse_output

pytestmark = [
    pytest.mark.demo,
    pytest.mark.usefixtures("daily_load_paused"),
    pytest.mark.transform,
]

TRANSFORM = "agoranomos_demo_transform"
ASSETS = 5
# The three properties of the test: two whose loads are healthy, and one whose second load brings
# no row, which G1a holds out with empty_window. It is one of the three properties of GA4 of the
# settings, and only more than half stops the source and holds the healthy ones too, with
# source_stopped (quarantine_stop_findings in dbt/macros/quarantine.sql).
HEALTHY, EMPTIED, ALSO_HEALTHY = "990000001", "990000002", "990000003"
PROPERTIES = (HEALTHY, EMPTIED, ALSO_HEALTHY)
IN_PROPERTIES = "('" + "', '".join(PROPERTIES) + "')"
ZONE = "America/New_York"
CLIENTS = f"""\
[clients.ga4-stack-test]
name = "GA4 of the stack test"

[[accounts]]
platform = "ga4"
account_id = "{HEALTHY}"
client = "ga4-stack-test"

[[accounts]]
platform = "ga4"
account_id = "{EMPTIED}"
client = "ga4-stack-test"

[[accounts]]
platform = "ga4"
account_id = "{ALSO_HEALTHY}"
client = "ga4-stack-test"
"""
# A stand-in of the access token, of low entropy: no request leaves the container.
TOKEN = "stand-in-token-of-the-ga4-stack-test"
# The rows of each day of each property, faked by hand: the source, the medium, the campaign name,
# the campaign ID and the default channel group, then the sessions, the engaged sessions, the key
# events and the revenue, as GA4 writes them, strings. The campaign ID of the first two is filled
# in by the test with the ID of a campaign of the demo, as its links would carry it in utm_id; GA4
# splits the sessions of that campaign between two default channel groups, which the key of the
# sessions keeps apart (ADR-0057).
CAMPAIGN = "{campaign}"
ROWS = (
    ("facebook", "paid_social", "Socks", CAMPAIGN, "Paid Social", "40", "25", "3", "120.5"),
    ("facebook", "paid_social", "Socks", CAMPAIGN, "Display", "7", "2", "0", "0"),
    ("(direct)", "(none)", "(direct)", "(not set)", "Direct", "60", "30", "1", "0"),
)

# A load of GA4 as of a moment, through ga4.load, with the HTTP client of dlt replaced by a
# stand-in that answers each request of the APIs with the answers above, given as JSON: the
# property, and the rows of its report for every day of the date range of the request, in one
# page, none for a property of empty. The credentials sign in at once. dlt reads the password of
# loader from the environment of the container, and the script prints the rows of each table.
LOAD = """
import json
import sys
from datetime import date, datetime, timedelta
from urllib.parse import urlsplit

from agoranomos_pipelines import ga4

now = datetime.fromisoformat(sys.argv[1])
faked = json.loads(sys.argv[2])
token = sys.argv[3]
empty = set(json.loads(sys.argv[4]))


class Signed:
    valid = True
    token = token


class Answer:
    def __init__(self, body):
        self.status_code, self.headers = 200, {}
        self.text = json.dumps(body)

    def iter_content(self, size):
        data = self.text.encode("utf-8")
        for start in range(0, len(data), size):
            yield data[start : start + size]

    def close(self):
        pass


class Api:
    def __init__(self, **settings):
        pass

    def request(self, method, url, json=None, headers=None, stream=False):
        assert headers.get("Authorization") == f"Bearer {token}", "a request without the token"
        path = urlsplit(url).path.removeprefix(f"/{ga4.API_VERSION}/properties/")
        if method == "GET":
            return Answer(faked[path]["property"])
        property_id = path.removesuffix(":runReport")
        [days] = json["dateRanges"]
        first = date.fromisoformat(days["startDate"])
        last = date.fromisoformat(days["endDate"])
        rows = []
        if property_id not in empty:
            for n in range((last - first).days + 1):
                day = (first + timedelta(days=n)).strftime("%Y%m%d")
                for *dimensions, sessions, engaged, events, revenue in faked[property_id]["rows"]:
                    rows.append(
                        {
                            "dimensionValues": [{"value": v} for v in (day, *dimensions)],
                            "metricValues": [
                                {"value": v} for v in (sessions, engaged, events, revenue)
                            ],
                        }
                    )
        body = {
            "dimensionHeaders": [{"name": name} for name in ga4.DIMENSIONS],
            "metricHeaders": [{"name": name, "type": "TYPE_INTEGER"} for name in ga4.METRICS],
            "metadata": {"currencyCode": "USD", "timeZone": faked[property_id]["zone"]},
            "propertyQuota": {"tokensPerDay": {"consumed": 10, "remaining": 199990}},
            "kind": "analyticsData#runReport",
        }
        if rows:
            body["rows"] = rows
            body["rowCount"] = len(rows)
        return Answer(body)


ga4.Client = Api
ga4.sign_in = Signed
counts = ga4.load(now)
print(json.dumps(counts, sort_keys=True))
"""


def answers(campaign_id: str) -> dict:
    """The answers of the APIs for each property of the test: its row of properties.get, its time
    zone, and the rows of its report for one day, with the campaign of the demo in the first."""
    rows = [[campaign_id if value == CAMPAIGN else value for value in row] for row in ROWS]
    return {
        property_id: {
            "property": {
                "name": f"properties/{property_id}",
                "displayName": f"Stack test {property_id[-1]}",
                "timeZone": ZONE,
                "currencyCode": "USD",
                "propertyType": "PROPERTY_TYPE_ORDINARY",
            },
            "zone": ZONE,
            "rows": rows,
        }
        for property_id in PROPERTIES
    }


# The rows of the three properties everywhere that the loads and the runs wrote them, in one
# transaction with the thaw of the facts (ADR-0035), as the superuser: the tables of raw by
# property, the loads of dlt that only these properties had, the facts, the quarantine, the loads
# of the core and the registry of accounts, and the rows of the tables of staging that the runs
# filled, which the next run fills anew. A table that does not exist is passed over.
REMOVE = """
BEGIN;
SELECT set_config('agoranomos.thaw', pg_current_xact_id()::text, true) <> '';
DO $$
DECLARE
    t record;
    accounts text[] := ARRAY[{accounts}];
    names text[];
    keys integer[];
BEGIN
    names := ARRAY(SELECT 'properties/' || a FROM unnest(accounts) AS a);
    IF to_regclass('raw_ga4._agoranomos_loads') IS NOT NULL
        AND to_regclass('raw_ga4._dlt_loads') IS NOT NULL THEN
        DELETE FROM raw_ga4._dlt_loads AS l
        WHERE l.load_id IN (
            SELECT _dlt_load_id FROM raw_ga4._agoranomos_loads
            WHERE account_id = ANY(accounts)
        )
            AND NOT EXISTS (
                SELECT FROM raw_ga4._agoranomos_loads AS j
                WHERE j._dlt_load_id = l.load_id AND NOT j.account_id = ANY(accounts)
            );
    END IF;
    IF to_regclass('raw_ga4._agoranomos_loads') IS NOT NULL THEN
        DELETE FROM raw_ga4._agoranomos_loads WHERE account_id = ANY(accounts);
    END IF;
    IF to_regclass('raw_ga4.properties') IS NOT NULL THEN
        DELETE FROM raw_ga4.properties WHERE name = ANY(names);
    END IF;
    IF to_regclass('raw_ga4.report_sessions_daily') IS NOT NULL THEN
        DELETE FROM raw_ga4.report_sessions_daily WHERE property = ANY(names);
    END IF;
    keys := ARRAY(
        SELECT account_sk FROM core.dim_account
        WHERE platform = 'ga4' AND account_id = ANY(accounts)
    );
    FOR t IN
        SELECT c.table_name
        FROM information_schema.columns AS c
        JOIN information_schema.tables AS b
            ON b.table_schema = c.table_schema AND b.table_name = c.table_name
        WHERE c.table_schema = 'core' AND c.column_name = 'account_sk'
            AND b.table_type = 'BASE TABLE' AND c.table_name <> 'dim_account'
            AND NOT EXISTS (
                SELECT FROM pg_inherits AS i
                WHERE i.inhrelid = format('core.%I', c.table_name)::regclass
            )
    LOOP
        EXECUTE format('DELETE FROM core.%I WHERE account_sk = ANY($1)', t.table_name)
            USING keys;
    END LOOP;
    DELETE FROM core.account_quarantine WHERE source = 'ga4' AND account_id = ANY(accounts);
    DELETE FROM core.loads WHERE source = 'ga4' AND account_id = ANY(accounts);
    DELETE FROM core.dim_account WHERE account_sk = ANY(keys);
    IF to_regclass('staging.int_g1_failures') IS NOT NULL THEN
        DELETE FROM staging.int_g1_failures
            WHERE source = 'ga4' AND account_id = ANY(accounts);
    END IF;
    IF to_regclass('staging.stg_loads__newest_days') IS NOT NULL THEN
        DELETE FROM staging.stg_loads__newest_days
            WHERE source = 'ga4' AND account_id = ANY(accounts);
    END IF;
END $$;
COMMIT;
""".replace("{accounts}", ", ".join(f"'{property_id}'" for property_id in PROPERTIES))

# A campaign of demo_social whose ID no other campaign of the core has, which the mart joins a
# session to, with its account.
DEMO_CAMPAIGN = """
SELECT m.campaign_id_text || '|' || min(a.account_id)
FROM core.dim_campaign AS m
JOIN core.dim_account AS a ON a.account_sk = m.account_sk
GROUP BY m.campaign_id_text
HAVING count(*) = 1 AND min(a.platform) = 'demo_social'
ORDER BY 1
LIMIT 1
"""


def rows(env, sql, role="postgres"):
    return warehouse_output(env, sql, role).splitlines()


def idle(airflow_url, token):
    wait_for(
        lambda: not is_busy(airflow_url, token, TRANSFORM, assets=ASSETS),
        1800,
        interval=5,
        message="the runs of the transform did not end",
    )


def load(env, now, campaign_id, empty=()):
    run_in_tasks(
        env,
        LOAD,
        now.isoformat(),
        json.dumps(answers(campaign_id)),
        TOKEN,
        json.dumps(sorted(empty)),
    )


def transform(airflow_url, token):
    unpause(airflow_url, token, TRANSFORM)
    run = trigger(airflow_url, token, TRANSFORM)
    run = wait_for_run(airflow_url, token, TRANSFORM, run["dag_run_id"], timeout=1800)
    assert run["state"] == "success", run["state"]


def statuses(env):
    """The status of each load of the core of each property, by property and load."""
    return rows(
        env,
        "SELECT account_id || '|' || status FROM core.loads "  # noqa: S608 (IDs of the test)
        f"WHERE source = 'ga4' AND account_id IN {IN_PROPERTIES} ORDER BY load_id, account_id",
    )


def held(env, property_id):
    """The rows and the sessions that the core holds for a property."""
    found = warehouse_output(
        env,
        "SELECT count(*) || '|' || coalesce(sum(f.sessions), 0) "  # noqa: S608 (IDs of the test)
        "FROM core.web_sessions_daily AS f "
        "JOIN core.dim_account AS a ON a.account_sk = f.account_sk "
        f"WHERE a.platform = 'ga4' AND a.account_id = '{property_id}'",
    ).split("|")
    return int(found[0]), int(found[1])


def test_a_load_of_ga4_reaches_the_core_and_the_marts(env, airflow_url, token):
    folder = env.get("AGORANOMOS_EXTENSIONS_DIR") or "./extensions"
    path = (REPO_ROOT / folder).resolve() / "model" / "clients.toml"
    assert not path.exists(), "extensions/model/clients.toml is in the way of this test"
    wait_for_dag(airflow_url, token, TRANSFORM)
    idle(airflow_url, token)
    # Left behind by a test that was stopped.
    warehouse_output(env, REMOVE)
    found = warehouse_output(env, DEMO_CAMPAIGN)
    assert found, "the core holds no campaign of demo_social with an ID of its own"
    campaign_id, ad_account_id = found.split("|")
    now = last_moment(env, "demo_web")
    path.write_text(CLIENTS, encoding="utf-8")
    try:
        load(env, now, campaign_id)
        # The load took the usual window of each property, the last 14 days to yesterday in its
        # time zone, and recorded each in its journal with its four flags, none of them set.
        windows = rows(
            env,
            "SELECT account_id || '|' || window_from || '|' || window_to || '|' || status "  # noqa: S608 (IDs of the test)
            "|| '|' || is_subject_to_thresholding || '|' || has_data_loss_from_other_row "
            "|| '|' || is_sampled || '|' || is_truncated "
            f"FROM raw_ga4._agoranomos_loads WHERE account_id IN {IN_PROPERTIES} ORDER BY 1",
        )
        assert len(windows) == 3, windows
        _, first, last, *rest = windows[0].split("|")
        first, last = date.fromisoformat(first), date.fromisoformat(last)
        assert rest == ["loaded", "false", "false", "false", "false"], windows
        assert last - first == timedelta(days=13), (first, last)
        assert [w.split("|")[1:] for w in windows[1:]] == [windows[0].split("|")[1:]] * 2
        days = (last - first).days + 1
        transform(airflow_url, token)

        # The core accepted every property, and registers each with its client and time zone.
        assert statuses(env) == [f"{p}|accepted" for p in PROPERTIES], statuses(env)
        registered = rows(
            env,
            "SELECT account_id || '|' || client_id || '|' || is_active || '|' || is_stub "  # noqa: S608 (IDs of the test)
            "|| '|' || time_zone || '|' || currency FROM core.dim_account "
            f"WHERE platform = 'ga4' AND account_id IN {IN_PROPERTIES} ORDER BY 1",
        )
        assert registered == [f"{p}|ga4-stack-test|true|false|{ZONE}|USD" for p in PROPERTIES], (
            registered
        )
        sessions = sum(int(row[5]) for row in ROWS)
        for property_id in PROPERTIES:
            assert held(env, property_id) == (len(ROWS) * days, sessions * days), property_id

        # The mart shows every day of each property, with the sums of its rows, the currency of
        # the property, and the ad campaign of the demo whose ID the first row carries.
        shown = warehouse_output(
            env,
            "SELECT count(*) || '|' || sum(sessions) || '|' || sum(engaged_sessions) || '|' || "  # noqa: S608 (IDs of the test)
            "sum(key_events) || '|' || sum(total_revenue) || '|' || min(currency) || '|' || "
            "min(client_id) || '|' || min(report_date) || '|' || max(report_date) "
            f"FROM marts.web_sessions_daily WHERE platform = 'ga4' AND property_id = '{HEALTHY}'",
            "metabase_reader",
        ).split("|")
        assert [int(shown[0]), int(shown[1]), int(shown[2])] == [
            len(ROWS) * days,
            sessions * days,
            sum(int(row[6]) for row in ROWS) * days,
        ], shown
        assert [Decimal(shown[3]), Decimal(shown[4])] == [
            Decimal(sum(int(row[7]) for row in ROWS) * days),
            sum(Decimal(row[8]) for row in ROWS) * days,
        ], shown
        assert shown[5:] == ["USD", "ga4-stack-test", str(first), str(last)], shown
        campaigns = rows(
            env,
            "SELECT DISTINCT session_campaign_id || '|' || coalesce(ad_platform, '') || '|' "  # noqa: S608 (IDs of the test)
            "|| coalesce(ad_account_id, '') || '|' || session_default_channel_group "
            f"FROM marts.web_sessions_daily WHERE platform = 'ga4' AND property_id = '{HEALTHY}' "
            "ORDER BY 1",
            "metabase_reader",
        )
        assert campaigns == sorted(
            [
                f"{campaign_id}|demo_social|{ad_account_id}|Display",
                f"{campaign_id}|demo_social|{ad_account_id}|Paid Social",
                "(not set)|||Direct",
            ]
        ), campaigns

        # A second load in which one property brings no row: G1a holds it out with empty_window,
        # as its window would erase the sessions of the core, its load stays pinned, and the core
        # keeps its sessions; the other two go on.
        load(env, now, campaign_id, empty={EMPTIED})
        transform(airflow_url, token)
        quarantined = rows(
            env,
            "SELECT platform || '|' || account_id || '|' || reason FROM ops.ops_quarantine "
            "WHERE platform = 'ga4' ORDER BY 1",
            "metabase_reader",
        )
        assert quarantined == [f"ga4|{EMPTIED}|empty_window"], quarantined
        assert statuses(env)[3:] == [
            f"{HEALTHY}|accepted",
            f"{EMPTIED}|pinned",
            f"{ALSO_HEALTHY}|accepted",
        ], statuses(env)
        for property_id in PROPERTIES:
            assert held(env, property_id) == (len(ROWS) * days, sessions * days), property_id
        # The mart, a view over the core, still shows the sessions that the core accepted for the
        # property in quarantine.
        assert rows(
            env,
            "SELECT property_id || '|' || sum(sessions) FROM marts.web_sessions_daily "  # noqa: S608 (IDs of the test)
            f"WHERE platform = 'ga4' AND property_id IN {IN_PROPERTIES} "
            "GROUP BY property_id ORDER BY 1",
            "metabase_reader",
        ) == [f"{p}|{sessions * days}" for p in PROPERTIES]
    finally:
        idle(airflow_url, token)
        warehouse_output(env, REMOVE)
        path.unlink(missing_ok=True)
    assert (
        rows(
            env,
            "SELECT account_id FROM core.dim_account WHERE platform = 'ga4' "  # noqa: S608 (IDs of the test)
            f"AND account_id IN {IN_PROPERTIES}",
        )
        == []
    )
