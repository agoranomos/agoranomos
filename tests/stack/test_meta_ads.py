"""Meta in the core and the marts (ADR-0050, task C1d of #221): a load of the connector of Meta,
through the real dlt source of airflow/dags/agoranomos_pipelines/meta_ads.py with the HTTP of the
Graph API faked, reaches the core and marts.ad_stats_campaign_daily through the next run of the
transform; an account whose campaigns do not add up to its totals goes to quarantine with
totals_differ (G0a of ADR-0035), and a run after it that reads no new load of Meta does not stop
the source for the one account that it still holds; an ad that only the statistics name gets a
stub in its dimension (ADR-0036); a campaign day with no impressions, link clicks or spend and
a purchase keeps its row of statistics, whose conversions_primary counts the purchase (ADR-0036);
and a campaign day whose row lacks impressions and clicks, with spend and link clicks of zero and
a purchase, as the stand saw in October 2026, reaches the core with 0 impressions and its purchase,
and keeps its account out of quarantine.

No load reaches Meta, and no answer comes from a real account: the answers below are faked by hand
from the reference of the Marketing API v27.0, as tests/unit/test_meta_ads_source.py fakes them,
and the stand-in of the HTTP client of dlt answers each request with them in the container of the
tasks. The test names two Meta ad accounts of its own, with IDs that no account has, in
extensions/model/clients.toml, loads them as of the newest load of demo_social, so that the rates
of the demo cover their days, and runs the transform. In its finally it deletes every row of the
two accounts from raw_meta_ads, from the core, as the superuser with the thaw of ADR-0035, from the
tables of staging that the run wrote, and the file, so that the demo is as before and no later
test finds an account that left the settings. If it is killed before that, run the cleanup of
REMOVE by hand as the superuser of the warehouse, and delete extensions/model/clients.toml.

The assertions print IDs, days, counts, sums and states, never a secret: the token of the load is
a stand-in of low entropy.
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
# The two accounts of the test: one whose data is healthy, and one whose totals are 10% above the
# sum of its campaigns, which G0a holds out with a tolerance of 1% for Meta.
HEALTHY, DIFFERING = "900000000000001", "900000000000002"
ACCOUNTS = (HEALTHY, DIFFERING)
IN_ACCOUNTS = "('" + "', '".join(ACCOUNTS) + "')"
CLIENTS = f"""\
[clients.meta-stack-test]
name = "Meta of the stack test"

[[accounts]]
platform = "meta_ads"
account_id = "{HEALTHY}"
client = "meta-stack-test"

[[accounts]]
platform = "meta_ads"
account_id = "{DIFFERING}"
client = "meta-stack-test"
"""
# A stand-in of the token of the system user, of low entropy: no request leaves the container.
TOKEN = "stand-in-token-of-the-meta-stack-test"

# Each day of each account, faked by hand: two campaigns, an active one and an archived one, an ad
# set in each, and three ads, of which the lists give two: the third, a deleted ad, only the
# insights of ads name, as the lists of Meta refuse DELETED in effective_status and return no
# deleted object (the stand S6, November 2026).
CAMPAIGNS = (("120200000000101", "Boots", "ACTIVE"), ("120200000000102", "Socks", "ARCHIVED"))
AD_SETS = (("120200000000201", "120200000000101", "Boots, men"),)
AD_SETS += (("120200000000202", "120200000000102", "Socks, all"),)
LISTED_ADS = (
    ("120200000000301", "120200000000201", "120200000000101", "Boots, red", "ACTIVE"),
    ("120200000000302", "120200000000202", "120200000000102", "Socks, grey", "ARCHIVED"),
)
STUB = ("120200000000303", "120200000000201", "120200000000101", "Boots, blue")
# A deleted campaign, which no list gives, with a purchase of the pixel on each day and no
# impressions, link clicks or spend, as the stand S6 (#220) saw on two campaign days, and no ad set
# in the insights: its row of statistics stays, with zeros, for its conversion (ADR-0036).
CONVERTED = ("120200000000103", "Gloves")
# A second deleted campaign with the same purchase, whose rows lack impressions and clicks, with
# spend "0" and inline_link_clicks "0", as the stand saw in the campaigns of one account on each
# day of 1 to 27 October 2026: the shape reads them as 0 impressions (meta_impressions in
# dbt/macros/shapes/meta_v26.sql), so G1 finds no missing measure.
ABSENT = ("120200000000104", "Scarves")
CONVERTED_ACTIONS = (
    [{"action_type": "offsite_conversion.fb_pixel_purchase", "value": "1", "7d_click": "1"}],
    [{"action_type": "offsite_conversion.fb_pixel_purchase", "value": "49.90"}],
)
# The statistics of a day: impressions, link clicks and spend of each campaign and its ad set,
# and of each ad; the actions of each campaign and ad set, with their values.
DAY = {
    "120200000000101": ("1000", "20", "12.34"),
    "120200000000102": ("500", "5", "3.21"),
}
ADS = {
    "120200000000301": ("600", "12", "7.00"),
    "120200000000303": ("400", "8", "5.34"),
    "120200000000302": ("500", "5", "3.21"),
}
ACTIONS = {
    "120200000000101": (
        [
            {"action_type": "link_click", "value": "20", "1d_click": "20", "7d_click": "20"},
            {
                "action_type": "offsite_conversion.fb_pixel_purchase",
                "value": "2",
                "1d_click": "1",
                "7d_click": "2",
            },
            {"action_type": "omni_purchase", "value": "2", "7d_click": "2"},
        ],
        [
            {"action_type": "offsite_conversion.fb_pixel_purchase", "value": "99.50"},
            {"action_type": "omni_purchase", "value": "99.50"},
        ],
    ),
    "120200000000102": (
        [{"action_type": "offsite_conversion.fb_pixel_lead", "value": "1", "1d_view": "1"}],
        [],
    ),
}
# The totals of a day of each account: the sum of the campaigns, and 10% more spend for the one
# that G0a holds out.
TOTALS = {
    HEALTHY: ("1500", "25", "15.55"),
    DIFFERING: ("1500", "25", "17.11"),
}

# A load of Meta as of a moment, through meta_ads.load, with the HTTP client of dlt replaced by a
# stand-in that answers each request of the Graph API with the answers above, given as JSON, for
# every day of the time_range that the request asks for, and fails a request whose token is not in
# the header Authorization or is in its URL. dlt reads the password of loader from the environment
# of the container, and the script prints the rows of each table.
LOAD = """
import json
import os
import sys
from datetime import date, datetime, timedelta
from urllib.parse import urlsplit

from agoranomos_pipelines import meta_ads

now = datetime.fromisoformat(sys.argv[1])
faked = json.loads(sys.argv[2])


class Answer:
    def __init__(self, body, status_code=200):
        self.status_code, self.body, self.headers = status_code, body, {}

    def json(self):
        return json.loads(json.dumps(self.body))

    @property
    def text(self):
        return json.dumps(self.body)


def days(params):
    found = json.loads(params["time_range"])
    first, last = date.fromisoformat(found["since"]), date.fromisoformat(found["until"])
    return [(first + timedelta(days=n)).isoformat() for n in range((last - first).days + 1)]


class Graph:
    def __init__(self, **settings):
        pass

    def get(self, url, params, headers):
        parts = urlsplit(url)
        path = parts.path.removeprefix(f"/{meta_ads.API_VERSION}/")
        # The token goes in the header Authorization of each call, and never in a URL.
        assert "access_token" not in params, "the token in a URL"
        assert "access_token" not in parts.query, "the token in a URL"
        token = os.environ["META_ADS_ACCESS_TOKEN"]
        assert headers == {"Authorization": "Bearer " + token}, "a request without the token"
        account_id = path.split("/")[0].removeprefix("act_")
        account = faked[account_id]
        if "/" not in path:
            return Answer(account["account"])
        edge = path.split("/")[1]
        if edge != "insights":
            # The lists refuse DELETED in effective_status, as Meta does.
            if "DELETED" in json.loads(params.get("effective_status", "[]")):
                refused = {
                    "message": "Invalid parameter",
                    "type": "OAuthException",
                    "code": 100,
                    "error_subcode": 1815001,
                    "error_user_title": "Cannot request deleted objects",
                    "error_user_msg": (
                        "Requests for deleted objects are not supported at this endpoint"
                    ),
                }
                return Answer({"error": refused}, 400)
            return Answer({"data": account[edge], "paging": {}})
        rows = []
        for day in days(params):
            for row in account["insights"][params["level"]]:
                rows.append({**row, "date_start": day, "date_stop": day})
        return Answer({"data": rows, "paging": {}})

    def post(self, url, data, headers):
        raise AssertionError("a daily load asks no report run")


meta_ads.Client = Graph
counts = meta_ads.load(now)
print(json.dumps(counts, sort_keys=True))
"""


def answers() -> dict:
    """The answers of the Graph API for each account of the test, by request: the account, its
    lists, and the rows of insights of each level for one day."""
    found = {}
    for account_id in ACCOUNTS:
        named = {"account_id": account_id, "account_name": f"Stack test {account_id[-1]}"}
        named["account_currency"] = "USD"
        campaigns, ad_sets, ads = [], [], []
        for campaign_id, name, status in CAMPAIGNS:
            listed = {"id": campaign_id, "account_id": account_id, "name": name}
            listed |= {"status": status, "configured_status": status, "effective_status": status}
            campaigns.append(listed | {"objective": "OUTCOME_SALES"})
        for ad_set_id, campaign_id, name in AD_SETS:
            [status] = [s for c, _, s in CAMPAIGNS if c == campaign_id]
            ad_sets.append(
                {
                    "id": ad_set_id,
                    "account_id": account_id,
                    "campaign_id": campaign_id,
                    "name": name,
                    "status": status,
                    "configured_status": status,
                    "effective_status": status,
                    "optimization_goal": "OFFSITE_CONVERSIONS",
                }
            )
        for ad_id, ad_set_id, campaign_id, name, status in LISTED_ADS:
            ads.append(
                {
                    "id": ad_id,
                    "account_id": account_id,
                    "campaign_id": campaign_id,
                    "adset_id": ad_set_id,
                    "name": name,
                    "status": status,
                    "configured_status": status,
                    "effective_status": status,
                }
            )
        impressions, clicks, spend = TOTALS[account_id]
        insights = {
            "account": [
                {
                    "account_id": account_id,
                    "impressions": impressions,
                    "inline_link_clicks": clicks,
                    "spend": spend,
                }
            ],
            "campaign": [],
            "adset": [],
            "ad": [],
        }

        def measures(impressions, clicks, spend):
            return {
                "impressions": impressions,
                "inline_link_clicks": clicks,
                "clicks": str(int(clicks) + 5),
                "spend": spend,
            }

        for campaign_id, name, _ in CAMPAIGNS:
            counted, valued = ACTIONS[campaign_id]
            converted = {"attribution_setting": "7d_click_1d_view", "actions": counted}
            converted["action_values"] = valued
            row = named | {"campaign_id": campaign_id, "campaign_name": name}
            insights["campaign"].append(row | measures(*DAY[campaign_id]) | converted)
            [(ad_set_id, ad_set_name)] = [(s, n) for s, c, n in AD_SETS if c == campaign_id]
            row = named | {"campaign_id": campaign_id, "adset_id": ad_set_id}
            row["adset_name"] = ad_set_name
            insights["adset"].append(row | measures(*DAY[campaign_id]) | converted)
        counted, valued = CONVERTED_ACTIONS
        converted = {"attribution_setting": "7d_click_1d_view", "actions": counted}
        row = named | {"campaign_id": CONVERTED[0], "campaign_name": CONVERTED[1]}
        insights["campaign"].append(
            row | measures("0", "0", "0") | converted | {"action_values": valued}
        )
        row = named | {"campaign_id": ABSENT[0], "campaign_name": ABSENT[1]}
        row |= {"inline_link_clicks": "0", "spend": "0"}
        insights["campaign"].append(row | converted | {"action_values": valued})
        for ad_id, ad_set_id, campaign_id, name, *_ in (*LISTED_ADS, STUB):
            row = named | {"campaign_id": campaign_id, "adset_id": ad_set_id, "ad_id": ad_id}
            row["ad_name"] = name
            insights["ad"].append(row | measures(*ADS[ad_id]) | {"actions": []})
        found[account_id] = {
            "account": {
                "id": f"act_{account_id}",
                "account_id": account_id,
                "name": named["account_name"],
                "currency": "USD",
                "timezone_name": "America/New_York",
                "account_status": 1,
            },
            "campaigns": campaigns,
            "adsets": ad_sets,
            "ads": ads,
            "insights": insights,
        }
    return found


# The rows of the two accounts everywhere that the load and the run wrote them, in one transaction
# with the thaw of the facts (ADR-0035), as the superuser: the child tables of raw by their row,
# the tables of raw by account, the loads of dlt that only these accounts had, the facts and the
# journals of restatements, the dimensions and their versions, the totals, the quarantine, the
# loads of the core and the registry of accounts, and the rows of the tables of staging that the
# run filled, which the next run fills anew. A table that does not exist is passed over.
REMOVE = """
BEGIN;
SELECT set_config('agoranomos.thaw', pg_current_xact_id()::text, true) <> '';
DO $$
DECLARE
    t record;
    accounts text[] := ARRAY[{accounts}];
    keys integer[];
BEGIN
    FOR t IN
        SELECT c.table_name, split_part(c.table_name, '__', 1) AS parent
        FROM information_schema.columns AS c
        WHERE c.table_schema = 'raw_meta_ads' AND c.column_name = '_dlt_parent_id'
    LOOP
        EXECUTE format(
            'DELETE FROM raw_meta_ads.%I AS c USING raw_meta_ads.%I AS r '
            'WHERE c._dlt_parent_id = r._dlt_id AND r.account_id = ANY($1)',
            t.table_name, t.parent
        ) USING accounts;
    END LOOP;
    IF to_regclass('raw_meta_ads._agoranomos_loads') IS NOT NULL
        AND to_regclass('raw_meta_ads._dlt_loads') IS NOT NULL THEN
        DELETE FROM raw_meta_ads._dlt_loads AS l
        WHERE l.load_id IN (
            SELECT _dlt_load_id FROM raw_meta_ads._agoranomos_loads
            WHERE account_id = ANY(accounts)
        )
            AND NOT EXISTS (
                SELECT FROM raw_meta_ads._agoranomos_loads AS j
                WHERE j._dlt_load_id = l.load_id AND NOT j.account_id = ANY(accounts)
            );
    END IF;
    FOR t IN
        SELECT c.table_name
        FROM information_schema.columns AS c
        WHERE c.table_schema = 'raw_meta_ads' AND c.column_name = 'account_id'
    LOOP
        EXECUTE format('DELETE FROM raw_meta_ads.%I WHERE account_id = ANY($1)', t.table_name)
            USING accounts;
    END LOOP;
    keys := ARRAY(
        SELECT account_sk FROM core.dim_account
        WHERE platform = 'meta_ads' AND account_id = ANY(accounts)
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
    DELETE FROM core.platform_totals_daily
        WHERE source = 'meta_ads' AND account_id = ANY(accounts);
    DELETE FROM core.account_quarantine
        WHERE source = 'meta_ads' AND account_id = ANY(accounts);
    DELETE FROM core.loads WHERE source = 'meta_ads' AND account_id = ANY(accounts);
    DELETE FROM core.dim_account WHERE account_sk = ANY(keys);
    IF to_regclass('staging.int_g1_failures') IS NOT NULL THEN
        DELETE FROM staging.int_g1_failures
            WHERE source = 'meta_ads' AND account_id = ANY(accounts);
    END IF;
    IF to_regclass('staging.stg_loads__newest_days') IS NOT NULL THEN
        DELETE FROM staging.stg_loads__newest_days
            WHERE source = 'meta_ads' AND account_id = ANY(accounts);
    END IF;
END $$;
COMMIT;
""".replace("{accounts}", ", ".join(f"'{account}'" for account in ACCOUNTS))


def rows(env, sql, role="postgres"):
    return warehouse_output(env, sql, role).splitlines()


def idle(airflow_url, token):
    wait_for(
        lambda: not is_busy(airflow_url, token, TRANSFORM, assets=ASSETS),
        1800,
        interval=5,
        message="the runs of the transform did not end",
    )


def test_a_load_of_meta_reaches_the_core_and_the_marts(env, airflow_url, token):
    folder = env.get("AGORANOMOS_EXTENSIONS_DIR") or "./extensions"
    path = (REPO_ROOT / folder).resolve() / "model" / "clients.toml"
    assert not path.exists(), "extensions/model/clients.toml is in the way of this test"
    wait_for_dag(airflow_url, token, TRANSFORM)
    idle(airflow_url, token)
    # Left behind by a test that was stopped.
    warehouse_output(env, REMOVE)
    now = last_moment(env, "demo_social")
    path.write_text(CLIENTS, encoding="utf-8")
    try:
        run_in_tasks(
            env,
            LOAD,
            now.isoformat(),
            json.dumps(answers()),
            environment={"META_ADS_ACCESS_TOKEN": TOKEN},
        )
        # The load took the usual window of each account, the last 28 days to yesterday in its
        # time zone, and recorded both in its journal.
        windows = rows(
            env,
            "SELECT account_id || '|' || window_from || '|' || window_to || '|' || status "  # noqa: S608 (IDs of the test)
            f"FROM raw_meta_ads._agoranomos_loads WHERE account_id IN {IN_ACCOUNTS} ORDER BY 1",
        )
        assert len(windows) == 2, windows
        _, first, last, status = windows[0].split("|")
        first, last = date.fromisoformat(first), date.fromisoformat(last)
        assert status == "loaded"
        assert last - first == timedelta(days=27), (first, last)
        assert [w.split("|")[1:] for w in windows[1:]] == [[str(first), str(last), "loaded"]]
        days = (last - first).days + 1
        unpause(airflow_url, token, TRANSFORM)
        run = trigger(airflow_url, token, TRANSFORM)
        run = wait_for_run(airflow_url, token, TRANSFORM, run["dag_run_id"], timeout=1800)
        assert run["state"] == "success", run["state"]

        # G0a held the account whose campaigns do not add up to its totals, and only it: its load
        # stays pinned, and the core accepted the other.
        quarantined = rows(
            env,
            "SELECT platform || '|' || account_id || '|' || reason FROM ops.ops_quarantine "
            "WHERE platform = 'meta_ads' ORDER BY 1",
            "metabase_reader",
        )
        assert quarantined == [f"meta_ads|{DIFFERING}|totals_differ"], quarantined
        statuses = rows(
            env,
            "SELECT account_id || '|' || status FROM core.loads "  # noqa: S608 (IDs of the test)
            f"WHERE source = 'meta_ads' AND account_id IN {IN_ACCOUNTS} ORDER BY 1",
        )
        assert statuses == [f"{HEALTHY}|accepted", f"{DIFFERING}|pinned"], statuses
        registered = rows(
            env,
            "SELECT account_id || '|' || client_id || '|' || is_active || '|' || is_stub "  # noqa: S608 (IDs of the test)
            f"FROM core.dim_account WHERE platform = 'meta_ads' AND account_id IN {IN_ACCOUNTS}",
        )
        assert registered == [f"{HEALTHY}|meta-stack-test|true|false"], registered

        # The core holds every day of the healthy account, and so does the view of the marts,
        # with the primary actions of the seed, the purchases and the leads of the pixel, and no
        # metric of conversions of the platform, which Meta does not have. Each day has a row of
        # each deleted campaign too, with zeros, for its purchase.
        spend = sum(Decimal(s) for _, _, s in DAY.values())
        impressions = sum(int(i) for i, _, _ in DAY.values())
        clicks = sum(int(c) for _, c, _ in DAY.values())
        core = warehouse_output(
            env,
            "SELECT count(*) || '|' || sum(f.impressions) || '|' || sum(f.clicks) || '|' || "  # noqa: S608 (IDs of the test)
            "sum(f.spend) FROM core.ad_stats_campaign_daily AS f "
            "JOIN core.dim_account AS a ON a.account_sk = f.account_sk "
            f"WHERE a.platform = 'meta_ads' AND a.account_id = '{HEALTHY}'",
        ).split("|")
        assert [int(core[0]), int(core[1]), int(core[2]), Decimal(core[3])] == [
            (len(DAY) + 2) * days,
            impressions * days,
            clicks * days,
            spend * days,
        ], core
        shown = warehouse_output(
            env,
            "SELECT count(DISTINCT account_id) || '|' || count(*) || '|' || sum(impressions) "
            "|| '|' || sum(clicks) || '|' || sum(spend) || '|' || sum(conversions_primary) "
            "|| '|' || count(conversions) || '|' || min(currency) "
            "|| '|' || sum(conversion_value_primary) || '|' || count(conversion_value) "
            "|| '|' || round(sum(conversion_value_primary_reporting) "
            "/ nullif(sum(spend_reporting), 0), 6) "
            "|| '|' || coalesce((sum(conversion_value_reporting) "
            "/ nullif(sum(spend_reporting), 0))::text, 'none') "
            "FROM marts.ad_stats_campaign_daily WHERE platform = 'meta_ads'",
            "metabase_reader",
        ).split("|")
        assert shown[:3] == ["1", str((len(DAY) + 2) * days), str(impressions * days)], shown
        assert [int(shown[3]), Decimal(shown[4]), Decimal(shown[5])] == [
            clicks * days,
            spend * days,
            Decimal(5 * days),
        ], shown
        assert shown[6:8] == ["0", "USD"], shown
        # The value of the primary actions, the purchases of the pixel of the two campaigns that
        # have one: Primary ROAS has a value on Meta, and ROAS, of the own metric, none
        # (ADR-0056). The account is in USD, the reporting currency of the stack.
        purchases = (ACTIONS[CAMPAIGNS[0][0]][1][0], CONVERTED_ACTIONS[1][0])
        value = sum(Decimal(v["value"]) for v in purchases)
        assert [Decimal(shown[8]), shown[9]] == [value * days, "0"], shown
        assert abs(Decimal(shown[10]) - value / spend) <= Decimal("0.000001"), shown
        assert shown[11] == "none", shown
        # The row of the deleted campaign on each day: its zeros, as Meta gave them, its name from
        # its statistics, and the purchase of the pixel in conversions_primary.
        zeros = rows(
            env,
            "SELECT campaign_name || '|' || impressions || '|' || clicks || '|' || spend || '|' "  # noqa: S608 (IDs of the test)
            "|| coalesce(conversions::text, 'none') || '|' || conversions_primary "
            "|| '|' || is_listed FROM marts.ad_stats_campaign_daily "
            f"WHERE platform = 'meta_ads' AND campaign_id = '{CONVERTED[0]}' ORDER BY report_date",
            "metabase_reader",
        )
        assert zeros == [f"{CONVERTED[1]}|0|0|0.000000|none|1.000000|false"] * days, zeros
        # The rows without impressions and clicks: 0 impressions in the core, its purchase in the
        # conversions of the core, and the same row in the view of the marts.
        absent = warehouse_output(
            env,
            "SELECT count(*) || '|' || sum(f.impressions) || '|' || sum(f.clicks) || '|' "  # noqa: S608 (IDs of the test)
            "|| sum(f.spend) FROM core.ad_stats_campaign_daily AS f "
            "JOIN core.dim_account AS a ON a.account_sk = f.account_sk "
            f"WHERE a.platform = 'meta_ads' AND a.account_id = '{HEALTHY}' "
            f"AND f.campaign_id = {ABSENT[0]}",
        ).split("|")
        assert [int(absent[0]), int(absent[1]), int(absent[2]), Decimal(absent[3])] == [
            days,
            0,
            0,
            Decimal(0),
        ], absent
        purchases = warehouse_output(
            env,
            "SELECT count(*) || '|' || sum(f.conversions) || '|' || sum(f.conversion_value) "  # noqa: S608 (IDs of the test)
            "FROM core.ad_conversions_campaign_daily AS f "
            "JOIN core.dim_account AS a ON a.account_sk = f.account_sk "
            f"WHERE a.platform = 'meta_ads' AND a.account_id = '{HEALTHY}' "
            f"AND f.campaign_id = {ABSENT[0]}",
        ).split("|")
        assert [int(purchases[0]), Decimal(purchases[1]), Decimal(purchases[2])] == [
            days,
            Decimal(days),
            Decimal("49.90") * days,
        ], purchases
        shown_absent = rows(
            env,
            "SELECT campaign_name || '|' || impressions || '|' || clicks || '|' || spend || '|' "  # noqa: S608 (IDs of the test)
            "|| conversions_primary FROM marts.ad_stats_campaign_daily "
            f"WHERE platform = 'meta_ads' AND campaign_id = '{ABSENT[0]}' ORDER BY report_date",
            "metabase_reader",
        )
        assert shown_absent == [f"{ABSENT[1]}|0|0|0.000000|1.000000"] * days, shown_absent
        actions = rows(
            env,
            "SELECT conversion_action_id || '|' || conversion_category || '|' || is_primary "
            "|| '|' || sum(conversions) || '|' || sum(conversion_value) "
            "FROM marts.ad_conversions_campaign_daily WHERE platform = 'meta_ads' "
            "GROUP BY conversion_action_id, conversion_category, is_primary ORDER BY 1",
            "metabase_reader",
        )
        assert [a.rsplit("|", 2)[0] for a in actions] == [
            "link_click|other|false",
            "offsite_conversion.fb_pixel_lead|lead|true",
            "offsite_conversion.fb_pixel_purchase|purchase|true",
            "omni_purchase|purchase|false",
        ], actions
        totals = warehouse_output(
            env,
            "SELECT count(*) || '|' || sum(impressions) || '|' || sum(clicks) || '|' || "  # noqa: S608 (IDs of the test)
            "sum(spend) FROM core.platform_totals_daily "
            f"WHERE source = 'meta_ads' AND account_id = '{HEALTHY}'",
        ).split("|")
        assert [int(totals[0]), int(totals[1]), int(totals[2]), Decimal(totals[3])] == [
            days,
            impressions * days,
            clicks * days,
            spend * days,
        ], totals

        # The common state of Meta: ACTIVE is active and ARCHIVED removed. The deleted ad that
        # only the statistics name is a stub of its ad set, with the name of its statistics, and
        # the listed ads are listed.
        states = rows(
            env,
            "SELECT m.campaign_id_text || '|' || coalesce(m.campaign_status, '') || '|' "  # noqa: S608 (IDs of the test)
            "|| m.campaign_state "
            "FROM core.dim_campaign AS m JOIN core.dim_account AS a ON a.account_sk = m.account_sk "
            f"WHERE a.platform = 'meta_ads' AND a.account_id = '{HEALTHY}' ORDER BY 1",
        )
        assert states == [
            "120200000000101|ACTIVE|active",
            "120200000000102|ARCHIVED|removed",
            f"{CONVERTED[0]}||other",
            f"{ABSENT[0]}||other",
        ], states
        ads = rows(
            env,
            "SELECT d.ad_id_text || '|' || d.ad_group_id || '|' || coalesce(d.ad_name, '') || '|' "  # noqa: S608 (IDs of the test)
            "|| d.is_listed || '|' || (d.last_seen_at IS NULL) "
            "FROM core.dim_ad AS d JOIN core.dim_account AS a ON a.account_sk = d.account_sk "
            f"WHERE a.platform = 'meta_ads' AND a.account_id = '{HEALTHY}' ORDER BY 1",
        )
        assert ads == [
            "120200000000301|120200000000201|Boots, red|true|false",
            "120200000000302|120200000000202|Socks, grey|true|false",
            "120200000000303|120200000000201|Boots, blue|false|true",
        ], ads
        stats = warehouse_output(
            env,
            "SELECT count(*) || '|' || sum(f.spend) FROM core.ad_stats_ad_daily AS f "  # noqa: S608 (IDs of the test)
            "JOIN core.dim_account AS a ON a.account_sk = f.account_sk "
            f"WHERE a.platform = 'meta_ads' AND a.account_id = '{HEALTHY}' "
            f"AND f.ad_id = {STUB[0]}",
        ).split("|")
        assert [int(stats[0]), Decimal(stats[1])] == [days, Decimal(ADS[STUB[0]][2]) * days]

        # A run that reads no new load of Meta, as one after the short cycle of Google Ads: the
        # one pinned account of Meta is the one that the run before held, and it is one of the two
        # accounts that the settings name, so the source goes on, with no source_stopped
        # (quarantine_stop_findings in dbt/macros/quarantine.sql, ADR-0035).
        run = trigger(airflow_url, token, TRANSFORM)
        run = wait_for_run(airflow_url, token, TRANSFORM, run["dag_run_id"], timeout=1800)
        assert run["state"] == "success", run["state"]
        quarantined = rows(
            env,
            "SELECT platform || '|' || account_id || '|' || reason FROM ops.ops_quarantine "
            "WHERE platform = 'meta_ads' ORDER BY 1",
            "metabase_reader",
        )
        assert quarantined == [f"meta_ads|{DIFFERING}|totals_differ"], quarantined
    finally:
        idle(airflow_url, token)
        warehouse_output(env, REMOVE)
        path.unlink(missing_ok=True)
    assert (
        rows(
            env,
            f"SELECT account_id FROM core.dim_account WHERE platform = 'meta_ads' "  # noqa: S608 (IDs of the test)
            f"AND account_id IN {IN_ACCOUNTS}",
        )
        == []
    )
