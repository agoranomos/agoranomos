"""TikTok in the core and the marts (ADR-0053, task C5d of #225): a load of the connector of TikTok,
through the real dlt source of airflow/dags/agoranomos_pipelines/tiktok_ads.py with the HTTP of the
TikTok API for Business faked, reaches the core and marts.ad_stats_campaign_daily through the next
run of the transform; an advertiser whose campaigns do not add up to its totals goes to quarantine
with totals_differ (G0a of ADR-0035), and one with an ID that does not fit bigint with invalid_id,
while the healthy one goes on; a deleted campaign, ad group and ad are removed, from their
secondary_status; and an ad that only the statistics name gets a stub in its dimension (ADR-0036).

No load reaches TikTok, and no answer comes from a real advertiser: the answers below are faked by
hand from the reference of the API v1.3, as tests/unit/test_tiktok_ads_source.py fakes them, and
the stand-in of the HTTP client of dlt answers each request with them in the container of the
tasks. The test names four TikTok advertisers of its own, with IDs that no advertiser has, in
extensions/model/clients.toml, loads them as of the newest load of demo_video, so that the rates
of the demo cover their days, and runs the transform. In its finally it deletes every row of the
four advertisers from raw_tiktok_ads, from the core, as the superuser with the thaw of ADR-0035,
from the tables of staging that the run wrote, and the file, so that the demo is as before and no
later test finds an advertiser that left the settings. If it is killed before that, run the
cleanup of REMOVE by hand as the superuser of the warehouse, and delete
extensions/model/clients.toml.

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
# The four advertisers of the test: one whose data is healthy, one whose totals are 10% above the
# sum of its campaigns, which G0a holds out with a tolerance of 1% for TikTok, one whose deleted
# campaign has an ID above the largest bigint, 9223372036854775807, which G1 holds out with
# invalid_id rather than cut it or fail the run, and a second healthy one. With it the two held out
# are half of the advertisers of TikTok, and only more than half stops the source and holds
# the healthy ones too, with source_stopped (quarantine_stop_findings in dbt/macros/quarantine.sql).
HEALTHY, DIFFERING, INVALID = "7900000000000000001", "7900000000000000002", "7900000000000000003"
ALSO_HEALTHY = "7900000000000000004"
ADVERTISERS = (HEALTHY, DIFFERING, INVALID, ALSO_HEALTHY)
OVERSIZED = {"1800000000000102": "9300000000000000102"}
IN_ADVERTISERS = "('" + "', '".join(ADVERTISERS) + "')"
CLIENTS = f"""\
[clients.tiktok-stack-test]
name = "TikTok of the stack test"

[[accounts]]
platform = "tiktok_ads"
account_id = "{HEALTHY}"
client = "tiktok-stack-test"

[[accounts]]
platform = "tiktok_ads"
account_id = "{DIFFERING}"
client = "tiktok-stack-test"

[[accounts]]
platform = "tiktok_ads"
account_id = "{INVALID}"
client = "tiktok-stack-test"

[[accounts]]
platform = "tiktok_ads"
account_id = "{ALSO_HEALTHY}"
client = "tiktok-stack-test"
"""
# A stand-in of the long-term token, of low entropy: no request leaves the container.
TOKEN = "stand-in-token-of-the-tiktok-stack-test"

# Each day of each advertiser, faked by hand: two campaigns, an enabled one and a deleted one, an
# ad group in each, and three ads, of which the lists give two: the third, an ad that the lists no
# longer return, only the report of ads names. Each entity with its operation_status and its
# secondary_status, as the lists of TikTok give them.
CAMPAIGNS = (
    ("1800000000000101", "Boots", "ENABLE", "CAMPAIGN_STATUS_ENABLE"),
    ("1800000000000102", "Socks", "DISABLE", "CAMPAIGN_STATUS_DELETE"),
)
AD_GROUPS = (
    ("1800000000000201", "1800000000000101", "Boots, men", "ENABLE", "ADGROUP_STATUS_DELIVERY_OK"),
    (
        "1800000000000202",
        "1800000000000102",
        "Socks, all",
        "ENABLE",
        "ADGROUP_STATUS_CAMPAIGN_DELETE",
    ),
)
LISTED_ADS = (
    (
        "1800000000000301",
        "1800000000000201",
        "1800000000000101",
        "Boots, red",
        "ENABLE",
        "AD_STATUS_DELIVERY_OK",
    ),
    (
        "1800000000000302",
        "1800000000000202",
        "1800000000000102",
        "Socks, grey",
        "ENABLE",
        "AD_STATUS_CAMPAIGN_DELETE",
    ),
)
STUB = ("1800000000000303", "1800000000000201", "1800000000000101", "Boots, blue")
# The metrics of a day: impressions, clicks (destination), spend, the conversions of the
# optimization event, the complete payments and their value, of each campaign and its ad group,
# and of each ad. "-" is a metric that means nothing on the day, as TikTok writes it.
DAY = {
    "1800000000000101": ("1000", "20", "12.34", "3", "2", "99.50"),
    "1800000000000102": ("500", "5", "3.21", "1", "-", "0.00"),
}
ADS = {
    "1800000000000301": ("600", "12", "7.00", "2", "1", "49.75"),
    "1800000000000303": ("400", "8", "5.34", "1", "1", "49.75"),
    "1800000000000302": ("500", "5", "3.21", "1", "-", "0.00"),
}
# The totals of a day of each advertiser: the sum of the campaigns, and 10% more spend for the one
# that G0a holds out.
TOTALS = {
    HEALTHY: ("1500", "25", "15.55"),
    DIFFERING: ("1500", "25", "17.11"),
    INVALID: ("1500", "25", "15.55"),
    ALSO_HEALTHY: ("1500", "25", "15.55"),
}

# A load of TikTok as of a moment, through tiktok_ads.load, with the HTTP client of dlt replaced by
# a stand-in that answers each request of the API with the answers above, given as JSON: the
# advertiser, its lists, and the rows of a report of each level for every day from start_date to
# end_date. dlt reads the password of loader from the environment of the container, and the script
# prints the rows of each table.
LOAD = """
import json
import sys
from datetime import date, datetime, timedelta
from urllib.parse import urlsplit

from agoranomos_pipelines import tiktok_ads

now = datetime.fromisoformat(sys.argv[1])
faked = json.loads(sys.argv[2])
LEVELS = {
    "AUCTION_ADVERTISER": "advertiser",
    "AUCTION_CAMPAIGN": "campaign",
    "AUCTION_ADGROUP": "adgroup",
    "AUCTION_AD": "ad",
}


class Answer:
    def __init__(self, rows):
        self.status_code, self.headers = 200, {}
        self.body = {
            "code": 0,
            "message": "OK",
            "request_id": "stack-test",
            "data": {"list": rows, "page_info": {"page": 1, "total_page": 1}},
        }

    def json(self):
        return json.loads(json.dumps(self.body))


def days(params):
    first = date.fromisoformat(params["start_date"])
    last = date.fromisoformat(params["end_date"])
    return [(first + timedelta(days=n)).isoformat() for n in range((last - first).days + 1)]


class Api:
    def __init__(self, **settings):
        pass

    def get(self, url, params, headers):
        assert headers.get("Access-Token"), "a request without the token"
        path = urlsplit(url).path.removeprefix(f"/open_api/{tiktok_ads.API_VERSION}/")
        if path == "advertiser/info/":
            [advertiser_id] = json.loads(params["advertiser_ids"])
            return Answer([faked[advertiser_id]["advertiser"]])
        advertiser = faked[params["advertiser_id"]]
        if path != "report/integrated/get/":
            return Answer(advertiser[path.split("/")[0]])
        rows = []
        for day in days(params):
            for row in advertiser["report"][LEVELS[params["data_level"]]]:
                dimensions = {**row["dimensions"], "stat_time_day": f"{day} 00:00:00"}
                rows.append({"dimensions": dimensions, "metrics": row["metrics"]})
        return Answer(rows)


tiktok_ads.Client = Api
counts = tiktok_ads.load(now)
print(json.dumps(counts, sort_keys=True))
"""


def answers() -> dict:
    """The answers of the API for each advertiser of the test, by request: the advertiser, its
    lists, and the rows of the report of each level for one day."""
    found = {}
    for advertiser_id in ADVERTISERS:
        # The IDs of the campaigns as the API writes them for the advertiser: the deleted one of
        # INVALID does not fit bigint, in its lists and its reports.
        ids = OVERSIZED if advertiser_id == INVALID else {}

        def cid(campaign_id, ids=ids):
            return ids.get(campaign_id, campaign_id)

        campaigns = [
            {
                "advertiser_id": advertiser_id,
                "campaign_id": cid(campaign_id),
                "campaign_name": name,
                "objective_type": "WEB_CONVERSIONS",
                "operation_status": operation,
                "secondary_status": secondary,
            }
            for campaign_id, name, operation, secondary in CAMPAIGNS
        ]
        ad_groups = [
            {
                "advertiser_id": advertiser_id,
                "campaign_id": cid(campaign_id),
                "adgroup_id": ad_group_id,
                "adgroup_name": name,
                "optimization_goal": "CONVERT",
                "operation_status": operation,
                "secondary_status": secondary,
            }
            for ad_group_id, campaign_id, name, operation, secondary in AD_GROUPS
        ]
        ads = [
            {
                "advertiser_id": advertiser_id,
                "campaign_id": cid(campaign_id),
                "adgroup_id": ad_group_id,
                "ad_id": ad_id,
                "ad_name": name,
                "ad_format": "SINGLE_VIDEO",
                "operation_status": operation,
                "secondary_status": secondary,
            }
            for ad_id, ad_group_id, campaign_id, name, operation, secondary in LISTED_ADS
        ]

        def metrics(impressions, clicks, spend, conversion, payments, value):
            return {
                "currency": "USD",
                "impressions": impressions,
                "clicks": clicks,
                "spend": spend,
                "conversion": conversion,
                "complete_payment": payments,
                "total_complete_payment_rate": value,
            }

        impressions, clicks, spend = TOTALS[advertiser_id]
        report = {
            "advertiser": [
                {
                    "dimensions": {"advertiser_id": advertiser_id},
                    "metrics": {
                        "currency": "USD",
                        "impressions": impressions,
                        "clicks": clicks,
                        "spend": spend,
                    },
                }
            ],
            "campaign": [],
            "adgroup": [],
            "ad": [],
        }
        for campaign_id, name, *_ in CAMPAIGNS:
            report["campaign"].append(
                {
                    "dimensions": {"campaign_id": cid(campaign_id)},
                    "metrics": {"campaign_name": name, **metrics(*DAY[campaign_id])},
                }
            )
        for ad_group_id, campaign_id, name, *_ in AD_GROUPS:
            named = {"campaign_id": cid(campaign_id), "adgroup_name": name}
            report["adgroup"].append(
                {
                    "dimensions": {"adgroup_id": ad_group_id},
                    "metrics": named | metrics(*DAY[campaign_id]),
                }
            )
        for ad_id, ad_group_id, campaign_id, name, *_ in (*LISTED_ADS, STUB):
            named = {"campaign_id": cid(campaign_id), "adgroup_id": ad_group_id, "ad_name": name}
            report["ad"].append(
                {"dimensions": {"ad_id": ad_id}, "metrics": named | metrics(*ADS[ad_id])}
            )
        found[advertiser_id] = {
            "advertiser": {
                "advertiser_id": advertiser_id,
                "name": f"Stack test {advertiser_id[-1]}",
                "currency": "USD",
                "timezone": "Etc/GMT+8",
                "display_timezone": "America/Los_Angeles",
                "status": "STATUS_ENABLE",
            },
            "campaign": campaigns,
            "adgroup": ad_groups,
            "ad": ads,
            "report": report,
        }
    return found


# The rows of the four advertisers everywhere that the load and the run wrote them, in one
# transaction with the thaw of the facts (ADR-0035), as the superuser: the tables of raw by
# advertiser, the loads of dlt that only these advertisers had, the facts and the journals of
# restatements, the dimensions and their versions, the totals, the quarantine, the loads of the
# core and the registry of accounts, and the rows of the tables of staging that the run filled,
# which the next run fills anew. A table that does not exist is passed over.
REMOVE = """
BEGIN;
SELECT set_config('agoranomos.thaw', pg_current_xact_id()::text, true) <> '';
DO $$
DECLARE
    t record;
    accounts text[] := ARRAY[{accounts}];
    keys integer[];
BEGIN
    IF to_regclass('raw_tiktok_ads._agoranomos_loads') IS NOT NULL
        AND to_regclass('raw_tiktok_ads._dlt_loads') IS NOT NULL THEN
        DELETE FROM raw_tiktok_ads._dlt_loads AS l
        WHERE l.load_id IN (
            SELECT _dlt_load_id FROM raw_tiktok_ads._agoranomos_loads
            WHERE account_id = ANY(accounts)
        )
            AND NOT EXISTS (
                SELECT FROM raw_tiktok_ads._agoranomos_loads AS j
                WHERE j._dlt_load_id = l.load_id AND NOT j.account_id = ANY(accounts)
            );
    END IF;
    FOR t IN
        SELECT c.table_name, c.column_name
        FROM information_schema.columns AS c
        WHERE c.table_schema = 'raw_tiktok_ads'
            AND c.column_name IN ('advertiser_id', 'account_id')
    LOOP
        EXECUTE format(
            'DELETE FROM raw_tiktok_ads.%I WHERE %I = ANY($1)', t.table_name, t.column_name
        ) USING accounts;
    END LOOP;
    keys := ARRAY(
        SELECT account_sk FROM core.dim_account
        WHERE platform = 'tiktok_ads' AND account_id = ANY(accounts)
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
        WHERE source = 'tiktok_ads' AND account_id = ANY(accounts);
    DELETE FROM core.account_quarantine
        WHERE source = 'tiktok_ads' AND account_id = ANY(accounts);
    DELETE FROM core.loads WHERE source = 'tiktok_ads' AND account_id = ANY(accounts);
    DELETE FROM core.dim_account WHERE account_sk = ANY(keys);
    IF to_regclass('staging.int_g1_failures') IS NOT NULL THEN
        DELETE FROM staging.int_g1_failures
            WHERE source = 'tiktok_ads' AND account_id = ANY(accounts);
    END IF;
    IF to_regclass('staging.stg_loads__newest_days') IS NOT NULL THEN
        DELETE FROM staging.stg_loads__newest_days
            WHERE source = 'tiktok_ads' AND account_id = ANY(accounts);
    END IF;
END $$;
COMMIT;
""".replace("{accounts}", ", ".join(f"'{advertiser}'" for advertiser in ADVERTISERS))


def rows(env, sql, role="postgres"):
    return warehouse_output(env, sql, role).splitlines()


def idle(airflow_url, token):
    wait_for(
        lambda: not is_busy(airflow_url, token, TRANSFORM, assets=ASSETS),
        1800,
        interval=5,
        message="the runs of the transform did not end",
    )


def test_a_load_of_tiktok_reaches_the_core_and_the_marts(env, airflow_url, token):
    folder = env.get("AGORANOMOS_EXTENSIONS_DIR") or "./extensions"
    path = (REPO_ROOT / folder).resolve() / "model" / "clients.toml"
    assert not path.exists(), "extensions/model/clients.toml is in the way of this test"
    wait_for_dag(airflow_url, token, TRANSFORM)
    idle(airflow_url, token)
    # Left behind by a test that was stopped.
    warehouse_output(env, REMOVE)
    now = last_moment(env, "demo_video")
    path.write_text(CLIENTS, encoding="utf-8")
    try:
        run_in_tasks(
            env,
            LOAD,
            now.isoformat(),
            json.dumps(answers()),
            environment={"TIKTOK_ADS_ACCESS_TOKEN": TOKEN},
        )
        # The load took the usual window of each advertiser, the last 28 days to yesterday in its
        # time zone, and recorded each in its journal.
        windows = rows(
            env,
            "SELECT account_id || '|' || window_from || '|' || window_to || '|' || status "  # noqa: S608 (IDs of the test)
            f"FROM raw_tiktok_ads._agoranomos_loads WHERE account_id IN {IN_ADVERTISERS} "
            "ORDER BY 1",
        )
        assert len(windows) == 4, windows
        _, first, last, status = windows[0].split("|")
        first, last = date.fromisoformat(first), date.fromisoformat(last)
        assert status == "loaded"
        assert last - first == timedelta(days=27), (first, last)
        assert [w.split("|")[1:] for w in windows[1:]] == [[str(first), str(last), "loaded"]] * 3
        days = (last - first).days + 1
        unpause(airflow_url, token, TRANSFORM)
        run = trigger(airflow_url, token, TRANSFORM)
        run = wait_for_run(airflow_url, token, TRANSFORM, run["dag_run_id"], timeout=1800)
        assert run["state"] == "success", run["state"]

        # G0a held the advertiser whose campaigns do not add up to its totals, and G1 the one with
        # an ID that does not fit bigint, with invalid_id, and missing_key for the rows of its
        # reports that the cast left without a key: their loads stay pinned, the run succeeded,
        # and the core accepted the two healthy ones, as no more than half of the source is held.
        quarantined = rows(
            env,
            "SELECT platform || '|' || account_id || '|' || reason FROM ops.ops_quarantine "
            "WHERE platform = 'tiktok_ads' ORDER BY 1",
            "metabase_reader",
        )
        assert [q for q in quarantined if DIFFERING in q] == [
            f"tiktok_ads|{DIFFERING}|totals_differ"
        ], quarantined
        held = {q.split("|")[2] for q in quarantined if INVALID in q}
        assert "invalid_id" in held, quarantined
        assert held <= {"invalid_id", "missing_key"}, quarantined
        assert {q.split("|")[1] for q in quarantined} == {DIFFERING, INVALID}, quarantined
        detail = rows(
            env,
            "SELECT detail FROM ops.ops_quarantine "  # noqa: S608 (IDs of the test)
            f"WHERE platform = 'tiktok_ads' AND account_id = '{INVALID}' "
            "AND reason = 'invalid_id'",
            "metabase_reader",
        )
        assert len(detail) == 1, detail
        assert "fits bigint" in detail[0], detail
        assert OVERSIZED["1800000000000102"] not in detail[0], detail
        statuses = rows(
            env,
            "SELECT account_id || '|' || status FROM core.loads "  # noqa: S608 (IDs of the test)
            f"WHERE source = 'tiktok_ads' AND account_id IN {IN_ADVERTISERS} ORDER BY 1",
        )
        assert statuses == [
            f"{HEALTHY}|accepted",
            f"{DIFFERING}|pinned",
            f"{INVALID}|pinned",
            f"{ALSO_HEALTHY}|accepted",
        ], statuses
        registered = rows(
            env,
            "SELECT account_id || '|' || client_id || '|' || is_active || '|' || is_stub "  # noqa: S608 (IDs of the test)
            "|| '|' || time_zone FROM core.dim_account "
            f"WHERE platform = 'tiktok_ads' AND account_id IN {IN_ADVERTISERS} ORDER BY 1",
        )
        assert registered == [
            f"{advertiser}|tiktok-stack-test|true|false|Etc/GMT+8"
            for advertiser in (HEALTHY, ALSO_HEALTHY)
        ], registered

        # The core holds every day of the healthy advertiser, the deleted campaign too, and so does
        # the view of the marts, with the complete payments as the primary conversions and the
        # optimization event as the conversions of the platform.
        spend = sum(Decimal(d[2]) for d in DAY.values())
        impressions = sum(int(d[0]) for d in DAY.values())
        clicks = sum(int(d[1]) for d in DAY.values())
        conversions = sum(int(d[3]) for d in DAY.values())
        payments = sum(int(d[4]) for d in DAY.values() if d[4] != "-")
        core = warehouse_output(
            env,
            "SELECT count(*) || '|' || sum(f.impressions) || '|' || sum(f.clicks) || '|' || "  # noqa: S608 (IDs of the test)
            "sum(f.spend) FROM core.ad_stats_campaign_daily AS f "
            "JOIN core.dim_account AS a ON a.account_sk = f.account_sk "
            f"WHERE a.platform = 'tiktok_ads' AND a.account_id = '{HEALTHY}'",
        ).split("|")
        assert [int(core[0]), int(core[1]), int(core[2]), Decimal(core[3])] == [
            len(DAY) * days,
            impressions * days,
            clicks * days,
            spend * days,
        ], core
        # The view shows the two healthy advertisers alone.
        assert rows(
            env,
            "SELECT DISTINCT account_id FROM marts.ad_stats_campaign_daily "
            "WHERE platform = 'tiktok_ads' ORDER BY 1",
            "metabase_reader",
        ) == [HEALTHY, ALSO_HEALTHY]
        shown = warehouse_output(
            env,
            "SELECT count(DISTINCT account_id) || '|' || count(*) || '|' || sum(impressions) "  # noqa: S608 (IDs of the test)
            "|| '|' || sum(clicks) || '|' || sum(spend) || '|' || sum(conversions) "
            "|| '|' || sum(conversions_primary) || '|' || min(currency) "
            "FROM marts.ad_stats_campaign_daily WHERE platform = 'tiktok_ads' "
            f"AND account_id = '{HEALTHY}'",
            "metabase_reader",
        ).split("|")
        assert shown[:3] == ["1", str(len(DAY) * days), str(impressions * days)], shown
        assert [int(shown[3]), Decimal(shown[4]), Decimal(shown[5]), Decimal(shown[6])] == [
            clicks * days,
            spend * days,
            Decimal(conversions * days),
            Decimal(payments * days),
        ], shown
        assert shown[7] == "USD", shown
        actions = rows(
            env,
            "SELECT conversion_action_id || '|' || conversion_category || '|' || is_primary "  # noqa: S608 (IDs of the test)
            "|| '|' || sum(conversions) || '|' || sum(conversion_value) "
            "FROM marts.ad_conversions_campaign_daily WHERE platform = 'tiktok_ads' "
            f"AND account_id = '{HEALTHY}' "
            "GROUP BY conversion_action_id, conversion_category, is_primary ORDER BY 1",
            "metabase_reader",
        )
        value = Decimal(DAY["1800000000000101"][5]) * days
        assert [a.split("|")[:3] for a in actions] == [
            ["complete_payment", "purchase", "true"],
            ["conversion", "other", "false"],
        ], actions
        assert Decimal(actions[0].split("|")[4]) == value, actions
        totals = warehouse_output(
            env,
            "SELECT count(*) || '|' || sum(impressions) || '|' || sum(clicks) || '|' || "  # noqa: S608 (IDs of the test)
            "sum(spend) FROM core.platform_totals_daily "
            f"WHERE source = 'tiktok_ads' AND account_id = '{HEALTHY}'",
        ).split("|")
        assert [int(totals[0]), int(totals[1]), int(totals[2]), Decimal(totals[3])] == [
            days,
            impressions * days,
            clicks * days,
            spend * days,
        ], totals

        # The common state of TikTok: the deleted campaign, and its ad group and ad, are removed
        # by their secondary_status, with it as their status; the enabled ones are active. The ad
        # that only the statistics name is a stub of its ad group, with the name of its
        # statistics, and the listed ads are listed.
        states = rows(
            env,
            "SELECT m.campaign_id_text || '|' || m.campaign_status || '|' || m.campaign_state "  # noqa: S608 (IDs of the test)
            "FROM core.dim_campaign AS m JOIN core.dim_account AS a ON a.account_sk = m.account_sk "
            f"WHERE a.platform = 'tiktok_ads' AND a.account_id = '{HEALTHY}' ORDER BY 1",
        )
        assert states == [
            "1800000000000101|ENABLE|active",
            "1800000000000102|CAMPAIGN_STATUS_DELETE|removed",
        ], states
        groups = rows(
            env,
            "SELECT g.ad_group_id_text || '|' || g.ad_group_status || '|' || g.ad_group_state "  # noqa: S608 (IDs of the test)
            "FROM core.dim_ad_group AS g JOIN core.dim_account AS a ON a.account_sk = g.account_sk "
            f"WHERE a.platform = 'tiktok_ads' AND a.account_id = '{HEALTHY}' ORDER BY 1",
        )
        assert groups == [
            "1800000000000201|ENABLE|active",
            "1800000000000202|ADGROUP_STATUS_CAMPAIGN_DELETE|removed",
        ], groups
        ads = rows(
            env,
            "SELECT d.ad_id_text || '|' || d.ad_group_id || '|' || coalesce(d.ad_name, '') || '|' "  # noqa: S608 (IDs of the test)
            "|| coalesce(d.ad_state, '') || '|' || d.is_listed || '|' || (d.last_seen_at IS NULL) "
            "FROM core.dim_ad AS d JOIN core.dim_account AS a ON a.account_sk = d.account_sk "
            f"WHERE a.platform = 'tiktok_ads' AND a.account_id = '{HEALTHY}' ORDER BY 1",
        )
        assert ads == [
            "1800000000000301|1800000000000201|Boots, red|active|true|false",
            "1800000000000302|1800000000000202|Socks, grey|removed|true|false",
            "1800000000000303|1800000000000201|Boots, blue|other|false|true",
        ], ads
        stats = warehouse_output(
            env,
            "SELECT count(*) || '|' || sum(f.spend) FROM core.ad_stats_ad_daily AS f "  # noqa: S608 (IDs of the test)
            "JOIN core.dim_account AS a ON a.account_sk = f.account_sk "
            f"WHERE a.platform = 'tiktok_ads' AND a.account_id = '{HEALTHY}' "
            f"AND f.ad_id = {STUB[0]}",
        ).split("|")
        assert [int(stats[0]), Decimal(stats[1])] == [days, Decimal(ADS[STUB[0]][2]) * days]
    finally:
        idle(airflow_url, token)
        warehouse_output(env, REMOVE)
        path.unlink(missing_ok=True)
    assert (
        rows(
            env,
            "SELECT account_id FROM core.dim_account WHERE platform = 'tiktok_ads' "  # noqa: S608 (IDs of the test)
            f"AND account_id IN {IN_ADVERTISERS}",
        )
        == []
    )
