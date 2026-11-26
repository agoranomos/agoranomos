"""The short cycles of Meta and of TikTok (ADR-0051, #412): a load of the kind intraday of each,
through the real dlt sources of airflow/dags/agoranomos_pipelines/meta_ads.py and tiktok_ads.py
with the HTTP of their APIs faked, after a whole load of the same account, reaches the core as a
partial day of the fact of campaigns, with no conversions, in no other fact, and puts no account in
quarantine, through the next run of the transform; the shapes of Meta and of TikTok read a load
without actions or conversions as the shape of Google Ads reads one of its short cycle, which
tests/stack/test_partial_day.py checks on demo_search.

No load reaches Meta or TikTok, and no answer comes from a real account: the answers below are faked
by hand from the references of the Marketing API v27.0 and of the TikTok API for Business v1.3, as
tests/stack/test_meta_ads.py and test_tiktok_ads.py fake them, and the stand-ins of the HTTP client
of dlt answer each request with them in the container of the tasks, with the fields or the metrics
that the request names, as the APIs do. The test names a Meta ad account and a TikTok advertiser of
its own, with IDs that no account has, in extensions/model/clients.toml, loads each whole and then
its current day as of the newest load of demo_social, so that the rates of the demo cover their
days, and runs the transform once. In its finally it deletes every row of the two from raw, from
the core, as the superuser with the thaw of ADR-0035, from the tables of staging that the run
wrote, and the file, so that the demo is as before. If it is killed before that, run the cleanup of
REMOVE by hand as the superuser of the warehouse, and delete extensions/model/clients.toml.

The assertions print IDs, days, counts, sums and states, never a secret: the tokens of the loads are
stand-ins of low entropy.
"""

import json
from datetime import timedelta
from zoneinfo import ZoneInfo

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
META, TIKTOK = "900000000000011", "7900000000000000011"
ACCOUNTS = {"meta_ads": META, "tiktok_ads": TIKTOK}
ZONES = {META: "America/New_York", TIKTOK: "Etc/GMT+8"}
CLIENTS = f"""\
[clients.short-cycle-stack-test]
name = "Short cycle of the stack test"

[[accounts]]
platform = "meta_ads"
account_id = "{META}"
client = "short-cycle-stack-test"

[[accounts]]
platform = "tiktok_ads"
account_id = "{TIKTOK}"
client = "short-cycle-stack-test"
"""
# Stand-ins of the tokens, of low entropy: no request leaves the container.
TOKENS = {
    "META_ADS_ACCESS_TOKEN": "stand-in-token-of-the-short-cycle-test",
    "TIKTOK_ADS_ACCESS_TOKEN": "stand-in-token-of-the-short-cycle-test",
}
# One campaign, ad set or ad group and ad of each account, the same numbers on every day: the
# impressions, link clicks and spend, and a purchase with its value.
CAMPAIGN, GROUP, AD = "120200000000111", "120200000000211", "120200000000311"
IMPRESSIONS, CLICKS, SPEND = "1000", "20", "12.34"

# The whole load and then the load of the current day of each account, as of a moment, through
# meta_ads.load, meta_ads.load_intraday, tiktok_ads.load and tiktok_ads.load_intraday, with the HTTP
# client of dlt replaced by stand-ins that answer each request with the answers given as JSON, for
# every day that it asks for, with the fields or the metrics that it names alone. dlt reads the
# password of loader from the environment of the container, and the script prints the rows of each
# table of each load.
LOAD = """
import json
import sys
from datetime import date, datetime, timedelta
from urllib.parse import urlsplit

from agoranomos_pipelines import meta_ads, tiktok_ads

now = datetime.fromisoformat(sys.argv[1])
faked = json.loads(sys.argv[2])


class Answer:
    def __init__(self, body):
        self.status_code, self.body, self.headers = 200, body, {}

    def json(self):
        return json.loads(json.dumps(self.body))

    @property
    def text(self):
        return json.dumps(self.body)


def between(first, last):
    first, last = date.fromisoformat(first), date.fromisoformat(last)
    return [(first + timedelta(days=n)).isoformat() for n in range((last - first).days + 1)]


class Graph:
    def __init__(self, **settings):
        pass

    def get(self, url, params, headers):
        path = urlsplit(url).path.removeprefix(f"/{meta_ads.API_VERSION}/")
        assert "access_token" not in params, "the token in a URL"
        account = faked["meta_ads"]
        if "/" not in path:
            return Answer(account["account"])
        edge = path.split("/")[1]
        if edge != "insights":
            return Answer({"data": account[edge], "paging": {}})
        asked = params["fields"].split(",")
        found = json.loads(params["time_range"])
        rows = []
        for day in between(found["since"], found["until"]):
            for row in account["insights"][params["level"]]:
                row = {**row, "date_start": day, "date_stop": day}
                rows.append({name: value for name, value in row.items() if name in asked})
        return Answer({"data": rows, "paging": {}})

    def post(self, url, data, headers):
        raise AssertionError("no report run")


LEVELS = {
    "AUCTION_ADVERTISER": "advertiser",
    "AUCTION_CAMPAIGN": "campaign",
    "AUCTION_ADGROUP": "adgroup",
    "AUCTION_AD": "ad",
}


def page(rows):
    info = {"page": 1, "total_page": 1}
    return Answer({"code": 0, "message": "OK", "data": {"list": rows, "page_info": info}})


class TikTok:
    def __init__(self, **settings):
        pass

    def get(self, url, params, headers):
        assert headers.get("Access-Token"), "a request without the token"
        path = urlsplit(url).path.removeprefix(f"/open_api/{tiktok_ads.API_VERSION}/")
        advertiser = faked["tiktok_ads"]
        if path == "advertiser/info/":
            return page([advertiser["advertiser"]])
        if path != "report/integrated/get/":
            return page(advertiser[path.split("/")[0]])
        asked = json.loads(params["metrics"])
        rows = []
        for day in between(params["start_date"], params["end_date"]):
            for row in advertiser["report"][LEVELS[params["data_level"]]]:
                dimensions = {**row["dimensions"], "stat_time_day": f"{day} 00:00:00"}
                metrics = {name: value for name, value in row["metrics"].items() if name in asked}
                rows.append({"dimensions": dimensions, "metrics": metrics})
        return page(rows)


meta_ads.Client = Graph
tiktok_ads.Client = TikTok
counts = {
    "meta_ads": meta_ads.load(now),
    "meta_ads_intraday": meta_ads.load_intraday(now),
    "tiktok_ads": tiktok_ads.load(now),
    "tiktok_ads_intraday": tiktok_ads.load_intraday(now),
}
print(json.dumps(counts, sort_keys=True))
"""


def meta_answers() -> dict:
    """The answers of the Graph API for the Meta account: the account, its lists, and the rows of
    the insights of each level for one day, the totals of the account the sum of its campaign."""
    named = {"account_id": META, "account_name": "Short cycle", "account_currency": "USD"}
    measures = {"impressions": IMPRESSIONS, "inline_link_clicks": CLICKS, "clicks": "25"}
    measures["spend"] = SPEND
    actions = {
        "attribution_setting": "7d_click_1d_view",
        "actions": [
            {"action_type": "offsite_conversion.fb_pixel_purchase", "value": "2", "7d_click": "2"}
        ],
        "action_values": [
            {"action_type": "offsite_conversion.fb_pixel_purchase", "value": "99.50"}
        ],
    }
    status = {"status": "ACTIVE", "configured_status": "ACTIVE", "effective_status": "ACTIVE"}
    campaign = {"campaign_id": CAMPAIGN, "campaign_name": "Boots"}
    group = {"campaign_id": CAMPAIGN, "adset_id": GROUP, "adset_name": "Boots, men"}
    ad = {**group, "ad_id": AD, "ad_name": "Boots, red"}
    del ad["adset_name"]
    return {
        "account": {
            "id": f"act_{META}",
            "account_id": META,
            "name": "Short cycle",
            "currency": "USD",
            "timezone_name": ZONES[META],
            "account_status": 1,
        },
        "campaigns": [
            {"id": CAMPAIGN, "account_id": META, "name": "Boots", **status}
            | {"objective": "OUTCOME_SALES"}
        ],
        "adsets": [
            {"id": GROUP, "account_id": META, "campaign_id": CAMPAIGN, "name": "Boots, men"}
            | status
            | {"optimization_goal": "OFFSITE_CONVERSIONS"}
        ],
        "ads": [
            {"id": AD, "account_id": META, "campaign_id": CAMPAIGN, "adset_id": GROUP}
            | {"name": "Boots, red"}
            | status
        ],
        "insights": {
            "account": [
                {
                    "account_id": META,
                    "impressions": IMPRESSIONS,
                    "inline_link_clicks": CLICKS,
                    "spend": SPEND,
                }
            ],
            "campaign": [named | campaign | measures | actions],
            "adset": [named | group | measures | actions],
            "ad": [named | ad | measures | actions],
        },
    }


def tiktok_answers() -> dict:
    """The answers of the TikTok API for the advertiser: the advertiser, its lists, and the rows of
    the report of each level for one day, the totals of the advertiser the sum of its campaign."""
    status = {"operation_status": "ENABLE", "secondary_status": "CAMPAIGN_STATUS_ENABLE"}
    totals = {"currency": "USD", "impressions": IMPRESSIONS, "clicks": CLICKS, "spend": SPEND}
    converted = {"conversion": "2", "complete_payment": "2", "total_complete_payment_rate": "99.50"}
    return {
        "advertiser": {
            "advertiser_id": TIKTOK,
            "name": "Short cycle",
            "currency": "USD",
            "timezone": ZONES[TIKTOK],
            "display_timezone": "America/Los_Angeles",
            "status": "STATUS_ENABLE",
        },
        "campaign": [
            {
                "advertiser_id": TIKTOK,
                "campaign_id": CAMPAIGN,
                "campaign_name": "Boots",
                "objective_type": "WEB_CONVERSIONS",
                **status,
            }
        ],
        "adgroup": [
            {
                "advertiser_id": TIKTOK,
                "campaign_id": CAMPAIGN,
                "adgroup_id": GROUP,
                "adgroup_name": "Boots, men",
                "optimization_goal": "CONVERT",
                "operation_status": "ENABLE",
                "secondary_status": "ADGROUP_STATUS_DELIVERY_OK",
            }
        ],
        "ad": [
            {
                "advertiser_id": TIKTOK,
                "campaign_id": CAMPAIGN,
                "adgroup_id": GROUP,
                "ad_id": AD,
                "ad_name": "Boots, red",
                "ad_format": "SINGLE_VIDEO",
                "operation_status": "ENABLE",
                "secondary_status": "AD_STATUS_DELIVERY_OK",
            }
        ],
        "report": {
            "advertiser": [{"dimensions": {"advertiser_id": TIKTOK}, "metrics": totals}],
            "campaign": [
                {
                    "dimensions": {"campaign_id": CAMPAIGN},
                    "metrics": {"campaign_name": "Boots", **totals, **converted},
                }
            ],
            "adgroup": [
                {
                    "dimensions": {"adgroup_id": GROUP},
                    "metrics": {"campaign_id": CAMPAIGN, "adgroup_name": "Boots, men"}
                    | totals
                    | converted,
                }
            ],
            "ad": [
                {
                    "dimensions": {"ad_id": AD},
                    "metrics": {"campaign_id": CAMPAIGN, "adgroup_id": GROUP, "ad_name": "Boots"}
                    | totals
                    | converted,
                }
            ],
        },
    }


# The rows of the two accounts everywhere that the loads and the run wrote them, in one transaction
# with the thaw of the facts (ADR-0035), as the superuser, as tests/stack/test_meta_ads.py and
# test_tiktok_ads.py delete theirs: the child tables of raw by their row, the tables of raw by
# account, the loads of dlt that only these accounts had, the facts and the journals of
# restatements, the dimensions and their versions, the totals, the quarantine, the loads of the
# core and the registry of accounts, and the rows of the tables of staging that the run filled. A
# table that does not exist is passed over.
REMOVE = """
BEGIN;
SELECT set_config('agoranomos.thaw', pg_current_xact_id()::text, true) <> '';
DO $$
DECLARE
    t record;
    s record;
    accounts text[];
    keys integer[];
BEGIN
    FOR s IN SELECT * FROM (VALUES {sources}) AS v (source, dataset, account) LOOP
        accounts := ARRAY[s.account];
        FOR t IN
            SELECT c.table_name, split_part(c.table_name, '__', 1) AS parent, p.column_name AS key
            FROM information_schema.columns AS c
            JOIN information_schema.columns AS p
                ON p.table_schema = c.table_schema
                AND p.table_name = split_part(c.table_name, '__', 1)
                AND p.column_name IN ('account_id', 'advertiser_id')
            WHERE c.table_schema = s.dataset AND c.column_name = '_dlt_parent_id'
        LOOP
            EXECUTE format(
                'DELETE FROM %I.%I AS c USING %I.%I AS r '
                'WHERE c._dlt_parent_id = r._dlt_id AND r.%I = ANY($1)',
                s.dataset, t.table_name, s.dataset, t.parent, t.key
            ) USING accounts;
        END LOOP;
        IF to_regclass(format('%I._agoranomos_loads', s.dataset)) IS NOT NULL
            AND to_regclass(format('%I._dlt_loads', s.dataset)) IS NOT NULL THEN
            EXECUTE format(
                'DELETE FROM %I._dlt_loads AS l WHERE l.load_id IN ('
                'SELECT _dlt_load_id FROM %I._agoranomos_loads WHERE account_id = ANY($1)) '
                'AND NOT EXISTS (SELECT FROM %I._agoranomos_loads AS j '
                'WHERE j._dlt_load_id = l.load_id AND NOT j.account_id = ANY($1))',
                s.dataset, s.dataset, s.dataset
            ) USING accounts;
        END IF;
        FOR t IN
            SELECT c.table_name, c.column_name
            FROM information_schema.columns AS c
            WHERE c.table_schema = s.dataset AND c.column_name IN ('account_id', 'advertiser_id')
        LOOP
            EXECUTE format(
                'DELETE FROM %I.%I WHERE %I = ANY($1)', s.dataset, t.table_name, t.column_name
            ) USING accounts;
        END LOOP;
        keys := ARRAY(
            SELECT account_sk FROM core.dim_account
            WHERE platform = s.source AND account_id = ANY(accounts)
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
            WHERE source = s.source AND account_id = ANY(accounts);
        DELETE FROM core.account_quarantine
            WHERE source = s.source AND account_id = ANY(accounts);
        DELETE FROM core.loads WHERE source = s.source AND account_id = ANY(accounts);
        DELETE FROM core.dim_account WHERE account_sk = ANY(keys);
        IF to_regclass('staging.int_g1_failures') IS NOT NULL THEN
            DELETE FROM staging.int_g1_failures
                WHERE source = s.source AND account_id = ANY(accounts);
        END IF;
        IF to_regclass('staging.stg_loads__newest_days') IS NOT NULL THEN
            DELETE FROM staging.stg_loads__newest_days
                WHERE source = s.source AND account_id = ANY(accounts);
        END IF;
    END LOOP;
END $$;
COMMIT;
""".replace(
    "{sources}",
    ", ".join(f"('{source}', 'raw_{source}', '{account}')" for source, account in ACCOUNTS.items()),
)


def rows(env, sql, role="postgres"):
    return warehouse_output(env, sql, role).splitlines()


def idle(airflow_url, token):
    wait_for(
        lambda: not is_busy(airflow_url, token, TRANSFORM, assets=ASSETS),
        1800,
        interval=5,
        message="the runs of the transform did not end",
    )


def of_the_test(platform: str = "a.platform", account_id: str = "a.account_id") -> str:
    """The condition on the accounts of the test, by the columns of their platform and ID."""
    return " OR ".join(
        f"({platform} = '{source}' AND {account_id} = '{account}')"
        for source, account in ACCOUNTS.items()
    )


def test_the_short_cycles_of_meta_and_tiktok_reach_the_core_as_partial_days(
    env, airflow_url, token
):
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
        result = run_in_tasks(
            env,
            LOAD,
            now.isoformat(),
            json.dumps({"meta_ads": meta_answers(), "tiktok_ads": tiktok_answers()}),
            environment=TOKENS,
        )
        counts = json.loads(result.stdout.strip().splitlines()[-1])
        # The load of the current day took the campaign of its account on one day, its current
        # day, as the whole load covers the day before, and nothing else.
        assert counts["meta_ads_intraday"] == {"insights_campaign": 1}, counts
        assert counts["tiktok_ads_intraday"] == {"report_campaign_daily": 1}, counts
        # The current day of each account at the moment of the loads, in its time zone.
        today = {account: now.astimezone(ZoneInfo(zone)).date() for account, zone in ZONES.items()}
        for source, account in ACCOUNTS.items():
            journal = rows(
                env,
                "SELECT kind || '|' || window_from || '|' || window_to || '|' || status "  # noqa: S608 (IDs of the test)
                f"FROM raw_{source}._agoranomos_loads WHERE account_id = '{account}' ORDER BY 1",
            )
            whole = today[account] - timedelta(days=1)
            assert journal == [
                f"intraday|{today[account]}|{today[account]}|loaded",
                f"window|{whole - timedelta(days=27)}|{whole}|loaded",
            ], journal
        unpause(airflow_url, token, TRANSFORM)
        run = trigger(airflow_url, token, TRANSFORM)
        run = wait_for_run(airflow_url, token, TRANSFORM, run["dag_run_id"], timeout=1800)
        assert run["state"] == "success", run["state"]

        # The run accepted both loads of each account, and held neither in quarantine: G0a does
        # not check a load without totals, and G1a skips an intraday load.
        accepted = rows(
            env,
            "SELECT source || '|' || kind || '|' || status FROM core.loads AS a "  # noqa: S608 (IDs of the test)
            f"WHERE {of_the_test('a.source')} ORDER BY 1",
        )
        assert accepted == [
            "meta_ads|intraday|accepted",
            "meta_ads|window|accepted",
            "tiktok_ads|intraday|accepted",
            "tiktok_ads|window|accepted",
        ], accepted
        quarantined = rows(
            env,
            "SELECT platform || '|' || account_id || '|' || reason FROM ops.ops_quarantine AS a "  # noqa: S608 (IDs of the test)
            f"WHERE {of_the_test()}",
            "metabase_reader",
        )
        assert quarantined == [], quarantined

        # The current day of each account is partial in the fact of campaigns, with its
        # impressions, link clicks and spend and no conversions, none and not zero; the whole days
        # are whole, with the conversions of the platform where it has any (TikTok).
        for source, account in ACCOUNTS.items():
            day = today[account]
            facts = rows(
                env,
                "SELECT f.report_date || '|' || f.is_partial_day || '|' || f.impressions || '|' "  # noqa: S608 (IDs of the test)
                "|| f.clicks || '|' || f.spend || '|' || coalesce(f.conversions::text, 'none') "
                "FROM core.ad_stats_campaign_daily AS f "
                "JOIN core.dim_account AS a ON a.account_sk = f.account_sk "
                f"WHERE a.platform = '{source}' AND a.account_id = '{account}' "
                f"AND f.report_date >= '{day - timedelta(days=1)}' ORDER BY 1",
            )
            whole = "none" if source == "meta_ads" else "2.000000"
            assert facts == [
                f"{day - timedelta(days=1)}|false|1000|20|12.340000|{whole}",
                f"{day}|true|1000|20|12.340000|none",
            ], (source, facts)
        partial = warehouse_output(
            env,
            "SELECT count(*) FROM core.ad_stats_campaign_daily AS f "  # noqa: S608 (IDs of the test)
            "JOIN core.dim_account AS a ON a.account_sk = f.account_sk "
            f"WHERE ({of_the_test()}) AND f.is_partial_day",
        )
        assert partial == "2", partial

        # No other fact holds the current day: it is a day of the fact of campaigns alone.
        for fact in (
            "ad_stats_ad_group_daily",
            "ad_stats_ad_daily",
            "ad_conversions_campaign_daily",
        ):
            found = rows(
                env,
                "SELECT a.platform || '|' || max(f.report_date) "  # noqa: S608 (IDs of the test)
                f"FROM core.{fact} AS f JOIN core.dim_account AS a ON a.account_sk = f.account_sk "
                f"WHERE {of_the_test()} GROUP BY a.platform ORDER BY 1",
            )
            assert found == [
                f"{source}|{today[account] - timedelta(days=1)}"
                for source, account in ACCOUNTS.items()
            ], (fact, found)

        # The view of the marts carries the partial day, with no primary conversions.
        shown = rows(
            env,
            "SELECT platform || '|' || report_date || '|' || is_partial_day || '|' "  # noqa: S608 (IDs of the test)
            "|| coalesce(conversions_primary::text, 'none') FROM marts.ad_stats_campaign_daily "
            f"WHERE is_partial_day AND ({of_the_test('platform', 'account_id')}) ORDER BY 1",
            "metabase_reader",
        )
        assert shown == [
            f"{source}|{today[account]}|true|none" for source, account in ACCOUNTS.items()
        ], shown
    finally:
        idle(airflow_url, token)
        warehouse_output(env, REMOVE)
        path.unlink(missing_ok=True)
    left = rows(env, f"SELECT account_id FROM core.dim_account AS a WHERE {of_the_test()}")  # noqa: S608 (IDs of the test)
    assert left == []
