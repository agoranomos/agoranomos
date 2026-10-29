"""The transform, agoranomos_demo_transform: the five assets of the history start it, it loads the
settings of the model, and dbt builds the model; a failed critical test, or a mistake in a settings
file, keeps the marts as they were.

The assertions print counts, sums, states and names, never a secret.
"""

import json
from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from itertools import groupby

import pytest
from agoranomos_pipelines.demo import common, fx, platforms, world
from airflow_api import (
    asset_events,
    downstream_task_ids,
    is_busy,
    run_dag,
    task_instances,
    task_log,
    trigger,
    unpause,
    wait_for_dag,
    wait_for_run,
)
from services import DEMO
from stack_helpers import (
    REPO_ROOT,
    compose,
    containers,
    docker,
    leaked,
    secrets,
    totals,
    wait_for,
    warehouse,
    warehouse_output,
)

pytestmark = [
    pytest.mark.skipif(
        not DEMO, reason="the demo is off: COMPOSE_PROFILES in .env does not list demo"
    ),
    pytest.mark.usefixtures("daily_load_paused"),
]

HISTORY = "agoranomos_demo_history"
TRANSFORM = "agoranomos_demo_transform"
SETTINGS = "load_settings"
# The task that builds the tables of elementary, after the settings and before the tasks of dbt
# (ADR-0029).
ELEMENTARY = "elementary"
MART = "marts.ad_stats_campaign_daily"
MARTS = (
    "ad_stats_campaign_daily",
    "ad_conversions_campaign_daily",
    "web_sessions_daily",
    "ad_platforms",
    "ad_accounts",
    "clients",
    "ad_campaigns",
    "ad_campaign_names",
    "fx_rates_daily",
    "ad_stats_ad_group_daily",
    "ad_stats_ad_daily",
    "ad_groups",
    "ads",
)
# The task of the mart: each model and each seed is one task, dbt build (ADR-0028).
MART_BUILD = "dbt.ad_stats_campaign_daily_model_build"
MART_ASSET = "postgres://warehouse-db:5432/warehouse/marts/ad_stats_campaign_daily"
ASSETS = len(platforms.PLATFORMS)
# The pool of the tasks of dbt, which airflow-init sets (ADR-0027); the settings load outside it.
DBT_POOL = "dbt"
Q6, Q8 = Decimal("0.000001"), Decimal("0.00000001")
# The sums of the statistics of each ad platform in raw, in SQL that does not use the macros of the
# model: the mart holds the same.
RAW_SUMS = {
    "demo_search": (
        "SELECT sum(metrics__impressions::bigint), sum(metrics__clicks::bigint), "
        "sum(metrics__cost_micros::numeric) / 1000000 FROM raw_demo_search.campaign_stats"
    ),
    "demo_social": (
        "SELECT sum(impressions::bigint), sum(inline_link_clicks::bigint), sum(spend::numeric) "
        "FROM raw_demo_social.insights_campaign"
    ),
    "demo_video": (
        "SELECT sum(nullif(metrics__impressions::text, '-')::bigint), "
        "sum(nullif(metrics__clicks::text, '-')::bigint), "
        "sum(nullif(metrics__spend::text, '-')::numeric) FROM raw_demo_video.report_campaign_daily"
    ),
}
# The conversions of the primary actions of demo_social in raw: those that the seed marks primary.
RAW_SOCIAL_CONVERSIONS = (
    "SELECT sum(value::numeric) FROM raw_demo_social.insights_campaign__actions WHERE action_type "
    "IN ('offsite_conversion.fb_pixel_purchase', 'offsite_conversion.fb_pixel_lead')"
)
# The versions of the names of the campaigns, as staging has them from the versions that dlt keeps
# (scd2), and the periods of the names of the mart: platform, account, campaign, name, from, to.
CAMPAIGN_VERSIONS = " UNION ALL ".join(
    "SELECT platform, account_id, campaign_id, observed_name, valid_from, valid_to "  # noqa: S608 (constants of this file)
    f"FROM staging.stg_{code}__campaigns"
    for code in ("demo_search", "demo_social", "demo_video")
)
NAME_PERIODS = (
    "SELECT platform, account_id, campaign_id, observed_name, valid_from, valid_to "
    "FROM marts.ad_campaign_names"
)
# The row that breaks a critical test of staging: negative clicks, on a day before the history.
BROKEN_ROW = (
    "INSERT INTO raw_demo_search.campaign_stats (customer__id, campaign__id, campaign__name, "
    "campaign__resource_name, metrics__impressions, metrics__clicks, metrics__cost_micros, "
    "metrics__conversions, metrics__conversions_value, segments__date, _dlt_load_id, _dlt_id) "
    "VALUES ('4170000011', '21099999998', 'Broken by a test', "
    "'customers/4170000011/campaigns/21099999998', '10', '-5', '1000000', 0, 0, '2020-01-01', "
    "'stack-test', 'stack-test-broken-row')"
)
REMOVE_BROKEN_ROW = (
    "DELETE FROM raw_demo_search.campaign_stats WHERE _dlt_id = 'stack-test-broken-row'"
)
BROKEN_TEST = "non_negative_stg_demo_search__campaign_stats_clicks"
# The task that builds the staging view of that row, and runs the test that it breaks.
STAGING_BUILD = "dbt.stg_demo_search__campaign_stats_model_build"
# The lines in which Cosmos says how it parsed the dbt project (cosmos/dbt/graph.py, Cosmos 1.15.1),
# from the logs that the DAG processor writes for the file of the transform DAG, as JSON lines, into
# dag_processor/<day>/<bundle>/ of the volume airflow-logs (Airflow 3.3.2); latest links to the
# newest day. The DAG processor has no secret, and only these events are printed.
PARSES = """
import json
import pathlib

events = []
for day in pathlib.Path("/opt/airflow/logs/dag_processor").iterdir():
    path = day / "agoranomos" / "agoranomos_demo_transform.py.log"
    if day.is_symlink() or not path.is_file():
        continue
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            entry = json.loads(line)
        except ValueError:
            continue
        if str(entry.get("event", "")).startswith("Trying to parse the dbt project"):
            events.append([entry["timestamp"], entry["event"]])
print(json.dumps(sorted(events)))
"""


def moment(run):
    return datetime.fromisoformat(run["run_after"].replace("Z", "+00:00"))


def settings_file(env, name):
    """A settings file of the model in the extensions folder that .env names."""
    folder = env.get("AGORANOMOS_EXTENSIONS_DIR") or "./extensions"
    return (REPO_ROOT / folder).resolve() / "model" / name


def transform(airflow_url, token):
    run = trigger(airflow_url, token, TRANSFORM)
    return wait_for_run(airflow_url, token, TRANSFORM, run["dag_run_id"], timeout=900)


def logged(entries, text):
    """Whether an entry of a log holds the text anywhere: in its event, or in the detail of an
    error that ended the task."""
    return any(text in entry for entry in entries)


def assert_no_secret_in_the_logs(env, airflow_url, token, run):
    """Every try of every task of the run, each entry whole: names are printed, never values. The
    log is read where it is used, so that no frame of a failed test holds it."""
    values = secrets(env)
    assert values, "no secrets in .env: run ./agoranomos init"
    for instance in task_instances(airflow_url, token, run):
        found = leaked(values, task_log(airflow_url, token, instance))
        assert not found, f"the log of {instance['task_id']} holds the values of {found}"


@pytest.fixture(scope="module")
def pipeline(env, airflow_url, token):
    """A run of the history, and the run of the transform that its five asset events started."""
    for dag_id in (HISTORY, TRANSFORM):
        wait_for_dag(airflow_url, token, dag_id)
    warehouse_output(env, REMOVE_BROKEN_ROW)  # left behind by a test that was stopped
    for name in ("clients.toml", "conversion_actions.toml"):
        found = settings_file(env, name).exists()
        assert not found, f"extensions/model/{name} is in the way of these tests"
    # The history starts the transform only while the transform is unpaused. While a run of the
    # transform is queued or running, Airflow 3.3.2 defers the next one, and that run takes only
    # the events up to the first one that waited: so the history of this fixture waits until the
    # runs that earlier histories started have ended.
    unpause(airflow_url, token, TRANSFORM)
    wait_for(
        lambda: not is_busy(airflow_url, token, TRANSFORM, assets=ASSETS),
        900,
        interval=5,
        message="the runs of the transform that earlier histories started did not end",
    )
    history = run_dag(airflow_url, token, HISTORY, timeout=900)
    assert history["state"] == "success", history["state"]
    last = platforms.asset_uri(platforms.PLATFORMS[platforms.LOAD_ORDER[-1]])

    def started_by_the_history():
        # The event of the last load of this history lists the run that the five events started.
        [event] = asset_events(
            airflow_url, token, last, source_dag_id=HISTORY, source_run_id=history["dag_run_id"]
        )
        return next((r for r in event["created_dagruns"] if r["dag_id"] == TRANSFORM), None)

    started = wait_for(
        started_by_the_history,
        180,
        interval=5,
        message="the five assets of the history started no run of the transform",
    )
    return history, wait_for_run(airflow_url, token, TRANSFORM, started["run_id"], timeout=900)


@pytest.fixture(scope="module")
def transform_run(pipeline):
    return pipeline[1]


def euro_rate(day):
    """The rate of US dollars for one euro that the demo source of rates gives a day: that of the
    day, or of the last day of publication before it, as the generator makes them."""
    while not fx.published(day):
        day -= timedelta(days=1)
    return Decimal(common.major(fx.rate_ten_thousandths(day), 4))


def expected_rate(day, currency, reporting):
    """The rate of a currency into the reporting currency on a day, by the rule of the model: the
    last rate published on or before the day, through the euro, to eight places. The demo has
    accounts in US dollars and in euros, and the reporting currency is one of the two."""
    if currency == reporting:
        return Decimal(1)
    if reporting == "USD":
        return euro_rate(day).quantize(Q8, ROUND_HALF_UP)
    return (Decimal(1) / euro_rate(day)).quantize(Q8, ROUND_HALF_UP)


def name_periods(versions):
    """The periods of the names of each campaign from its versions, as the model must make them:
    the versions in the order of their start, those of one name in a row as one period, which is
    open while one of its versions is."""
    periods = []
    ordered = sorted(versions, key=lambda version: (version[:3], version[4]))
    for key, campaign in groupby(ordered, key=lambda version: version[:3]):
        for name, run in groupby(campaign, key=lambda version: version[3]):
            run = list(run)
            ends = [version[5] for version in run]
            periods.append((*key, name, run[0][4], "" if "" in ends else max(ends)))
    return sorted(periods)


def rows_of(env, sql):
    """The rows of a query as tuples of texts, a NULL as an empty text."""
    return [tuple(line.split("|")) for line in warehouse_output(env, sql).splitlines()]


def test_the_five_assets_of_the_history_start_the_transform(transform_run):
    assert transform_run["run_type"] == "asset_triggered"
    assert transform_run["state"] == "success", transform_run["state"]


def test_the_settings_load_first_and_each_node_is_built_with_its_tests_in_one_task(
    airflow_url, token, transform_run
):
    downstream = downstream_task_ids(airflow_url, token, TRANSFORM)
    assert downstream[SETTINGS] == [ELEMENTARY]
    reached, todo = set(), [ELEMENTARY]
    while todo:
        for following in downstream.get(todo.pop(), []):
            if following not in reached:
                reached.add(following)
                todo.append(following)
    built = [task for task in downstream if task not in (SETTINGS, ELEMENTARY)]
    assert reached == set(built), "a task of dbt does not wait for the tables of elementary"
    assert MART_BUILD in built, sorted(built)[:5]
    # One task of dbt build for each model and each seed: no task runs a node or its tests alone.
    alone = [task for task in built if not task.endswith(("_model_build", "_seed_build"))]
    assert not alone, sorted(alone)[:5]
    states = {i["task_id"]: i["state"] for i in task_instances(airflow_url, token, transform_run)}
    assert set(states.values()) == {"success"}, sorted(
        t for t, s in states.items() if s != "success"
    )


def test_the_tasks_of_dbt_run_in_their_pool(airflow_url, token, transform_run):
    # At most two at a time, whatever AIRFLOW_PARALLELISM allows (ADR-0027); the task that loads
    # the settings runs before them, outside it.
    pools = {i["task_id"]: i["pool"] for i in task_instances(airflow_url, token, transform_run)}
    assert pools.pop(SETTINGS) == "default_pool"
    # The task of elementary runs dbt too, so it takes a slot of the pool.
    assert pools.pop(ELEMENTARY) == DBT_POOL
    assert pools, "the run has no task of dbt"
    assert {task.split(".")[0] for task in pools} == {"dbt"}, sorted(pools)[:5]
    assert set(pools.values()) == {DBT_POOL}, pools


def started_at(service):
    """When the container of a service last started, in UTC."""
    started = docker("inspect", "--format", "{{.State.StartedAt}}", containers()[service]["ID"])
    # Docker gives nanoseconds and a Z: fromisoformat reads them, and drops what is finer.
    return datetime.fromisoformat(started.stdout.strip()).astimezone(UTC)


def test_cosmos_renders_the_transform_from_the_manifest(transform_run):
    # Every parse of the transform DAG since the DAG processor started read the manifest, and none
    # ran dbt ls: an upgraded stack keeps the lines of its parses with dbt ls in the same logs.
    result = compose("exec", "-T", "airflow-dag-processor", "python", "-c", PARSES, check=False)
    assert result.returncode == 0, f"the script exited with {result.returncode}"
    since = started_at("airflow-dag-processor")
    events = [
        event
        for timestamp, event in json.loads(result.stdout)
        if datetime.fromisoformat(timestamp).astimezone(UTC) >= since
    ]
    assert events, "Cosmos logged no parse of the transform DAG since the DAG processor started"
    assert all("using a dbt manifest" in event for event in events), events


@pytest.mark.parametrize("code", sorted(RAW_SUMS))
def test_the_mart_holds_the_statistics_of_raw(env, transform_run, code):
    raw = [Decimal(value or 0) for value in warehouse_output(env, RAW_SUMS[code]).split("|")]
    mart = warehouse_output(
        env,
        f"SELECT sum(impressions), sum(clicks), sum(spend) FROM {MART} WHERE platform = '{code}'",  # noqa: S608 (constants of this file)
    )
    assert [Decimal(value or 0) for value in mart.split("|")] == raw
    assert raw[0] > 0, f"no statistics of {code} in raw"


def test_the_conversions_of_a_campaign_are_those_of_its_primary_actions(env, transform_run):
    raw = Decimal(warehouse_output(env, RAW_SOCIAL_CONVERSIONS) or 0)
    social = f"SELECT sum(conversions) FROM {MART} WHERE platform = 'demo_social'"  # noqa: S608 (constants of this file)
    assert Decimal(warehouse_output(env, social) or 0) == raw
    assert raw > 0, "no conversion of a primary action of demo_social in raw"


def test_the_rates_of_the_mart_are_those_of_the_generator(env, transform_run):
    reporting = env.get("AGORANOMOS_REPORTING_CURRENCY") or "USD"
    rows = rows_of(
        env,
        "SELECT rate_date, currency, reporting_currency, rate_into_reporting_currency "
        "FROM marts.fx_rates_daily",
    )
    wrong = [
        row
        for row in rows
        if row[2] != reporting
        or Decimal(row[3]) != expected_rate(date.fromisoformat(row[0]), row[1], reporting)
    ]
    assert rows, "marts.fx_rates_daily is empty"
    assert not wrong, wrong[:3]
    assert {row[1] for row in rows} == {"USD", "EUR"}


def test_spend_in_the_reporting_currency_is_spend_at_the_rate_of_its_day(env, transform_run):
    reporting = env.get("AGORANOMOS_REPORTING_CURRENCY") or "USD"
    rows = rows_of(env, f"SELECT report_date, currency, spend, spend_reporting FROM {MART}")  # noqa: S608 (constants of this file)
    wrong = [
        row
        for row in rows
        if Decimal(row[3])
        != (
            Decimal(row[2]) * expected_rate(date.fromisoformat(row[0]), row[1], reporting)
        ).quantize(Q6, ROUND_HALF_UP)
    ]
    assert rows, f"{MART} is empty"
    assert not wrong, wrong[:3]
    assert {row[1] for row in rows} == {"USD", "EUR"}


def test_every_day_of_a_campaign_has_its_current_name(env, transform_run):
    split = warehouse_output(
        env,
        f"SELECT count(*) FROM (SELECT 1 FROM {MART} GROUP BY platform, account_id, campaign_id "  # noqa: S608 (constants of this file)
        "HAVING count(DISTINCT campaign_name) > 1) AS split",
    )
    assert int(split) == 0
    # A campaign that the history renamed every half-year: the name of its open version.
    current = warehouse_output(
        env,
        "SELECT campaign__name FROM raw_demo_search.campaigns "
        "WHERE campaign__id = '21000000102' AND _dlt_valid_to IS NULL",
    )
    shown = warehouse_output(
        env,
        "SELECT campaign_name || '|' || name_count FROM marts.ad_campaigns "
        "WHERE campaign_id = '21000000102'",
    )
    assert shown == f"{current}|2"
    named = warehouse_output(
        env,
        f"SELECT DISTINCT campaign_name FROM {MART} WHERE campaign_id = '21000000102'",  # noqa: S608 (constants of this file)
    )
    assert named == current


def test_the_names_of_a_campaign_are_the_periods_of_its_versions(env, transform_run):
    # The versions of one name in a row, as a change of status makes them, are one period, open
    # while its last version is: a period of each version, or a closed period of an open name,
    # would show a name twice or no current name.
    expected = name_periods(rows_of(env, CAMPAIGN_VERSIONS))
    assert sorted(rows_of(env, NAME_PERIODS)) == expected
    versions = int(warehouse_output(env, f"SELECT count(*) FROM ({CAMPAIGN_VERSIONS}) AS v"))  # noqa: S608 (constants of this file)
    assert len(expected) < versions, "no campaign has two versions of one name in a row"
    assert any(period[5] == "" for period in expected), "no campaign has a current name"


def test_the_accounts_of_the_demo_belong_to_its_clients(env, transform_run):
    found = warehouse_output(
        env, "SELECT platform || ' ' || account_id || ' ' || client_id FROM marts.ad_accounts"
    )
    assert set(found.splitlines()) == {
        f"{a.platform} {a.account_id} {a.client}"
        for a in world.ACCOUNTS
        if a.platform != "demo_web"
    }
    clients = set(warehouse_output(env, "SELECT client_id FROM marts.clients").split())
    assert clients == {"tarnwick", "brindlemoor", "unassigned"}


def test_the_day_that_the_demo_misses_is_a_warning_and_has_no_row(
    env, airflow_url, token, pipeline
):
    history, run = pipeline
    [(account, day)] = platforms.gap(moment(history))
    rows = warehouse_output(
        env,
        f"SELECT count(*) FROM {MART} WHERE platform = 'demo_social' AND account_id = '{account}' "  # noqa: S608 (constants of this file)
        f"AND report_date = DATE '{day.isoformat()}'",
    )
    assert int(rows) == 0
    [tests] = [
        i
        for i in task_instances(airflow_url, token, run)
        if i["task_id"] == "dbt.int_ad_stats__unioned_model_build"
    ]
    assert tests["state"] == "success", "a warning failed the task"
    assert logged(task_log(airflow_url, token, tests), "int_ad_stats__unioned_has_no_missing_day")


def test_cosmos_emits_the_asset_of_the_mart(airflow_url, token, transform_run):
    # The event of this run: Airflow keeps the events of every run before it.
    run_id = transform_run["dag_run_id"]
    events = asset_events(
        airflow_url, token, MART_ASSET, source_dag_id=TRANSFORM, source_run_id=run_id
    )
    assert events, f"the run {run_id} did not update the asset of the mart"
    # The API ignores a filter that it does not know, so the events must be those of this run.
    sources = {(e["source_dag_id"], e["source_run_id"], e["source_task_id"]) for e in events}
    assert sources == {(TRANSFORM, run_id, MART_BUILD)}


@pytest.mark.parametrize("mart", MARTS)
def test_metabase_reader_reads_every_mart(env, transform_run, mart):
    # dbt creates the tables as transformer, and the default privileges of transformer give
    # metabase_reader the new tables of marts.
    rows = warehouse_output(env, f"SELECT count(*) FROM marts.{mart}", "metabase_reader")  # noqa: S608 (constants of this file)
    assert int(rows) > 0, mart


def test_metabase_reader_reads_no_layer_below_the_marts(env, transform_run):
    result = warehouse(
        env, "SELECT 1 FROM staging.int_ad_stats__unioned LIMIT 1", "metabase_reader"
    )
    assert result.returncode != 0
    assert "permission denied" in result.stderr


def test_the_logs_hold_no_secret(env, airflow_url, token, transform_run):
    # The line that dbt prints first shows that the scan reads the logs of dbt.
    [mart] = [
        i for i in task_instances(airflow_url, token, transform_run) if i["task_id"] == MART_BUILD
    ]
    assert logged(task_log(airflow_url, token, mart), "Running with dbt=")
    assert_no_secret_in_the_logs(env, airflow_url, token, transform_run)


@pytest.fixture(scope="module")
def put_back(env, airflow_url, token, transform_run):
    """The tests below break the data or the settings, and each takes its change out again in the
    end. One run of the transform after the last of them puts the marts back as the demo makes
    them, and checks it: a run takes minutes on a runner of CI, so the tests share it."""
    before = totals(env, MART)
    yield
    run = transform(airflow_url, token)
    assert run["state"] == "success", run["state"]
    assert totals(env, MART) == before
    clients = warehouse_output(env, "SELECT client_id || ' ' || client_name FROM marts.clients")
    assert "acme" not in clients.split()
    assert "tarnwick Tarnwick Outfitters" in clients.splitlines()


def test_a_failed_critical_test_keeps_the_marts(env, airflow_url, token, transform_run, put_back):
    # Negative clicks break non_negative of stg_demo_search__campaign_stats: the models that read
    # it do not run, and the marts keep the rows of the last good run.
    before = totals(env, MART)
    warehouse_output(env, REMOVE_BROKEN_ROW)  # left behind by a test that was stopped
    try:
        warehouse_output(env, BROKEN_ROW)
        run = transform(airflow_url, token)
        assert run["state"] == "failed", run["state"]
        instances = task_instances(airflow_url, token, run)
        states = {i["task_id"]: i["state"] for i in instances}
        assert states[SETTINGS] == "success"
        assert states[STAGING_BUILD] == "failed"
        assert states[MART_BUILD] == "upstream_failed"
        assert totals(env, MART) == before
        # The log of the failed task names the test that failed, with its one row, as dbt 1.12
        # prints it. No log of the run holds a secret: a failure prints more than a success does.
        [failed] = [i for i in instances if i["task_id"] == STAGING_BUILD]
        named = logged(task_log(airflow_url, token, failed), f"FAIL 1 {BROKEN_TEST}")
        assert named, f"the log of the failed task does not name the test {BROKEN_TEST}"
        assert_no_secret_in_the_logs(env, airflow_url, token, run)
    finally:
        warehouse_output(env, REMOVE_BROKEN_ROW)


# The settings of a deployment: a client of its own, which takes an account of a client of the
# demo, and a name of its own for a client of the demo.
OWN_CLIENTS = """\
[clients.acme]
name = "Acme Outdoor"

[clients.tarnwick]
name = "Tarnwick of the deployment"

[[accounts]]
platform = "demo_search"
account_id = "4170000011"
client = "acme"
"""


def test_a_deployment_names_its_own_client_for_an_account(
    env, airflow_url, token, transform_run, put_back
):
    path = settings_file(env, "clients.toml")
    path.write_text(OWN_CLIENTS, encoding="utf-8")
    account = "platform = 'demo_search' AND account_id = '4170000011'"
    try:
        run = transform(airflow_url, token)
        assert run["state"] == "success", run["state"]
        shown = f"SELECT client_name FROM marts.ad_accounts WHERE {account}"  # noqa: S608 (constants of this file)
        assert warehouse_output(env, shown) == "Acme Outdoor"
        assert warehouse_output(env, f"SELECT DISTINCT client_id FROM {MART} WHERE {account}") == (  # noqa: S608 (constants of this file)
            "acme"
        )
        # The deployment's name of a client of the demo wins over the demo's.
        renamed = "SELECT client_name FROM marts.clients WHERE client_id = 'tarnwick'"
        assert warehouse_output(env, renamed) == "Tarnwick of the deployment"
    finally:
        path.unlink(missing_ok=True)


def test_a_broken_settings_file_stops_the_transform_and_keeps_the_marts(
    env, airflow_url, token, transform_run, put_back
):
    before = totals(env, MART)
    path = settings_file(env, "clients.toml")
    path.write_text('[clients.Acme]\nname = "Acme Outdoor"\n', encoding="utf-8")
    try:
        run = transform(airflow_url, token)
        assert run["state"] == "failed", run["state"]
        instances = task_instances(airflow_url, token, run)
        states = {i["task_id"]: i["state"] for i in instances}
        assert states.pop(SETTINGS) == "failed"
        assert set(states.values()) == {"upstream_failed"}, sorted(set(states.values()))
        # The one line of the mistake, as the settings name it: the file and the entry.
        [failed] = [i for i in instances if i["task_id"] == SETTINGS]
        line = "clients.toml: [clients.Acme]: a client ID is"
        assert logged(task_log(airflow_url, token, failed), line)
        assert totals(env, MART) == before
    finally:
        path.unlink(missing_ok=True)
