"""The metadata of the marts in Metabase (ADR-0016): the export that metadata-export writes, and
the names, descriptions and types that metabase-init and demo-init apply, with the overrides of the
deployment.

The tests change the metadata of the stack that they run against, as a person could, and put it
back: an edit through the API, which the next run of metabase-init reverts, a hidden field, and a
file of overrides, which they remove; they leave a file that the deployment has alone. The output
of the jobs and the answers of Metabase could hold secrets: the output is scanned where it is read,
and the assertions print names and the values of the metadata, which are no secrets.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import demo_init
import metadata
import pytest
from metabase_content import item, registry, run_job
from services import DEMO
from stack_helpers import REPO_ROOT, compose, http_json, leaked, read_env, secrets

needs_the_demo = pytest.mark.skipif(
    not DEMO, reason="the demo is off: COMPOSE_PROFILES in .env does not list demo"
)
# The daily load of the demo stays paused while these tests run (conftest.py).
pytestmark = pytest.mark.usefixtures("daily_load_paused")

EXPORT = "/opt/agoranomos/metadata/metadata.json"
MART = ("marts", "ad_stats_campaign_daily")
WAREHOUSE_NAME = "Agoranomos warehouse"
EDIT = "Edited by a stack test"
OVERRIDE = """# Written by tests/stack/test_metabase_metadata.py, which removes it.
[marts.ad_stats_campaign_daily.columns.conversions]
display_name = "Purchases (stack test)"
note = "A note of the stack test."
"""
# The questions of the demo, by their keys in the registry of the content, and by their names.
QUESTIONS = {question["key"]: question for question in demo_init.QUESTIONS}
QUESTIONS_BY_NAME = {question["name"]: question for question in demo_init.QUESTIONS}
# The first color of the palette of the project for a light background.
SERIES_COLOR = "#A86400"
KINDS = (metadata.FIRST, metadata.UPDATE, metadata.REVERT)


def overrides_file() -> Path:
    """The overrides file of the deployment, in the extensions folder that .env names."""
    folder = read_env(REPO_ROOT / ".env").get("AGORANOMOS_EXTENSIONS_DIR") or "./extensions"
    return (REPO_ROOT / folder).resolve() / "metadata" / "overrides.toml"


def run_metabase_init(values: dict[str, str], *arguments: str) -> tuple[int, list[str], list[str]]:
    """metabase-init once more, as every start runs it (diff with the argument diff): its exit
    code, the lines that say what it changed, and the names of the secrets in its output, which
    stays in this frame."""
    command = ["python", "/opt/agoranomos/metabase/metabase_init.py", *arguments]
    result = compose("run", "--rm", "--no-deps", "-T", "metabase-init", *command, check=False)
    prefixes = tuple(f"metabase-init: {kind}: " for kind in KINDS) + KINDS
    changes = [line for line in result.stdout.splitlines() if line.startswith(prefixes)]
    return result.returncode, changes, leaked(values, [result.stdout, result.stderr])


@pytest.fixture(scope="module")
def marts(tmp_path_factory) -> metadata.Metadata:
    """The effective metadata now: the export in the volume, and the overrides of the deployment."""
    result = compose(
        "run",
        "--rm",
        "--no-deps",
        "-T",
        "--entrypoint",
        "cat",
        "metabase-init",
        EXPORT,
        check=False,
    )
    assert result.returncode == 0, f"cannot read {EXPORT}: exit code {result.returncode}"
    export = tmp_path_factory.mktemp("metadata") / "metadata.json"
    export.write_text(result.stdout, encoding="utf-8")
    return metadata.load(str(export), str(overrides_file()))


def warehouse_metadata(metabase_url: str, session: dict[str, str]) -> dict[str, Any]:
    status, databases = http_json("GET", f"{metabase_url}/api/database", headers=session)
    assert status == 200, f"GET /api/database answered HTTP {status}"
    [warehouse] = [d for d in databases["data"] if d.get("name") == WAREHOUSE_NAME]
    path = f"/api/database/{warehouse['id']}/metadata?include_hidden=true"
    status, answer = http_json("GET", f"{metabase_url}{path}", headers=session)
    assert status == 200, f"GET {path} answered HTTP {status}"
    return answer


def field_of(metabase_url: str, session: dict[str, str], name: str) -> dict[str, Any]:
    tables = metadata.tables_of(warehouse_metadata(metabase_url, session))
    return next(f for f in tables[MART]["fields"] if f["name"] == name)


def put_field(metabase_url: str, session: dict[str, str], field_id: int, body: dict) -> None:
    status, _ = http_json("PUT", f"{metabase_url}/api/field/{field_id}", body, headers=session)
    assert status == 200, f"PUT /api/field/{field_id} answered HTTP {status}"


def test_metadata_export_wrote_the_export_as_nobody(marts):
    result = compose(
        "run",
        "--rm",
        "--no-deps",
        "-T",
        "--entrypoint",
        "stat",
        "metabase-init",
        "-c",
        "%u %a",
        EXPORT,
        check=False,
    )
    assert (result.returncode, result.stdout.strip()) == (0, "65534 644")
    assert MART in marts.effective


def test_metadata_export_wrote_the_manifest_that_airflow_reads():
    # As nobody, and every Airflow service reads it: the DAG processor here.
    result = compose(
        "exec",
        "-T",
        "airflow-dag-processor",
        "python",
        "-c",
        "import json, os; p = '/opt/agoranomos/manifest/manifest.json'; "
        "m = json.load(open(p)); print(os.stat(p).st_uid, len(m['nodes']))",
        check=False,
    )
    assert result.returncode == 0, result.stderr[-500:]
    owner, nodes = map(int, result.stdout.split())
    assert owner == 65534
    assert nodes > 0


@needs_the_demo
def test_metabase_shows_the_effective_metadata_of_the_marts(metabase_url, session, marts):
    shown = metadata.tables_of(warehouse_metadata(metabase_url, session))
    wrong = []
    for key, table in marts.effective.items():
        found = shown[key]
        wrong += [
            f"{table.name} {prop}"
            for prop in ("display_name", "description")
            if found[prop] != getattr(table, prop)
        ]
        fields = {f["name"]: f for f in found["fields"]}
        for column in table.columns:
            field = fields[column.name]
            wanted = {
                "display_name": column.display_name,
                "description": column.description,
                "semantic_type": column.semantic_type,
            }
            current = {**field, "currency": (field.get("settings") or {}).get("currency")}
            if column.currency is not None:
                wanted["currency"] = column.currency
            if not column.semantic_type_declared:
                del wanted["semantic_type"]  # Metabase keeps its own
            wrong += [
                f"{table.name}.{column.name} {prop}"
                for prop, value in wanted.items()
                if current[prop] != value
            ]
    assert not wrong, wrong


@needs_the_demo
def test_the_dry_run_finds_nothing_to_change_after_a_start(env):
    result = subprocess.run(
        [str(REPO_ROOT / "agoranomos"), "metadata", "diff"],
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    found = leaked(secrets(env), [result.stdout, result.stderr])
    assert not found, f"the output of metadata diff holds the values of {found}"
    assert result.returncode == 0, f"metadata diff exited with {result.returncode}"
    last = result.stdout.splitlines()[-1]
    assert last == "Metabase shows the metadata of the code: nothing to change"


@needs_the_demo
def test_an_edit_in_metabase_is_reverted_by_the_next_start_and_logged(
    env, metabase_url, session, marts
):
    [conversions] = [c for c in marts.effective[MART].columns if c.name == "conversions"]
    field = field_of(metabase_url, session, "conversions")
    put_field(metabase_url, session, field["id"], {"display_name": EDIT})
    try:
        code, changes, found = run_metabase_init(secrets(env))
        assert not found, f"the output of metabase-init holds the values of {found}"
        assert code == 0, f"metabase-init exited with {code}"
        assert changes == [
            "metabase-init: revert of an edit: "
            "marts.ad_stats_campaign_daily.conversions display_name: "
            f'"{EDIT}" -> {json.dumps(conversions.display_name, ensure_ascii=False)}'
        ]
    finally:
        if field_of(metabase_url, session, "conversions")["display_name"] == EDIT:
            run_metabase_init(secrets(env))
    shown = field_of(metabase_url, session, "conversions")["display_name"]
    assert shown == conversions.display_name


@needs_the_demo
def test_a_hidden_field_gets_its_metadata_and_stays_hidden(env, metabase_url, session, marts):
    [spend] = [c for c in marts.effective[MART].columns if c.name == "spend_reporting"]
    field = field_of(metabase_url, session, "spend_reporting")
    put_field(
        metabase_url, session, field["id"], {"visibility_type": "hidden", "display_name": EDIT}
    )
    try:
        code, changes, found = run_metabase_init(secrets(env))
        assert not found, f"the output of metabase-init holds the values of {found}"
        assert code == 0, f"metabase-init exited with {code}"
        assert changes == [
            "metabase-init: revert of an edit: "
            "marts.ad_stats_campaign_daily.spend_reporting display_name: "
            f'"{EDIT}" -> {json.dumps(spend.display_name, ensure_ascii=False)}'
        ]
        after = field_of(metabase_url, session, "spend_reporting")
        assert after["display_name"] == spend.display_name
        assert after["visibility_type"] == "hidden"
    finally:
        # Both properties that the test edited go back, also when it failed before the job set the
        # name back.
        before = {key: field[key] for key in ("visibility_type", "display_name")}
        put_field(metabase_url, session, field["id"], before)


@needs_the_demo
def test_an_override_shows_after_the_next_start_and_its_removal_brings_the_base_back(
    env, metabase_url, session, marts
):
    path = overrides_file()
    if path.exists():
        pytest.skip(f"the deployment has an overrides file of its own: {path}")
    [base] = [c for c in marts.effective[MART].columns if c.name == "conversions"]
    try:
        path.write_text(OVERRIDE, encoding="utf-8")
        code, _, found = run_metabase_init(secrets(env))
        assert not found, f"the output of metabase-init holds the values of {found}"
        assert code == 0, f"metabase-init exited with {code}"
        field = field_of(metabase_url, session, "conversions")
        assert field["display_name"] == "Purchases (stack test)"
        assert field["description"] == f"{base.description}\n\nA note of the stack test."
    finally:
        path.unlink(missing_ok=True)
        code, _, _ = run_metabase_init(secrets(env))
    assert code == 0, f"metabase-init exited with {code} after the file was removed"
    field = field_of(metabase_url, session, "conversions")
    assert (field["display_name"], field["description"]) == (base.display_name, base.description)


def test_metadata_export_renders_the_currency_of_the_setting(env, marts):
    # dbt renders env_var() in meta when it reads the project (ADR-0025), so the export carries the
    # code of the setting, which metabase-init gives the field.
    [column] = [c for c in marts.effective[MART].columns if c.name == "spend_reporting"]
    assert column.currency == (env.get("AGORANOMOS_REPORTING_CURRENCY") or "USD")
    assert column.semantic_type == "type/Currency"


@needs_the_demo
def test_the_reporting_currency_comes_from_the_setting(env, metabase_url, session):
    # The money of the reporting currency is money in Metabase, in the currency of the setting.
    field = field_of(metabase_url, session, "spend_reporting")
    shown = (field["semantic_type"], (field.get("settings") or {}).get("currency"))
    assert shown == ("type/Currency", env.get("AGORANOMOS_REPORTING_CURRENCY") or "USD")


def demo_questions(metabase_url: str, session: dict[str, str]) -> dict[str, dict[str, Any]]:
    """The questions of the demo by name, found through the registry of the content."""
    entries = registry()
    found = (item(metabase_url, session, f"demo/{key}", entries) for key in QUESTIONS)
    return {question["name"]: question for question in found}


@needs_the_demo
def test_the_demo_questions_have_the_titles_of_their_columns_and_axes(metabase_url, session):
    # Every column of every question has a title, and every chart the titles of its axes.
    titled = {}
    for name, card in demo_questions(metabase_url, session).items():
        settings = card["visualization_settings"]
        names = [json.loads(key)[1] for key in settings["column_settings"]]
        titles = [entry.get("column_title") for entry in settings["column_settings"].values()]
        axes = [settings.get(key) for key in ("graph.x_axis.title_text", "graph.y_axis.title_text")]
        charted = card["display"] in ("scalar", "table") or all(axes)
        every = sorted(names) == sorted(QUESTIONS_BY_NAME[name]["columns"])
        titled[name] = all(titles) and charted and every
    assert titled == dict.fromkeys(QUESTIONS_BY_NAME, True), titled


@needs_the_demo
def test_the_demo_charts_are_drawn_in_the_color_of_the_project(metabase_url, session):
    # Metabase keys the color of the one series of a chart by the name of its metric column.
    # A chart with a breakout, a number and a table keep the colors of Metabase.
    colors = {
        name: card["visualization_settings"].get("series_settings")
        for name, card in demo_questions(metabase_url, session).items()
    }
    assert colors == {
        **dict.fromkeys(QUESTIONS_BY_NAME),
        "Spend by client": {"spend_reporting": {"color": SERIES_COLOR}},
    }, colors


@needs_the_demo
def test_metabase_lists_the_columns_of_the_warehouse_through_its_connection(
    metabase_url, session, marts
):
    # The jobs ask this when Metabase does not show a mart with the fields of its columns: to tell
    # a mart that dbt has not built yet from one that waits for a sync, and to know which fields
    # to wait for. The answer of Metabase 0.63.19 and 0.64.1 to a native query, one row a column.
    status, databases = http_json("GET", f"{metabase_url}/api/database", headers=session)
    assert status == 200, f"GET /api/database answered HTTP {status}"
    [warehouse] = [d for d in databases["data"] if d.get("name") == WAREHOUSE_NAME]
    query = {"type": "native", "native": {"query": metadata.WAREHOUSE_COLUMNS}}
    status, answer = http_json(
        "POST",
        f"{metabase_url}/api/dataset",
        {"database": warehouse["id"], **query},
        headers=session,
    )
    assert (status, answer.get("status")) in {(200, "completed"), (202, "completed")}, status
    rows = {tuple(row) for row in answer["data"]["rows"]}
    declared = {(*MART, column.name) for column in marts.effective[MART].columns}
    assert declared <= rows, sorted(declared - rows)


@needs_the_demo
def test_demo_init_puts_back_the_title_of_a_column(env, metabase_url, session, marts):
    # An admin removes the title of a column, the title of an axis and the color of a chart: the
    # next start puts the settings of display back as the code says, and logs it. The settings go
    # back as they were also when the test fails.
    [spend] = [c for c in marts.effective[MART].columns if c.name == "spend_reporting"]
    card = item(metabase_url, session, "demo/by-client")
    card_path = f"{metabase_url}/api/card/{card['id']}"
    before = card["visualization_settings"]
    omitted = ("graph.y_axis.title_text", "series_settings")
    lacking = {key: value for key, value in before.items() if key not in omitted}
    lacking["column_settings"] = {
        key: {k: v for k, v in entry.items() if k != "column_title"}
        for key, entry in before["column_settings"].items()
    }
    try:
        status, _ = http_json(
            "PUT", card_path, {"visualization_settings": lacking}, headers=session
        )
        assert status == 200, f"PUT /api/card/{card['id']} answered HTTP {status}"
        code, changes, found, _ = run_job("demo-init", secrets(env))
        assert not found, f"the output of demo-init holds the values of {found}"
        assert code == 0, f"demo-init exited with {code}"
        settings = item(metabase_url, session, "demo/by-client")["visualization_settings"]
        assert settings.get("graph.y_axis.title_text") == spend.display_name
        assert settings["column_settings"]['["name","spend_reporting"]'] == {
            "column_title": spend.display_name,
            "number_style": "currency",
            "currency": spend.currency,
        }
        assert settings.get("series_settings") == {"spend_reporting": {"color": SERIES_COLOR}}
        assert settings["graph.dimensions"] == ["client_name"]
        assert changes == [
            "demo-init: revert of an edit: question demo/by-client visualization_settings"
        ], changes
    finally:
        # No assert here: it would hide the failure of the test when the way back fails too.
        back, _ = http_json("PUT", card_path, {"visualization_settings": before}, headers=session)
    assert back == 200, f"PUT /api/card/{card['id']} answered HTTP {back} on the way back"
