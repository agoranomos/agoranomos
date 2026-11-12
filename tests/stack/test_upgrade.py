"""The upgrade from the previous release (ADR-0044): the stack of the tag of that release, with the
demo, then the current code in the same checkout, on the same volumes, and ./agoranomos up.

The workflow Upgrade runs it in two steps. AGORANOMOS_UPGRADE=before runs on the stack of the
previous release, once the checkout holds the current code and before ./agoranomos up: it records
the relations of the warehouse with their oids, the migrations of the core that it has applied,
the backups, the rows of the marts, the dashboards of the package and the tables and fields of the
marts as Metabase knows them. AGORANOMOS_UPGRADE=after, once ./agoranomos up has upgraded the
stack, checks what the upgrade must do: db-init applied and recorded every migration that waited,
up took a backup before them whose manifest names them, marts-init built the views of the marts
and dropped the relations of the release that no model builds or reads, every card of the
dashboards of the package answers, on the same tables and fields of Metabase, the metadata of the
marts is applied, the raw tables of the demo are in the shape of the current code, no relation of
the release stays that the current code neither builds nor reads, and the demo goes on, with a
daily load and a run of the transform. AGORANOMOS_UPGRADE_FILE
names the file in which the first step leaves what it found for the second. With
AGORANOMOS_UPGRADE_SUMMARY, the second step appends to that file, such as the summary of a job of
CI, the rows of the marts before and after, and the relations of the previous release that the
upgrade left as they were. Without these variables the tests skip: they need a stack of the
previous release, which no other run of the stack tests has.

The assertions print names, numbers, states and kinds of relations, never a secret.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import marts_init
import metadata
import pytest
from agoranomos_pipelines.demo import platforms
from airflow_api import dag_runs, is_busy, pause, trigger, unpause, wait_for_dag, wait_for_run
from metabase_content import item, registry, warehouse_database_id
from stack_helpers import (
    REPO_ROOT,
    compose,
    containers,
    http_json,
    leaked,
    read_env,
    secrets,
    wait_for,
    warehouse_output,
)

pytestmark = [pytest.mark.demo, pytest.mark.transform]

STEP = os.environ.get("AGORANOMOS_UPGRADE", "")
FOUND = Path(os.environ.get("AGORANOMOS_UPGRADE_FILE", "/nonexistent"))
SUMMARY = os.environ.get("AGORANOMOS_UPGRADE_SUMMARY", "")
before_only = pytest.mark.skipif(STEP != "before", reason="only before the upgrade")
after_only = pytest.mark.skipif(STEP != "after", reason="only after the upgrade")

MIGRATIONS = REPO_ROOT / "postgres" / "migrations"
MARTS = sorted(path.stem for path in (REPO_ROOT / "dbt" / "models" / "marts").glob("*.sql"))
# The env fixture says what is missing when there is no .env: the module still collects without it.
ENV_FILE = REPO_ROOT / ".env"
BACKUPS = REPO_ROOT / (
    (read_env(ENV_FILE).get("AGORANOMOS_BACKUP_DIR") if ENV_FILE.exists() else None) or "./backups"
)
HISTORY = "agoranomos_demo_history"
TRANSFORM = "agoranomos_demo_transform"
DAILY = "agoranomos_demo_daily"
# The journal of the loads of the releases before v0.5, whose loads merged the days in place, and
# the journal of the loads that append (airflow/dags/agoranomos_pipelines/loads.py).
OLD_JOURNAL = "_agoranomos_reloads"
JOURNAL = "_agoranomos_loads"
# The datasets of the demo, where its pipelines load.
DATASETS = sorted(platform.dataset for platform in platforms.PLATFORMS.values())
# The snapshot that the views of the marts read: marts-init builds what they read when it is
# missing (metabase/marts_init.py).
SNAPSHOT = "core.dim_campaign_versions"
# Every table, view and materialized view of the warehouse outside the schemas of PostgreSQL, with
# its kind and oid; the partitions of a table come with it.
RELATIONS = (
    "SELECT concat_ws('|', n.nspname, c.relname, c.relkind, c.oid) "
    "FROM pg_catalog.pg_class AS c JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace "
    "WHERE c.relkind IN ('r', 'p', 'v', 'm') AND NOT c.relispartition "
    "AND n.nspname <> 'information_schema' AND n.nspname !~ '^pg_' ORDER BY 1"
)
SCHEMAS = (
    "SELECT nspname FROM pg_catalog.pg_namespace "
    "WHERE nspname <> 'information_schema' AND nspname !~ '^pg_' ORDER BY 1"
)
HAS_MIGRATIONS = "SELECT to_regclass('core.schema_migrations') IS NOT NULL"
APPLIED = (
    "SELECT concat_ws('|', version, name, checksum, extract(epoch FROM applied_at)) "
    "FROM core.schema_migrations ORDER BY version"
)
MANIFEST = "/opt/agoranomos/manifest/manifest.json"
# The tables that dlt keeps beside those of a source in each dataset that it loads: the record of
# its loads, the state of its pipeline and the versions of its schema (dlt.common.schema.typing of
# dlt 1.32.0, as of November 2026).
DLT_TABLES = ("_dlt_loads", "_dlt_pipeline_state", "_dlt_version")
# The extensions that db-init creates in the warehouse on every start, such as pg_stat_statements,
# and the relations of each extension of the warehouse, by extension.
EXTENSIONS = re.findall(
    r"^CREATE EXTENSION IF NOT EXISTS (\w+);$",
    (REPO_ROOT / "postgres" / "warehouse.sql").read_text(encoding="utf-8"),
    flags=re.MULTILINE,
)
OF_EXTENSIONS = (
    "SELECT concat_ws('|', e.extname, n.nspname, c.relname) FROM pg_catalog.pg_depend AS d "
    "JOIN pg_catalog.pg_extension AS e ON e.oid = d.refobjid "
    "JOIN pg_catalog.pg_class AS c ON c.oid = d.objid "
    "JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace "
    "WHERE d.classid = 'pg_catalog.pg_class'::regclass "
    "AND d.refclassid = 'pg_catalog.pg_extension'::regclass AND d.deptype = 'e' ORDER BY 1"
)


def relations(env) -> dict[str, tuple[str, int]]:
    """The kind and the oid of each relation of the warehouse, by schema.name."""
    found = {}
    for line in warehouse_output(env, RELATIONS).splitlines():
        schema, name, kind, oid = line.split("|")
        found[f"{schema}.{name}"] = (kind, int(oid))
    return found


def applied(env) -> dict[int, tuple[str, str, float]]:
    """The migrations of the core that the warehouse has applied, by number: the name, the
    checksum and when it was applied, in seconds. Empty when the warehouse has no table of the
    migrations, as one of a release before v0.5."""
    if warehouse_output(env, HAS_MIGRATIONS) != "t":
        return {}
    found = {}
    for line in warehouse_output(env, APPLIED).splitlines():
        version, name, checksum, at = line.split("|")
        found[int(version)] = (name, checksum, float(at))
    return found


def migration_files() -> dict[int, tuple[str, str]]:
    """The migrations of the checkout, by number: the name and the SHA-256 of the file, which
    db-init records as its checksum."""
    return {
        int(path.name[:4]): (path.stem[5:], hashlib.sha256(path.read_bytes()).hexdigest())
        for path in sorted(MIGRATIONS.glob("[0-9][0-9][0-9][0-9]_*.sql"))
    }


def backups() -> list[str]:
    """The names of the complete backups in the backups folder: a folder with a manifest."""
    if not BACKUPS.is_dir():
        return []
    return sorted(path.parent.name for path in BACKUPS.glob("*/manifest.json"))


def manifest(name: str) -> dict[str, Any]:
    return json.loads((BACKUPS / name / "manifest.json").read_text(encoding="utf-8"))


def mart_rows(env, names) -> dict[str, int]:
    """The number of rows of each mart that the warehouse holds, by name."""
    found = relations(env)
    return {
        name: int(warehouse_output(env, f"SELECT count(*) FROM marts.{name}"))  # noqa: S608 (a name of the catalog)
        for name in names
        if f"marts.{name}" in found
    }


def metabase_marts(metabase_url, session) -> dict[str, dict[str, Any]]:
    """The tables of the schema marts that Metabase shows, by name: the ID of each and of each of
    its fields, by name."""
    database = warehouse_database_id(metabase_url, session)
    path = f"/api/database/{database}/metadata?include_hidden=true"
    status, answer = http_json("GET", f"{metabase_url}{path}", headers=session)
    assert status == 200, f"GET {path} answered HTTP {status}"
    return {
        name: {"id": table["id"], "fields": {f["name"]: f["id"] for f in table["fields"]}}
        for (schema, name), table in metadata.tables_of(answer).items()
        if schema == "marts"
    }


def dashboards(metabase_url, session) -> dict[str, dict[str, Any]]:
    """The dashboards of the package in Metabase, by their keys in the registry of the content."""
    entries = registry()
    return {
        key: item(metabase_url, session, key, entries)
        for key, entry in sorted(entries.items())
        if entry["model"] == "dashboard"
    }


def parameters(board: dict[str, Any], dashcard: dict[str, Any]) -> list[dict[str, Any]]:
    """The filters of a card with the default values of its dashboard, as a person who opens the
    dashboard gets them."""
    shapes = {shape["id"]: shape for shape in board.get("parameters") or []}
    found = []
    for mapping in dashcard.get("parameter_mappings") or []:
        shape = shapes.get(mapping["parameter_id"])
        if shape is None or shape.get("default") is None:
            continue
        found.append(
            {
                "id": shape["id"],
                "type": shape["type"],
                "value": shape["default"],
                "target": mapping["target"],
            }
        )
    return found


def unanswered(metabase_url, session, board: dict[str, Any]) -> list[str]:
    """The cards of a dashboard that do not answer with its default filters: the name of each,
    the HTTP status and the status of the query, and the first line of its error, which names
    a relation or a column of the warehouse and no secret."""
    failed = []
    for dashcard in board["dashcards"]:
        if dashcard.get("card_id") is None:
            continue  # a heading or a text
        path = (
            f"/api/dashboard/{board['id']}/dashcard/{dashcard['id']}"
            f"/card/{dashcard['card_id']}/query"
        )
        status, answer = http_json(
            "POST",
            f"{metabase_url}{path}",
            {"parameters": parameters(board, dashcard)},
            headers=session,
            timeout=180,
        )
        state = answer.get("status") if isinstance(answer, dict) else None
        if status in (200, 202) and state == "completed":
            continue
        error = str(answer.get("error") or "") if isinstance(answer, dict) else ""
        line = (error.strip().splitlines() or [""])[0][:200]
        failed.append(f"{board['name']} / {dashcard['card']['name']}: {status} {state} {line}")
    return failed


def started(run: dict[str, Any]) -> float:
    """When a run of a DAG started, or was to run when it has not started yet, in seconds."""
    moment = run.get("start_date") or run["run_after"]
    return datetime.fromisoformat(moment.replace("Z", "+00:00")).timestamp()


def built_by_the_code(env) -> set[str]:
    """The relations that the current code builds or reads, as schema.name: the models, seeds and
    snapshots of every package of the dbt project of the checkout, and its sources, from the
    manifest that metadata-export writes for Airflow; the tables of dlt in the dataset of each
    source, a raw schema; and the relations of the extensions that db-init creates."""
    result = compose("exec", "-T", "airflow-scheduler", "cat", MANIFEST, check=False)
    assert result.returncode == 0, f"cannot read {MANIFEST}: exit code {result.returncode}"
    found = json.loads(result.stdout)
    names = {
        f"{node['schema']}.{node.get('alias') or node['name']}"
        for node in found["nodes"].values()
        if node["resource_type"] in ("model", "seed", "snapshot")
        and node["config"].get("materialized") != "ephemeral"
    }
    sources = {(s["schema"], s["identifier"]) for s in found["sources"].values()}
    names |= {f"{schema}.{table}" for schema, table in sources}
    names |= {
        f"{schema}.{table}"
        for schema in {schema for schema, _ in sources if schema.startswith("raw_")}
        for table in DLT_TABLES
    }
    for line in warehouse_output(env, OF_EXTENSIONS).splitlines():
        extension, schema, name = line.split("|")
        if extension in EXTENSIONS:
            names.add(f"{schema}.{name}")
    return names


def recorded() -> dict[str, Any]:
    assert FOUND.exists(), f"{FOUND} is missing: run the step before the upgrade first"
    return json.loads(FOUND.read_text(encoding="utf-8"))


def summarize(lines: list[str]) -> None:
    if SUMMARY:
        with open(SUMMARY, "a", encoding="utf-8") as summary:
            summary.write("\n".join(lines) + "\n\n")


@before_only
def test_before_the_upgrade_record_the_stack_of_the_previous_release(env, metabase_url, session):
    found = relations(env)
    # The first start of the previous release has loaded the demo and transformed it.
    assert "marts.ad_stats_campaign_daily" in found, "the previous release built no mart"
    rows = mart_rows(env, sorted({key.split(".", 1)[1] for key in found if key[:6] == "marts."}))
    assert rows["ad_stats_campaign_daily"] > 0, "the first transform of the demo did not end"
    boards = dashboards(metabase_url, session)
    assert boards, "Metabase has no dashboard of the package"
    FOUND.write_text(
        json.dumps(
            {
                "at": time.time(),
                "relations": found,
                "schemas": warehouse_output(env, SCHEMAS).split(),
                "migrations": {str(k): v[0] for k, v in applied(env).items()},
                "backups": backups(),
                "rows": rows,
                "dashboards": {key: board["id"] for key, board in boards.items()},
                "unanswered": [
                    line for b in boards.values() for line in unanswered(metabase_url, session, b)
                ],
                "marts": metabase_marts(metabase_url, session),
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )


@after_only
def test_db_init_applied_and_recorded_every_migration_of_the_checkout(env):
    saved = recorded()
    files = migration_files()
    assert files, "postgres/migrations holds no migration"
    found = applied(env)
    assert {v: (name, checksum) for v, (name, checksum, _) in found.items()} == files
    # The migrations that waited were applied by the upgrade, and the others before it.
    waiting = sorted(set(files) - {int(v) for v in saved["migrations"]})
    assert [v for v in waiting if found[v][2] < saved["at"]] == []
    assert {int(v): name for v, name in saved["migrations"].items()} == {
        v: found[v][0] for v in found if v not in waiting
    }


@after_only
def test_up_took_a_backup_before_db_init_applied_the_migrations_that_waited(env):
    saved = recorded()
    files = migration_files()
    waiting = [
        f"{v:04d}_{files[v][0]}" for v in sorted(set(files) - set(map(int, saved["migrations"])))
    ]
    new = [name for name in backups() if name not in saved["backups"]]
    of_up = [name for name in new if manifest(name).get("migrations")]
    if not waiting:
        assert of_up == [], "up took a backup before migrations, and none waited"
        return
    [name] = [n for n in of_up if manifest(n)["migrations"] == " ".join(waiting)]
    found = manifest(name)
    assert found["kind"] == "manual"
    assert found["note"] == (
        "taken by ./agoranomos up before db-init applied the migrations of the core "
        + " ".join(waiting)
    )
    first = min(at for _, _, at in applied(env).values() if at >= saved["at"])
    assert found["finished_epoch"] <= first
    # The dump of the warehouse is the warehouse before the migrations: it holds the table of the
    # migrations only when the previous release had applied some.
    listed = compose(
        "run",
        "--rm",
        "--no-deps",
        "-T",
        "--entrypoint",
        "pg_restore",
        "backup",
        "--list",
        f"/backups/{name}/warehouse.dump",
        check=False,
    )
    assert listed.returncode == 0, f"pg_restore exited with {listed.returncode}"
    holds_the_table = " TABLE core schema_migrations " in listed.stdout
    assert holds_the_table == bool(saved["migrations"])


@after_only
def test_marts_init_built_the_views_of_the_marts_in_place_of_those_of_the_release(env):
    saved = recorded()
    job = containers()["marts-init"]
    assert (job["State"], job["ExitCode"]) == ("exited", 0)
    logs = compose("logs", "--no-log-prefix", "--no-color", "marts-init").stdout
    assert "marts-init: done" in logs
    # On a warehouse whose core has no snapshot yet, as one of v0.4, it builds what the views read
    # first, from the seeds to the snapshots.
    if SNAPSHOT not in saved["relations"]:
        assert "the snapshots that the views of marts read do not exist yet" in logs
    found = relations(env)
    marts = {key: value for key, value in found.items() if key.startswith("marts.")}
    assert {key: kind for key, (kind, _) in marts.items()} == dict.fromkeys(
        (f"marts.{name}" for name in MARTS), "v"
    )
    old = {key: value for key, value in saved["relations"].items() if key.startswith("marts.")}
    assert [key for key, (_, oid) in old.items() if marts.get(key, ("", 0))[1] == oid] == []


@after_only
def test_marts_init_dropped_the_relations_of_the_release_that_no_model_builds_or_reads(env):
    saved = recorded()
    logs = compose("logs", "--no-log-prefix", "--no-color", "marts-init").stdout
    # Those that the release built, as v0.4 built all of them: the upgrade dropped each, with a
    # line of the log that names it.
    left = [
        (name, kind)
        for name, kind in marts_init.LEFTOVERS
        if saved["relations"].get(name, ("", 0))[0] == marts_init.RELKINDS[kind]
    ]
    missing = [
        name
        for name, kind in left
        if f"marts-init: dropped the {kind.lower()} {name} of an earlier release" not in logs
    ]
    assert missing == []
    assert "marts-init: warning:" not in logs
    found = relations(env)
    assert [name for name, _ in marts_init.LEFTOVERS if name in found] == []


@after_only
@pytest.mark.usefixtures("daily_load_paused")
def test_every_card_of_the_dashboards_of_the_package_answers_on_the_same_tables(
    metabase_url, session
):
    saved = recorded()
    boards = dashboards(metabase_url, session)
    # Every dashboard of the release that the package keeps has its ID, so a link or a bookmark to
    # it keeps working. One that the current code retires is in the summary.
    kept = sorted(set(saved["dashboards"]) & set(boards))
    assert {key: boards[key]["id"] for key in kept} == {
        key: saved["dashboards"][key] for key in kept
    }
    assert not any(board.get("archived") for board in boards.values())
    failed = [
        line for board in boards.values() for line in unanswered(metabase_url, session, board)
    ]
    assert failed == []
    # A view takes the place of a table of the same name, and Metabase keeps the IDs of the table
    # and of each field that stays, which the questions and filters of a person name.
    shown = metabase_marts(metabase_url, session)
    tables = {name: shown[name]["id"] for name in saved["marts"] if name in shown}
    assert tables == {name: saved["marts"][name]["id"] for name in tables}
    moved = [
        f"{name}.{field}"
        for name in tables
        for field, field_id in saved["marts"][name]["fields"].items()
        if field in shown[name]["fields"] and shown[name]["fields"][field] != field_id
    ]
    assert moved == []


@after_only
def test_metabase_shows_the_metadata_of_the_marts_after_the_upgrade(env):
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
    assert result.stdout.splitlines()[-1] == (
        "Metabase shows the metadata of the code: nothing to change"
    )


@after_only
def test_the_raw_tables_of_the_demo_are_loaded_anew_and_what_stays_is_listed(env):
    saved = recorded()
    found = relations(env)
    old = saved["relations"]
    in_the_old_shape = [d for d in DATASETS if f"{d}.{OLD_JOURNAL}" in old]
    # What the upgrade left as it was: a relation of the release with the same oid.
    kept = sorted(key for key, value in old.items() if tuple(found.get(key, ())) == tuple(value))
    used = built_by_the_code(env)
    empty = sorted(
        schema
        for schema in warehouse_output(env, SCHEMAS).split()
        if not any(key.startswith(f"{schema}.") for key in found)
    )
    rows = mart_rows(env, MARTS)
    gone = sorted(
        set(saved["dashboards"]) - {k for k, e in registry().items() if e["model"] == "dashboard"}
    )
    summarize(
        [
            "### What the upgrade left of the previous release",
            "",
            "The cards that did not answer before the upgrade: "
            + ("; ".join(saved["unanswered"]) or "none")
            + ".",
            "",
            "The dashboards of the package that the upgrade retired: "
            + (", ".join(f"`{key}`" for key in gone) or "none")
            + ".",
            "",
            "| Mart | Rows before | Rows after |",
            "|---|---:|---:|",
            *(
                f"| {name} | {saved['rows'].get(name, '')} | {rows.get(name, '')} |"
                for name in sorted(set(saved["rows"]) | set(rows))
            ),
            "",
            "The relations that kept their oid, which the dbt project of the current code builds "
            "or reads: " + (", ".join(f"`{k}`" for k in kept if k in used) or "none") + ".",
            "",
            "The relations that kept their oid, which the current code neither builds nor reads: "
            + (", ".join(f"`{k}`" for k in kept if k not in used) or "none")
            + ".",
            "",
            "The schemas that hold no relation: "
            + (", ".join(f"`{s}`" for s in empty) or "none")
            + ".",
        ]
    )
    # The loads of the demo of a release before v0.5 merged in place: the history of the upgrade
    # dropped every table of such a dataset and loaded it again, each load under its own id.
    for dataset in in_the_old_shape:
        assert f"{dataset}.{OLD_JOURNAL}" not in found, dataset
        assert f"{dataset}.{JOURNAL}" in found, dataset
        assert [key for key in kept if key.startswith(f"{dataset}.")] == [], dataset
    # Every load appends, so the staging datasets of dlt hold nothing; the schemas stay, as
    # warehouse.sql makes them.
    assert [
        key for key in found if key.split(".", 1)[0] in {f"{d}_staging" for d in DATASETS}
    ] == []
    # The marts are views that marts-init built, and no table of the release stays there.
    assert [key for key in kept if key.startswith("marts.")] == []
    # What stays of the release with its oid, the current code builds or reads: marts-init dropped
    # the rest.
    assert [key for key in kept if key not in used] == []
    # The demo shows its data again.
    assert rows["ad_stats_campaign_daily"] > 0
    assert rows["web_sessions_daily"] > 0


@after_only
@pytest.mark.usefixtures("daily_load_paused")
def test_the_demo_goes_on_after_the_upgrade(env, airflow_url, token):
    saved = recorded()
    old = saved["relations"]
    since = saved["at"]
    # The start of the upgrade ran the history again when the raw tables of the demo had the shape
    # of a release before v0.5, and the transform after it; every run since is green.
    history = [r for r in dag_runs(airflow_url, token, HISTORY, 50) if started(r) >= since]
    if any(f"{d}.{OLD_JOURNAL}" in old for d in DATASETS):
        assert history, "the upgrade did not run the history of the demo again"
    transforms = [r for r in dag_runs(airflow_url, token, TRANSFORM, 50) if started(r) >= since]
    assert transforms, "no run of the transform since the upgrade"
    states = [(r["dag_id"], r["dag_run_id"], r["state"]) for r in history + transforms]
    assert [s for s in states if s[2] != "success"] == []
    # A daily load and a run of the transform after it, as the next day brings them.
    wait_for_dag(airflow_url, token, DAILY)
    unpause(airflow_url, token, DAILY)
    try:
        run = trigger(airflow_url, token, DAILY)
        run = wait_for_run(airflow_url, token, DAILY, run["dag_run_id"], timeout=900)
        assert run["state"] == "success", run["state"]
        wait_for(
            lambda: not is_busy(airflow_url, token, DAILY),
            900,
            interval=5,
            message=f"the runs of {DAILY} did not end",
        )
    finally:
        pause(airflow_url, token, DAILY)
    wait_for(
        lambda: not is_busy(airflow_url, token, TRANSFORM, assets=len(platforms.PLATFORMS)),
        2400,
        interval=10,
        message="the runs of the transform that the daily load started did not end",
    )
    unpause(airflow_url, token, TRANSFORM)
    run = trigger(airflow_url, token, TRANSFORM)
    run = wait_for_run(airflow_url, token, TRANSFORM, run["dag_run_id"], timeout=2400)
    assert run["state"] == "success", run["state"]
    later = [r for r in dag_runs(airflow_url, token, TRANSFORM, 50) if started(r) >= since]
    assert [r["dag_run_id"] for r in later if r["state"] != "success"] == []
