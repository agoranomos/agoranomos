"""The descriptions of the dbt project as dbt itself reads them, and their copies in the warehouse
(docs/reference/describe-the-marts.md).

dbt ls resolves the doc blocks and the configs as dbt does. It needs no database and no secret, so
it runs in airflow-dag-processor, which holds no password of the warehouse. The comments that
persist_docs writes are read as the superuser of the warehouse. The assertions print names, never
a description: the checks are functions of the output of dbt ls, and a test of their own shows that
each one finds what it looks for.
"""

from __future__ import annotations

import json
from collections import defaultdict
from typing import Any

import pytest
from airflow_api import is_busy, trigger, unpause, wait_for_dag, wait_for_run
from services import DEMO
from stack_helpers import compose, wait_for, warehouse_output

DBT = "/opt/airflow/dbt_venv/bin/dbt"
PROJECT = "/opt/agoranomos/dbt"
# dbt writes its target and logs folders here, in the container of the DAG processor, apart from
# the folders of Cosmos.
WORK = "/tmp/agoranomos-stack-test-dbt"  # noqa: S108 (a folder in the container, not on this host)
KEYS = (
    "unique_id",
    "resource_type",
    "schema",
    "name",
    "description",
    "columns",
    "config",
    "meta",
    "attached_node",
    "column_name",
    "test_metadata",
)
# The layers whose models and columns all need a description, and the one that Metabase shows.
LAYERS = ("staging", "marts")
MARTS = "marts"
# The semantic types that Metabase shows as money: type/Currency and the types under it.
CURRENCY_TYPES = {
    "type/Currency",
    "type/Cost",
    "type/Discount",
    "type/GrossMargin",
    "type/Income",
    "type/Price",
}
# The style of a display name: Title Case, with these words in lower case after the first word,
# and these abbreviations in capitals.
SMALL_WORDS = {
    "a",
    "an",
    "and",
    "as",
    "at",
    "by",
    "for",
    "in",
    "of",
    "on",
    "or",
    "per",
    "the",
    "to",
}
ABBREVIATIONS = {"ID", "CTR", "CPC", "CPM", "CPA", "ROAS", "ISO"}
LOAD = "agoranomos_demo_load"
TRANSFORM = "agoranomos_demo_transform"
# The comments of the relations of the layers, and of their columns, as one JSON document:
# [schema, relation, column or null, comment or null]. A comment may hold any character.
COMMENTS = """
SELECT coalesce(json_agg(item), '[]') FROM (
  SELECT json_build_array(n.nspname, c.relname, NULL, obj_description(c.oid, 'pg_class')) AS item
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname IN ('staging', 'marts') AND c.relkind IN ('r', 'v')
  UNION ALL
  SELECT json_build_array(n.nspname, c.relname, a.attname, col_description(c.oid, a.attnum))
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
  WHERE n.nspname IN ('staging', 'marts') AND c.relkind IN ('r', 'v')
) AS comments
"""
PRIMARY_KEY = (
    "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
    "WHERE conrelid = 'marts.ad_spend_daily'::regclass AND contype = 'p'"
)

needs_the_demo = pytest.mark.skipif(
    not DEMO, reason="the demo is off: COMPOSE_PROFILES in .env does not list demo"
)


def run_dbt_ls() -> list[dict[str, Any]]:
    """The models, sources and tests of the project, as dbt ls prints them: one object a line."""
    result = compose(
        "exec",
        "-T",
        "airflow-dag-processor",
        DBT,
        "--quiet",
        "--no-use-colors",
        "ls",
        "--project-dir",
        PROJECT,
        "--profiles-dir",
        PROJECT,
        "--target-path",
        f"{WORK}/target",
        "--log-path",
        f"{WORK}/logs",
        "--resource-type",
        "model",
        "--resource-type",
        "source",
        "--resource-type",
        "test",
        "--output",
        "json",
        "--output-keys",
        *KEYS,
        check=False,
    )
    # dbt prints an error on standard output; the project and its errors hold no secret.
    assert result.returncode == 0, (
        f"dbt ls exited with {result.returncode}: {result.stdout[-1500:]}"
    )
    return [json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")]


@pytest.fixture(scope="module")
def nodes() -> list[dict[str, Any]]:
    found = run_dbt_ls()
    ids = {node["unique_id"] for node in found}
    # Not vacuous: the checks below must see the models and the source of the demo.
    expected = {
        "model.agoranomos.ad_spend_daily",
        "model.agoranomos.stg_demo__ad_stats_daily",
        "source.agoranomos.raw_demo.ad_stats_daily",
    }
    assert expected <= ids, f"dbt ls does not list {sorted(expected - ids)}"
    return found


def models(nodes: list[dict[str, Any]], layers: tuple[str, ...] = LAYERS) -> list[dict[str, Any]]:
    return [n for n in nodes if n["resource_type"] == "model" and n["schema"] in layers]


def metabase_meta(item: dict[str, Any]) -> dict[str, Any]:
    """What a model or a column declares for Metabase, under config: meta: metabase:."""
    return (item.get("meta") or {}).get("metabase") or {}


def missing_descriptions(nodes: list[dict[str, Any]]) -> list[str]:
    """The models and columns of the layers, and the tables of the sources, with no description."""
    missing = []
    for node in models(nodes):
        if not (node.get("description") or "").strip():
            missing.append(node["name"])
        for column in node["columns"].values():
            if not (column.get("description") or "").strip():
                missing.append(f"{node['name']}.{column['name']}")
    for node in nodes:
        if node["resource_type"] == "source" and not (node.get("description") or "").strip():
            missing.append(node["unique_id"])
    return sorted(missing)


def ragged_descriptions(nodes: list[dict[str, Any]]) -> list[str]:
    """The descriptions that start or end with white space, such as the line break that a folded
    description (>) leaves at its end, and that the comment in the warehouse would keep."""
    ragged = []
    for node in [*models(nodes), *(n for n in nodes if n["resource_type"] == "source")]:
        texts = {node["name"]: node.get("description") or ""}
        texts |= {
            f"{node['name']}.{c}": d.get("description") or "" for c, d in node["columns"].items()
        }
        ragged += [where for where, text in texts.items() if text != text.strip()]
    return sorted(ragged)


def missing_metabase_metadata(nodes: list[dict[str, Any]]) -> list[str]:
    """What the marts and their columns lack for Metabase: a display name everywhere, and the key
    semantic_type on every column, with a type of Metabase or null."""
    problems = []
    for node in models(nodes, (MARTS,)):
        if not str(metabase_meta(node).get("display_name") or "").strip():
            problems.append(f"{node['name']}: display_name")
        for column in node["columns"].values():
            declared, where = metabase_meta(column), f"{node['name']}.{column['name']}"
            if not str(declared.get("display_name") or "").strip():
                problems.append(f"{where}: display_name")
            if "semantic_type" not in declared:
                problems.append(f"{where}: semantic_type")
            elif declared["semantic_type"] is not None and not str(
                declared["semantic_type"]
            ).startswith("type/"):
                problems.append(f"{where}: semantic_type is not a type of Metabase")
    return problems


def different_descriptions(nodes: list[dict[str, Any]]) -> list[str]:
    """The column names that have more than one description across the models of the layers."""
    texts: dict[str, set[str]] = defaultdict(set)
    for node in models(nodes):
        for column in node["columns"].values():
            texts[column["name"]].add(column.get("description") or "")
    return sorted(name for name, found in texts.items() if len(found) > 1)


def currency_problems(nodes: list[dict[str, Any]]) -> list[str]:
    """The mart columns that are money in Metabase without one currency for the whole table: a
    currency type and a currency code go together, and an accepted_values test of the column
    currency of the model must allow that one code and no other."""
    tested = {}
    for node in nodes:
        test = node.get("test_metadata") or {}
        is_one_of = node["resource_type"] == "test" and test.get("name") == "accepted_values"
        if is_one_of and node.get("column_name") == "currency":
            tested[node["attached_node"]] = list((test.get("kwargs") or {}).get("values") or [])
    problems = []
    for node in models(nodes, (MARTS,)):
        for column in node["columns"].values():
            declared, where = metabase_meta(column), f"{node['name']}.{column['name']}"
            is_money = declared.get("semantic_type") in CURRENCY_TYPES
            code = declared.get("currency")
            if not is_money and code is None:
                continue
            if is_money != (code is not None):
                problems.append(f"{where}: a currency type and a currency code go together")
            elif tested.get(node["unique_id"]) != [code]:
                problems.append(f"{where}: no accepted_values test allows only {code}")
    return problems


def title_case(name: str) -> bool:
    """Whether a display name is in Title Case, with the abbreviations of the list in capitals."""
    for index, word in enumerate(name.split(" ")):
        if word.upper() in ABBREVIATIONS:
            if word != word.upper():
                return False
        elif index > 0 and word.lower() in SMALL_WORDS:
            if word != word.lower():
                return False
        elif not word[:1].isupper():
            return False
    return True


def style_problems(nodes: list[dict[str, Any]]) -> list[str]:
    """The display names of the marts that break the style, and the names that two columns of one
    table share, which people and AI clients could not tell apart."""
    problems = []
    for node in models(nodes, (MARTS,)):
        names = {node["name"]: metabase_meta(node).get("display_name")}
        names |= {
            f"{node['name']}.{c}": metabase_meta(d).get("display_name")
            for c, d in node["columns"].items()
        }
        problems += [
            f"{where}: {name!r}" for where, name in names.items() if name and not title_case(name)
        ]
        seen: dict[str, str] = {}
        for where, name in list(names.items())[1:]:
            if name and name.lower() in seen:
                problems.append(f"{seen[name.lower()]} and {where} share the display name {name!r}")
            elif name:
                seen[name.lower()] = where
    return problems


def contract_problems(nodes: list[dict[str, Any]]) -> list[str]:
    """The marts without an enforced contract, and their columns without a data type."""
    problems = []
    for node in models(nodes, (MARTS,)):
        if not ((node.get("config") or {}).get("contract") or {}).get("enforced"):
            problems.append(f"{node['name']}: the contract is not enforced")
        problems += [
            f"{node['name']}.{name}: data_type"
            for name, column in node["columns"].items()
            if not column.get("data_type")
        ]
    return problems


def test_every_model_column_and_source_table_has_a_description(nodes):
    assert not missing_descriptions(nodes), missing_descriptions(nodes)


def test_no_description_starts_or_ends_with_white_space(nodes):
    # A multi-line description is folded with >-, which leaves no line break at its end.
    assert not ragged_descriptions(nodes), ragged_descriptions(nodes)


def test_every_mart_and_its_columns_have_a_display_name_and_a_semantic_type(nodes):
    assert not missing_metabase_metadata(nodes), missing_metabase_metadata(nodes)


def test_columns_with_the_same_name_have_the_same_description(nodes):
    # A column of more than one model takes its description from the doc block of its name.
    assert not different_descriptions(nodes), different_descriptions(nodes)


def test_a_column_is_money_only_where_a_test_allows_one_currency(nodes):
    assert not currency_problems(nodes), currency_problems(nodes)


def test_the_display_names_follow_the_style(nodes):
    assert not style_problems(nodes), style_problems(nodes)


def test_every_mart_has_an_enforced_contract(nodes):
    assert not contract_problems(nodes), contract_problems(nodes)


def column(name: str, description: str = "A text.", **metabase: str) -> dict[str, Any]:
    """A column as dbt ls prints it, for the test of the checks."""
    return {
        "name": name,
        "description": description,
        "data_type": "text",
        "meta": {"metabase": metabase},
    }


def test_the_checks_find_what_they_look_for():
    # Each check above passes on an empty list, so each must find the problems planted here.
    mart = {
        "unique_id": "model.x.mart",
        "resource_type": "model",
        "schema": "marts",
        "name": "mart",
        "description": "A mart.\n",
        "config": {"contract": {"enforced": False}},
        "meta": {"metabase": {"display_name": "A Mart"}},
        "columns": {
            "spend": column("spend", display_name="Spend", semantic_type="type/Currency"),
            "cost": column("cost", "", display_name="Account Id", currency="EUR"),
            "clicks": {**column("clicks", "Other.", display_name="Spend"), "data_type": None},
            "budget": column(
                "budget", display_name="Budget", semantic_type="type/Cost", currency="USD"
            ),
        },
    }
    staging = {
        "unique_id": "model.x.staging",
        "resource_type": "model",
        "schema": "staging",
        "name": "staging",
        "description": "",
        "meta": {},
        "columns": {"clicks": column("clicks", "Clicks.")},
    }
    source = {
        "unique_id": "source.x.raw.table",
        "resource_type": "source",
        "schema": "raw",
        "name": "table",
        "description": "",
        "columns": {},
    }
    # Two currencies are allowed, so the table does not have one currency.
    test = {
        "unique_id": "test.x.accepted_values",
        "resource_type": "test",
        "attached_node": "model.x.mart",
        "column_name": "currency",
        "test_metadata": {"name": "accepted_values", "kwargs": {"values": ["USD", "EUR"]}},
    }
    planted = [mart, staging, source, test]
    assert missing_descriptions(planted) == ["mart.cost", "source.x.raw.table", "staging"]
    assert ragged_descriptions(planted) == ["mart"]
    assert missing_metabase_metadata(planted) == [
        "mart.cost: semantic_type",
        "mart.clicks: semantic_type",
    ]
    assert different_descriptions(planted) == ["clicks"]
    assert currency_problems(planted) == [
        "mart.spend: a currency type and a currency code go together",
        "mart.cost: a currency type and a currency code go together",
        "mart.budget: no accepted_values test allows only USD",
    ]
    assert style_problems(planted) == [
        "mart.cost: 'Account Id'",
        "mart.spend and mart.clicks share the display name 'Spend'",
    ]
    assert contract_problems(planted) == [
        "mart: the contract is not enforced",
        "mart.clicks: data_type",
    ]
    assert title_case("Ad Spend by Day")
    assert title_case("Cost per Click")
    assert not title_case("Cost Per Click")
    assert not title_case("ad spend")


@pytest.fixture(scope="module")
def fresh_build(airflow_url, token):
    """A run of the transform that succeeded now: dbt writes the comments when it builds a model,
    so the warehouse then holds those of the checkout. The mart exists before the run, so the run
    also rebuilds it by the swap of tables, with its primary key."""
    for dag_id in (LOAD, TRANSFORM):
        wait_for_dag(airflow_url, token, dag_id)
    wait_for(
        lambda: not any(is_busy(airflow_url, token, dag_id) for dag_id in (LOAD, TRANSFORM)),
        600,
        interval=5,
        message="the runs of the demo DAGs that earlier tests started did not end",
    )
    unpause(airflow_url, token, TRANSFORM)
    run = trigger(airflow_url, token, TRANSFORM)
    run = wait_for_run(airflow_url, token, TRANSFORM, run["dag_run_id"], timeout=600)
    assert run["state"] == "success", run["state"]
    return run


@needs_the_demo
def test_the_comments_of_the_warehouse_are_the_descriptions_of_dbt(env, nodes, fresh_build):
    stored = {
        (schema, relation, column): comment
        for schema, relation, column, comment in json.loads(warehouse_output(env, COMMENTS))
    }
    wanted = {}
    for node in models(nodes):
        wanted[(node["schema"], node["name"], None)] = node["description"]
        for name, column in node["columns"].items():
            wanted[(node["schema"], node["name"], name)] = column["description"]
    # Every relation of a model, and every column that it has, carries the comment of dbt.
    relations = {(schema, relation) for schema, relation, _ in wanted}
    physical = {key for key in stored if key[:2] in relations}
    assert set(wanted) <= set(stored), sorted(map(str, set(wanted) - set(stored)))
    differ = sorted(
        ".".join(filter(None, key)) for key in physical if stored[key] != wanted.get(key)
    )
    assert not differ, f"the comments of these differ from the descriptions of dbt: {differ}"


@needs_the_demo
def test_the_key_of_the_mart_is_a_primary_key_after_a_rebuild(env, fresh_build):
    definition = warehouse_output(env, PRIMARY_KEY)
    assert definition == "PRIMARY KEY (report_date, account_id, campaign_id)", definition
