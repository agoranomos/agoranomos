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
# Every check below takes the layers from here, the check of the comments too.
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
# The comments of the tables and views of the warehouse outside the catalogs of PostgreSQL, and of
# their columns, as one JSON document: [schema, relation, column or null, comment or null]. A
# comment may hold any character. comment_problems keeps the relations that dbt builds in LAYERS.
COMMENTS = """
SELECT coalesce(json_agg(item), '[]') FROM (
  SELECT json_build_array(n.nspname, c.relname, NULL, obj_description(c.oid, 'pg_class')) AS item
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname NOT IN ('pg_catalog', 'information_schema') AND c.relkind IN ('r', 'v')
  UNION ALL
  SELECT json_build_array(n.nspname, c.relname, a.attname, col_description(c.oid, a.attnum))
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
  WHERE n.nspname NOT IN ('pg_catalog', 'information_schema') AND c.relkind IN ('r', 'v')
) AS comments
"""
PRIMARY_KEY = (
    "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
    "WHERE conrelid = 'marts.ad_stats_campaign_daily'::regclass AND contype = 'p'"
)
# The one expression of the reporting currency in the YAML (docs/reference/describe-the-marts.md),
# which dbt keeps unrendered in the arguments of a test.
REPORTING = "{{ env_var('AGORANOMOS_REPORTING_CURRENCY', 'USD') }}"

needs_the_demo = pytest.mark.skipif(
    not DEMO, reason="the demo is off: COMPOSE_PROFILES in .env does not list demo"
)


def run_dbt_ls() -> list[dict[str, Any]]:
    """The models, seeds, sources and tests of the project, as dbt ls prints them: one object a
    line."""
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
        "seed",
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
    # Not vacuous: the checks below must see a mart, a staging model, a source and a seed.
    expected = {
        "model.agoranomos.ad_stats_campaign_daily",
        "model.agoranomos.stg_demo_search__campaign_stats",
        "source.agoranomos.raw_demo_search.campaign_stats",
        "seed.agoranomos.seed_ad_platforms",
    }
    assert expected <= ids, f"dbt ls does not list {sorted(expected - ids)}"
    return found


def models(nodes: list[dict[str, Any]], layers: tuple[str, ...] = LAYERS) -> list[dict[str, Any]]:
    return [n for n in nodes if n["resource_type"] == "model" and n["schema"] in layers]


def described(nodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The relations that dbt builds and describes, with every column: the models of the layers,
    and the seeds, which persist_docs comments too."""
    return [*models(nodes), *(n for n in nodes if n["resource_type"] == "seed")]


def metabase_meta(item: dict[str, Any]) -> dict[str, Any]:
    """What a model or a column declares for Metabase, under config: meta: metabase:."""
    return (item.get("meta") or {}).get("metabase") or {}


def missing_descriptions(nodes: list[dict[str, Any]]) -> list[str]:
    """The models and columns of the layers, the seeds and their columns, and the tables of the
    sources, with no description."""
    missing = []
    for node in described(nodes):
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
    for node in [*described(nodes), *(n for n in nodes if n["resource_type"] == "source")]:
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


def currency_column(column: str) -> str:
    """The column of the currency of a money column: reporting_currency for a column that ends in
    _reporting, currency for any other (docs/reference/describe-the-marts.md)."""
    return "reporting_currency" if column.endswith("_reporting") else "currency"


def currency_problems(nodes: list[dict[str, Any]], reporting_currency: str = "USD") -> list[str]:
    """The mart columns that are money in Metabase without one currency for the whole table: a
    currency type and a currency code go together, an accepted_values test of the column of its
    currency must allow that one code and no other, and a not_null test of that column must go with
    it, as accepted_values lets NULL through. The expression of the reporting currency in the
    values of a test counts as reporting_currency, the setting that dbt renders it to."""
    allowed, not_null = {}, set()
    for node in nodes:
        test = node.get("test_metadata") or {}
        column = node.get("column_name")
        if node["resource_type"] != "test" or column not in ("currency", "reporting_currency"):
            continue
        key = (node["attached_node"], column)
        if test.get("name") == "accepted_values":
            values = list((test.get("kwargs") or {}).get("values") or [])
            allowed[key] = [reporting_currency if value == REPORTING else value for value in values]
        elif test.get("name") == "not_null":
            not_null.add(key)
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
                continue
            key = (node["unique_id"], currency_column(column["name"]))
            if allowed.get(key) != [code]:
                problems.append(f"{where}: no accepted_values test of {key[1]} allows only {code}")
            if key not in not_null:
                problems.append(f"{where}: no not_null test keeps {key[1]} from being NULL")
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


def wanted_comments(nodes: list[dict[str, Any]]) -> dict[tuple[str, str, str | None], str]:
    """The comment that each relation of a model of the layers or of a seed, and each column that
    dbt declares for it, should carry: its description, by (schema, relation, column or None)."""
    wanted: dict[tuple[str, str, str | None], str] = {}
    for node in described(nodes):
        wanted[(node["schema"], node["name"], None)] = node["description"]
        for name, column in node["columns"].items():
            wanted[(node["schema"], node["name"], name)] = column["description"]
    return wanted


def dotted(key: tuple[str, str, str | None]) -> str:
    """schema.relation, or schema.relation.column."""
    return ".".join(part for part in key if part)


def comment_problems(
    stored: dict[tuple[str, str, str | None], str | None],
    wanted: dict[tuple[str, str, str | None], str],
) -> list[str]:
    """What the warehouse holds against what dbt declares, for each relation that dbt builds in the
    layers: a relation or a declared column that the warehouse lacks, a column of the warehouse
    that dbt does not declare, so that it has no description, and a comment that is empty or is not
    the description. A relation that no model builds is not dbt's to describe."""
    relations = {key[:2] for key in wanted}
    physical = {key for key in stored if key[:2] in relations}
    lacking, undeclared = wanted.keys() - physical, physical - wanted.keys()
    problems = [f"{dotted(key)}: the warehouse does not have it" for key in lacking]
    problems += [f"{dotted(key)}: dbt does not declare it" for key in undeclared]
    for key in physical & wanted.keys():
        if not (stored[key] or "").strip():
            problems.append(f"{dotted(key)}: no comment")
        elif stored[key] != wanted[key]:
            problems.append(f"{dotted(key)}: the comment is not the description")
    return sorted(problems)


def test_every_model_column_and_source_table_has_a_description(nodes):
    problems = missing_descriptions(nodes)
    assert not problems, problems


def test_no_description_starts_or_ends_with_white_space(nodes):
    # A multi-line description is folded with >-, which leaves no line break at its end.
    problems = ragged_descriptions(nodes)
    assert not problems, problems


def test_every_mart_and_its_columns_have_a_display_name_and_a_semantic_type(nodes):
    problems = missing_metabase_metadata(nodes)
    assert not problems, problems


def test_columns_with_the_same_name_have_the_same_description(nodes):
    # A column of more than one model takes its description from the doc block of its name.
    problems = different_descriptions(nodes)
    assert not problems, problems


def test_a_column_is_money_only_where_a_test_allows_one_currency(env, nodes):
    problems = currency_problems(nodes, env.get("AGORANOMOS_REPORTING_CURRENCY") or "USD")
    assert not problems, problems


def test_the_display_names_follow_the_style(nodes):
    problems = style_problems(nodes)
    assert not problems, problems


def test_every_mart_has_an_enforced_contract(nodes):
    problems = contract_problems(nodes)
    assert not problems, problems


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
        # No display name for the table.
        "meta": {},
        "columns": {
            "spend": column("spend", display_name="Spend", semantic_type="type/Currency"),
            "cost": column("cost", "", display_name="Account Id", currency="EUR"),
            "clicks": {**column("clicks", "Other.", display_name="Spend"), "data_type": None},
            "budget": column(
                "budget", display_name="Budget", semantic_type="type/Cost", currency="USD"
            ),
            # No display name, and a semantic type that is not one of Metabase.
            "reach": column("reach", semantic_type="Quantity"),
            # Money in the reporting currency, whose column has one code, as the setting renders it.
            "cost_reporting": column(
                "cost_reporting",
                display_name="Cost in Reporting Currency",
                semantic_type="type/Currency",
                currency="EUR",
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
    # A seed without a description, with a column without one.
    seed = {
        "unique_id": "seed.x.seed_list",
        "resource_type": "seed",
        "schema": "staging",
        "name": "seed_list",
        "description": "",
        "columns": {"code": column("code", "")},
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
    # A not_null test of another column, so currency can still be NULL.
    not_null = {
        "unique_id": "test.x.not_null",
        "resource_type": "test",
        "attached_node": "model.x.mart",
        "column_name": "spend",
        "test_metadata": {"name": "not_null", "kwargs": {}},
    }
    # reporting_currency allows the one expression of the setting, and is never NULL.
    reporting = [
        {
            "unique_id": f"test.x.{name}",
            "resource_type": "test",
            "attached_node": "model.x.mart",
            "column_name": "reporting_currency",
            "test_metadata": {"name": name, "kwargs": kwargs},
        }
        for name, kwargs in (("accepted_values", {"values": [REPORTING]}), ("not_null", {}))
    ]
    planted = [mart, staging, seed, source, test, not_null, *reporting]
    assert missing_descriptions(planted) == [
        "mart.cost",
        "seed_list",
        "seed_list.code",
        "source.x.raw.table",
        "staging",
    ]
    assert ragged_descriptions(planted) == ["mart"]
    assert missing_metabase_metadata(planted) == [
        "mart: display_name",
        "mart.cost: semantic_type",
        "mart.clicks: semantic_type",
        "mart.reach: display_name",
        "mart.reach: semantic_type is not a type of Metabase",
    ]
    assert different_descriptions(planted) == ["clicks"]
    found = [
        "mart.spend: a currency type and a currency code go together",
        "mart.cost: a currency type and a currency code go together",
        "mart.budget: no accepted_values test of currency allows only USD",
        "mart.budget: no not_null test keeps currency from being NULL",
    ]
    # With the setting EUR, the reporting currency of the test is the code of cost_reporting.
    assert currency_problems(planted, reporting_currency="EUR") == found
    assert currency_problems(planted, reporting_currency="USD") == [
        *found,
        "mart.cost_reporting: no accepted_values test of reporting_currency allows only EUR",
    ]
    assert style_problems(planted) == [
        "mart.cost: 'Account Id'",
        "mart.spend and mart.clicks share the display name 'Spend'",
    ]
    assert contract_problems(planted) == [
        "mart: the contract is not enforced",
        "mart.clicks: data_type",
    ]
    # The warehouse has a column that dbt does not declare, lacks a declared column and a relation,
    # and holds a missing and a stale comment; a relation that no model builds is left alone.
    stored = {
        ("marts", "mart", None): "A mart.\n",
        ("marts", "mart", "spend"): "A text.",
        ("marts", "mart", "cost"): None,
        ("marts", "mart", "clicks"): "An older text.",
        ("marts", "mart", "reach"): "A text.",
        ("marts", "mart", "cost_reporting"): "A text.",
        ("marts", "mart", "extra"): None,
        ("marts", "by_hand", None): None,
    }
    assert comment_problems(stored, wanted_comments(planted)) == [
        "marts.mart.budget: the warehouse does not have it",
        "marts.mart.clicks: the comment is not the description",
        "marts.mart.cost: no comment",
        "marts.mart.extra: dbt does not declare it",
        "staging.seed_list.code: the warehouse does not have it",
        "staging.seed_list: the warehouse does not have it",
        "staging.staging.clicks: the warehouse does not have it",
        "staging.staging: the warehouse does not have it",
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
    # Every relation of a model has exactly the columns that dbt declares, and each of them, like
    # the relation, carries its description as its comment.
    stored = {
        (schema, relation, column): comment
        for schema, relation, column, comment in json.loads(warehouse_output(env, COMMENTS))
    }
    problems = comment_problems(stored, wanted_comments(nodes))
    assert not problems, problems


@needs_the_demo
def test_the_key_of_the_mart_is_a_primary_key_after_a_rebuild(env, fresh_build):
    definition = warehouse_output(env, PRIMARY_KEY)
    expected = "PRIMARY KEY (report_date, platform, account_id, campaign_id)"
    assert definition == expected, definition
