"""The acceptance of the loads and the cleanup of the raw layer at the end of a run of the transform
(ADR-0035, ADR-0046), without Docker: the SQL of the acceptance, which rows of which tables the
cleanup removes, by load and account, how it empties a table under its lock, which tables it leaves
alone, and its constants against the staging models and the DAGs of the loads.
tests/stack/test_raw_cleanup.py runs both on a stack."""

import ast
import re
from datetime import timedelta
from pathlib import Path

import pytest
from agoranomos_pipelines import acceptance

REPO_ROOT = Path(__file__).resolve().parents[2]
DAGS = REPO_ROOT / "airflow" / "dags"
DBT = REPO_ROOT / "dbt"
STAGING = DBT / "models" / "staging"
MODULE = DAGS / "agoranomos_pipelines" / "acceptance.py"

# A call of newest_load or newest_list in a staging model: the relation of its raw table, through
# demo_source or relation_or_empty, the raw schema, and an account of its own for a list, if any.
READ = re.compile(
    r"(newest_load|newest_list)\((?:demo_source\('(raw_\w+)', '(\w+)'\)"
    r"|relation_or_empty\(source\('(raw_\w+)', '(\w+)'\), \{.*?\}\)), '(raw_\w+)', "
    r"(?:ref|source)\([^)]*\)(?:, ([\"'])(.*?)\7)?\)",
    flags=re.DOTALL,
)


def read_by_staging() -> dict[str, dict[str, str | None]]:
    """How each raw table that staging reads by load or by list names the account of a row, as the
    staging models say: the account of its source in newest_load_sources, or that of the call."""
    text = (DBT / "macros" / "newest_load.sql").read_text(encoding="utf-8")
    sources = {
        source: ast.literal_eval(f"({pair.replace('(none,', '(None,')})")[0]
        for source, pair in re.findall(r"^    '(raw_\w+)': (\(.*\)),$", text, flags=re.MULTILINE)
    }
    found: dict[str, dict[str, str | None]] = {}
    for path in sorted(STAGING.rglob("*.sql")):
        for call in READ.finditer(path.read_text(encoding="utf-8")):
            _, schema, table, other_schema, other_table, source, _, account = call.groups()
            schema, table = schema or other_schema, table or other_table
            assert schema == source, path.name
            named = account if account is not None else sources[schema]
            assert found.setdefault(schema, {}).setdefault(table, named) == named, path.name
    return found


def test_the_cleanup_names_the_account_of_each_raw_table_as_staging_reads_it():
    # A row goes by load and account, so the account of each table must be the one through which
    # staging finds its load: a wrong column would keep rows, or take those of another account.
    found = read_by_staging()
    assert len(found) == 7
    assert sum(len(tables) for tables in found.values()) == 40
    assert found == acceptance.ACCOUNTS
    assert acceptance.ACCOUNTS["raw_demo_search"]["customer_clients"] == "r.customer_client__id"
    assert acceptance.ACCOUNTS["raw_ecb"] == {"rates_daily": None}


def dag_timeouts() -> dict[str, timedelta]:
    found = {}
    for path in sorted(DAGS.glob("*.py")):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.keyword) and node.arg == "dagrun_timeout":
                call = node.value
                assert isinstance(call, ast.Call)
                assert ast.unparse(call.func) == "timedelta"
                found[path.stem] = timedelta(
                    **{k.arg: ast.literal_eval(k.value) for k in call.keywords}
                )
    return found


def test_a_load_that_did_not_end_waits_longer_than_any_run_of_a_dag_lives():
    # ADR-0046: the cut of the loads that did not end is no shorter than the dagrun_timeout of a
    # load, seven hours for Google Ads; a load id is the moment when dlt began to extract the load.
    timeouts = dag_timeouts()
    assert timeouts["agoranomos_google_ads_daily"] == timedelta(hours=7)
    for name in (
        "agoranomos_demo_history",
        "agoranomos_demo_daily",
        "agoranomos_ecb_daily",
        "agoranomos_google_ads_daily",
    ):
        assert name in timeouts, name
    assert max(timeouts.values()) < acceptance.UNFINISHED_AFTER
    assert timedelta(days=1) == acceptance.UNFINISHED_AFTER


def test_the_acceptance_takes_what_the_run_read_and_did_not_hold():
    # ADR-0046: what this run or an earlier one pinned, no later than the newest pin that the run
    # read, by load and account, and none of an account that the checks of the run held, a stopped
    # source among them.
    sql = " ".join(acceptance.ACCEPT.split())
    assert acceptance.NEWEST_DAYS == "staging.stg_loads__newest_days"
    assert acceptance.FAILURES == "staging.int_g1_failures"
    assert (
        "WITH read AS ( SELECT max(l.pinned_at) AS pinned_at FROM core.loads AS l JOIN "
        "staging.stg_loads__newest_days AS d ON d.source = l.source AND d.load_id = l.load_id "
        "AND d.account_id = l.account_id )"
    ) in sql
    assert "UPDATE core.loads AS l SET status = 'accepted', accepted_at = now() FROM read" in sql
    assert "WHERE l.status = 'pinned' AND l.pinned_at <= read.pinned_at" in sql
    assert (
        "AND NOT EXISTS ( SELECT FROM staging.int_g1_failures AS q "
        "WHERE q.source = l.source AND q.account_id = l.account_id )"
    ) in sql
    assert (STAGING / "loads" / "stg_loads__newest_days.sql").exists()
    assert (DBT / "models" / "intermediate" / "int_g1_failures.sql").exists()
    project = (DBT / "dbt_project.yml").read_text(encoding="utf-8")
    assert "    intermediate:\n" in project
    assert "      +schema: staging\n" in project


def test_the_days_of_the_accepted_loads_leave_the_days_that_the_facts_replace():
    # After the cleanup raw no longer holds the rows of an accepted load, so a model of a fact
    # built by hand from the days of the run would empty them: the acceptance takes those days out
    # of int_window_days in its transaction, and the model of the days joins the pinned loads only,
    # so that a build of it by hand gives none of them either.
    assert acceptance.WINDOW_DAYS == "staging.int_window_days"
    assert " ".join(acceptance.FORGET_DAYS.split()) == (
        "DELETE FROM staging.int_window_days AS w USING core.loads AS l "
        "WHERE l.load_sk = w.load_sk AND l.status = 'accepted'"
    )
    function = MODULE.read_text(encoding="utf-8")
    function = function[function.index("def accept(") : function.index("def quoted(")]
    assert function.index("cursor.execute(ACCEPT)") < function.index("cursor.execute(FORGET_DAYS)")
    assert function.count("connection.cursor() as cursor") == 1
    model = (DBT / "models" / "intermediate" / "int_window_days.sql").read_text(encoding="utf-8")
    assert (
        "join {{ source('core', 'loads') }} as l\n"
        "    on l.source = d.source and l.load_id = d.load_id and l.account_id = d.account_id\n"
        "    and l.status = 'pinned'\n"
    ) in model


def test_a_row_goes_by_its_load_and_its_account():
    condition = acceptance.removable("raw_demo_search", "r.customer__id")
    assert condition.startswith("(((r._dlt_load_id, (r.customer__id)::text) IN (SELECT load_id, ")
    assert (
        "account_id FROM core.loads WHERE source = %(source)s AND status = 'accepted')"
    ) in condition
    # A row of an account that its load does not name, as the manager in the list of the accounts
    # of Google Ads, goes with the whole load.
    assert (
        " OR ((r.customer__id) IS NULL OR (r._dlt_load_id, (r.customer__id)::text) NOT IN (SELECT "
        "load_id, account_id FROM core.loads WHERE source = %(source)s)) AND r._dlt_load_id IN "
        "(SELECT load_id FROM core.loads WHERE source = %(source)s GROUP BY load_id HAVING "
        "bool_and(status = 'accepted'))) OR ("
    ) in condition
    # The rows of a load that did not end: no row in _dlt_loads, none in core.loads, and a load id
    # older than the cut; a load id that is no number stays.
    assert 'r._dlt_load_id NOT IN (SELECT load_id FROM "raw_demo_search"._dlt_loads' in condition
    assert "r._dlt_load_id NOT IN (SELECT load_id FROM core.loads WHERE source = %(source)s)" in (
        condition
    )
    assert r"CASE WHEN r._dlt_load_id ~ '^[0-9]+(\.[0-9]+)?$'" in condition
    assert "to_timestamp(r._dlt_load_id::double precision) END < now() - %(cut)s::interval" in (
        condition
    )


def test_a_table_without_an_account_loses_only_whole_loads():
    # The rates, and a table that staging does not read: only once every account of the load is
    # accepted.
    condition = acceptance.removable("raw_ecb", None)
    assert condition.startswith(
        "((r._dlt_load_id IN (SELECT load_id FROM core.loads WHERE source = %(source)s "
        "GROUP BY load_id HAVING bool_and(status = 'accepted'))) OR ("
    )


def test_the_child_tables_go_with_the_rows_of_their_table():
    roots = ["insights_ad", "insights_adset", "insights_campaign", "ads"]
    nested = [
        "insights_ad__actions",
        "insights_ad__action_values",
        "insights_adset__actions",
        "insights_campaign__actions",
        "insights_campaign__actions__windows",
        "lonely__values",
    ]
    assert acceptance.children_of("insights_ad", roots, nested) == [
        "insights_ad__action_values",
        "insights_ad__actions",
    ]
    assert acceptance.children_of("insights_adset", roots, nested) == ["insights_adset__actions"]
    assert acceptance.children_of("ads", roots, nested) == []
    # A child of a child joins no row of the root: it stays.
    assert acceptance.children_of("insights_campaign", roots, nested) == [
        "insights_campaign__actions"
    ]


@pytest.mark.parametrize("name", ['a"; DROP TABLE x; --', "Campaigns", "raw-demo", ""])
def test_the_cleanup_takes_only_the_names_that_dlt_gives(name):
    with pytest.raises(ValueError, match="dlt"):
        acceptance.quoted(name)


class Cursor:
    """A cursor that records each statement and answers the queries of the cleanup in turn."""

    def __init__(self, answers):
        self.answers = list(answers)
        self.statements = []
        self.rowcount = 0

    def execute(self, sql, parameters=None):
        self.statements.append(" ".join(sql.split()))

    def fetchone(self):
        return self.answers.pop(0)

    def fetchall(self):
        return self.answers.pop(0)


def test_the_journal_and_the_tables_of_dlt_stay():
    cursor = Cursor(
        [
            [
                ("_agoranomos_loads", True, False),
                ("_dlt_loads", False, False),
                ("_dlt_pipeline_state", True, False),
                ("campaigns", True, False),
                ("insights_ad", True, False),
                ("insights_ad__actions", False, True),
            ]
        ]
    )
    assert acceptance.tables_of(cursor, "raw_demo_social") == (
        ["campaigns", "insights_ad"],
        ["insights_ad__actions"],
    )
    assert acceptance.KEPT_PREFIXES == ("_dlt_", "_agoranomos_")


def test_truncate_runs_under_the_lock_in_the_transaction_of_its_check():
    # #188: a check apart from the TRUNCATE let a load in progress write between the two, and the
    # TRUNCATE took its rows. Under ACCESS EXCLUSIVE, the check sees every row that a load wrote.
    cursor = Cursor([(5, 0), (8, 0)])
    gone = acceptance.truncate_if_all_go(
        cursor, "raw_demo_social", "insights_ad", ["insights_ad__actions"], "(true)", {}
    )
    assert gone == 13
    assert cursor.statements[0] == "SET LOCAL lock_timeout = '10s'"
    assert cursor.statements[1] == (
        'LOCK TABLE "raw_demo_social"."insights_ad", "raw_demo_social"."insights_ad__actions" '
        "IN ACCESS EXCLUSIVE MODE"
    )
    assert cursor.statements[2].startswith(
        "SELECT count(*), count(*) FILTER (WHERE NOT coalesce((true), false))"
    )
    assert cursor.statements[-1] == (
        'TRUNCATE "raw_demo_social"."insights_ad", "raw_demo_social"."insights_ad__actions"'
    )


@pytest.mark.parametrize(
    "answers",
    [
        [(5, 1)],  # a row stays, of a load in progress or in quarantine
        [(0, 0)],  # nothing to empty
        [(5, 0), (8, 2)],  # a child row has no row of its table yet
    ],
)
def test_no_truncate_while_a_row_stays(answers):
    cursor = Cursor(answers)
    assert (
        acceptance.truncate_if_all_go(
            cursor, "raw_demo_social", "insights_ad", ["insights_ad__actions"], "(true)", {}
        )
        is None
    )
    assert not [s for s in cursor.statements if s.startswith("TRUNCATE")]


def test_the_delete_takes_the_child_rows_first():
    cursor = Cursor([])
    acceptance.delete_what_goes(
        cursor, "raw_demo_social", "insights_ad", ["insights_ad__actions"], "(cond)", {}
    )
    assert cursor.statements == [
        'DELETE FROM "raw_demo_social"."insights_ad__actions" AS c USING '
        '"raw_demo_social"."insights_ad" AS r WHERE c._dlt_parent_id = r._dlt_id AND (cond)',
        'DELETE FROM "raw_demo_social"."insights_ad" AS r WHERE (cond)',
    ]


def test_the_acceptance_connects_as_transformer_and_the_cleanup_as_loader():
    tree = ast.parse(MODULE.read_text(encoding="utf-8"))
    functions = {
        node.name: ast.unparse(node) for node in tree.body if isinstance(node, ast.FunctionDef)
    }
    assert "transformer_credentials()" in functions["accept"]
    assert "loader_credentials()" not in functions["accept"]
    assert "loader_credentials()" in functions["clean_raw"]
    assert "transformer_credentials()" not in functions["clean_raw"]
    # The cleanup writes nothing but the deletes of the rows of raw.
    text = MODULE.read_text(encoding="utf-8")
    for statement in ("DROP ", "CREATE ", "ALTER ", "GRANT ", "INSERT ", "_agoranomos_loads WHERE"):
        assert statement not in text, statement
