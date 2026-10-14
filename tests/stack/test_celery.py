"""What only the Celery variant has: the broker and the worker (skipped in the local variant)."""

import json

import pytest
from services import DEMO
from stack_helpers import (
    active_variant,
    compose,
    job_failed,
    process_uids,
    psql,
    psql_over_network,
    rendered,
)

pytestmark = pytest.mark.skipif(
    active_variant() != "celery",
    reason="the broker and the worker need COMPOSE_FILE=compose.yaml:compose.celery.yaml",
)


def valkey_cli(env, *args, password=True):
    """Run valkey-cli in the broker. The password goes in the environment of the process, which
    -e VALKEYCLI_AUTH hands on to the container: never in an argument, where ps could show it."""
    options = ["-e", "VALKEYCLI_AUTH"] if password else []
    return compose(
        "exec",
        "-T",
        *options,
        "broker",
        "valkey-cli",
        *args,
        check=False,
        env={"VALKEYCLI_AUTH": env["VALKEY_PASSWORD"]},
    )


def test_the_broker_asks_for_the_password(env):
    result = valkey_cli(env, "ping", password=False)
    assert "NOAUTH" in result.stdout + result.stderr
    result = valkey_cli(env, "ping")
    assert result.stdout.strip() == "PONG", result.stderr


def test_the_broker_keeps_nothing_on_disk(env):
    result = valkey_cli(env, "CONFIG", "GET", "save")
    assert result.returncode == 0, result.stderr
    assert result.stdout.split("\n")[:2] == ["save", ""], result.stdout
    result = valkey_cli(env, "CONFIG", "GET", "appendonly")
    assert result.stdout.split() == ["appendonly", "no"], result.stdout


def test_the_broker_server_does_not_run_as_root():
    # The image drops privileges only when its command is valkey-server, and the command here is a
    # shell, so compose.celery.yaml sets the user. The image has no ps: the helper reads the
    # process files. It always succeeds, so a server that is not there is an empty answer, and
    # fails here.
    result = compose("exec", "-T", "broker", "sh", "-c", process_uids("valkey-server"))
    assert result.stdout.split() == ["Uid:", "999", "999", "999", "999"], result.stdout


def test_the_worker_does_not_run_as_root():
    result = compose("exec", "-T", "airflow-worker", "id", "-u")
    assert result.stdout.strip() == "50000"


def worker_check(*options, python=("python", "-c")):
    """Run the health check of compose.celery.yaml in the worker, as Docker runs it: options go to
    docker compose exec, and python is the command that runs the code of the check."""
    code = rendered("celery")["airflow-worker"]["healthcheck"]["test"][3]
    return compose("exec", "-T", *options, "airflow-worker", *python, code, check=False)


def test_the_check_of_the_worker_passes_for_its_own_node_only():
    # The worker of the container answers the ping for its own node name. For another host name
    # no node answers within the 5 seconds of the check, and it fails.
    assert worker_check().returncode == 0
    assert worker_check("-e", "HOSTNAME=another-container").returncode == 1


# Runs the code of the check, its argument, and then prints the names of the modules that the
# process has imported, as JSON, and exits as the check did. -X importtime would not do: it leaves
# out a module that importlib.import_module loads, as kombu loads the transport of the broker.
AFTER_THE_CHECK = """
import json
import sys

try:
    exec(sys.argv[1], {"__name__": "__main__"})
finally:
    print(json.dumps(sorted(sys.modules)))
"""


def test_the_check_of_the_worker_starts_no_airflow():
    # The check imports Celery and the transport of the broker, and nothing of Airflow, which the
    # old check started at every run.
    result = worker_check(python=("python", "-c", AFTER_THE_CHECK))
    assert result.returncode == 0
    imported = set(json.loads(result.stdout))
    assert {"celery", "kombu.transport.redis"} <= imported
    assert not sorted(name for name in imported if name.split(".")[0] == "airflow")


def test_the_worker_holds_neither_the_metadata_database_nor_the_signing_key():
    # The names of the variables in the running worker, never their values: the security model of
    # Airflow 3.3.2 keeps both from workers, and compose.celery.yaml takes them away.
    names = compose("exec", "-T", "airflow-worker", "sh", "-c", "env | cut -d= -f1").stdout.split()
    assert "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN" not in names
    assert "AIRFLOW__API_AUTH__JWT_SECRET" not in names
    assert "AIRFLOW__CELERY__RESULT_BACKEND" in names


@pytest.mark.parametrize("database", ["airflow", "metabase"])
def test_the_role_of_the_results_cannot_reach_the_other_databases(env, database):
    # The worker holds the password of celery_results: with it, the code of a task reaches the
    # results of Celery, and neither the metadata database of Airflow nor that of Metabase.
    denied = psql_over_network(
        "platform-db", "celery_results", env["CELERY_RESULTS_DB_PASSWORD"], database, "SELECT 1"
    )
    assert denied.returncode != 0
    assert f'permission denied for database "{database}"' in denied.stderr


def test_the_worker_writes_the_results_into_their_own_database(env):
    # The worker creates the tables of the results when it starts. With the demo, demo-init has run
    # the demo pipeline on the worker at the first start, so there are results.
    own = psql_over_network(
        "platform-db",
        "celery_results",
        env["CELERY_RESULTS_DB_PASSWORD"],
        "celery_results",
        "SELECT count(*) FROM celery_taskmeta",
    )
    assert own.returncode == 0, own.stderr
    if DEMO:
        assert int(own.stdout.strip()) > 0, "no result of a task in celery_results"


# A trigger that the role of the results puts on its own table, as the code of a task on the worker
# could: it records the role that runs each deletion. And what puts the database back.
WATCH_THE_DELETIONS = (
    "CREATE TABLE agoranomos_test_deleted_by (name text); "
    "CREATE FUNCTION agoranomos_test_record_deleter() RETURNS trigger LANGUAGE plpgsql AS "
    "$$ BEGIN INSERT INTO agoranomos_test_deleted_by VALUES (current_user); RETURN OLD; END $$; "
    "CREATE TRIGGER agoranomos_test_record_deleter BEFORE DELETE ON celery_taskmeta "
    "FOR EACH ROW EXECUTE FUNCTION agoranomos_test_record_deleter()"
)
PUT_BACK = (
    "DROP TRIGGER IF EXISTS agoranomos_test_record_deleter ON celery_taskmeta; "
    "DROP FUNCTION IF EXISTS agoranomos_test_record_deleter(); "
    "DROP TABLE IF EXISTS agoranomos_test_deleted_by; "
    "DELETE FROM celery_taskmeta WHERE task_id LIKE 'agoranomos-test-%'"
)


def test_db_init_deletes_the_old_results_as_their_role_and_never_as_the_superuser(env):
    # Nothing in Airflow cleans the database of the results, so db-init, which runs on every start,
    # deletes those older than 90 days. Two results of no task, as the worker writes them, one 91
    # and one 89 days old: db-init run again deletes the first only. Whoever holds the password of
    # celery_results decides what a deletion in its database runs, so db-init deletes as that role:
    # the trigger, which runs with the powers of the role that deletes, sees celery_results.
    def results(sql):
        return psql(
            "platform-db",
            "celery_results",
            env["CELERY_RESULTS_DB_PASSWORD"],
            "celery_results",
            sql,
        )

    results(PUT_BACK)
    watched = results(WATCH_THE_DELETIONS)
    prepared = results(
        "INSERT INTO celery_taskmeta (id, task_id, status, date_done) VALUES "
        "(nextval('task_id_sequence'), 'agoranomos-test-91-days', 'SUCCESS', "
        "now() - interval '91 days'), "
        "(nextval('task_id_sequence'), 'agoranomos-test-89-days', 'SUCCESS', "
        "now() - interval '89 days')"
    )
    try:
        assert watched.returncode == 0, watched.stderr
        assert prepared.returncode == 0, prepared.stderr
        rerun = compose("run", "--rm", "--no-deps", "db-init", check=False)
        assert rerun.returncode == 0, job_failed("db-init", rerun)
        cleaned = "deleting them failed" not in rerun.stderr
        assert cleaned, "db-init says that it could not delete the old results"
        left = results(
            "SELECT task_id FROM celery_taskmeta WHERE task_id LIKE 'agoranomos-test-%' ORDER BY 1"
        )
        assert left.returncode == 0, left.stderr
        assert left.stdout.split() == ["agoranomos-test-89-days"]
        deleted_by = results("SELECT name FROM agoranomos_test_deleted_by")
        assert deleted_by.returncode == 0, deleted_by.stderr
        assert deleted_by.stdout.split() == ["celery_results"]
    finally:
        results(PUT_BACK)
