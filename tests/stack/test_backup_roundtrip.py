"""The round trip of a backup (ADR-0020): data, a backup, two restores, the same data.

The server job of the Stack workflow runs it in three steps. AGORANOMOS_ROUNDTRIP=before leaves a
mark in each database and takes a backup. AGORANOMOS_ROUNDTRIP=live adds to Metabase and to the
warehouse, restores Metabase alone into the running stack, the way back from an upgrade, and
finds Metabase as the backup has it and the warehouse as it was. AGORANOMOS_ROUNDTRIP=after, once
./agoranomos restore has put the backup on empty volumes, finds each mark again and signs in to
Metabase. AGORANOMOS_ROUNDTRIP_FILE names the file in which the first step leaves the marks and
the name of the backup for the others. Without these variables the tests skip: a round trip
replaces databases and removes the volumes of the stack, which no other run of the tests may do.
The marks are random words, not secrets.
"""

import json
import os
import secrets as random_words
import subprocess
from pathlib import Path

import pytest
from airflow_api import api
from services import DEMO
from stack_helpers import (
    REPO_ROOT,
    Secret,
    SecretHeaders,
    http_json,
    read_env,
    totals,
    warehouse_output,
)

# The daily load of the demo stays paused while these tests run (conftest.py).
pytestmark = pytest.mark.usefixtures("daily_load_paused")

STEP = os.environ.get("AGORANOMOS_ROUNDTRIP", "")
MARKS = Path(os.environ.get("AGORANOMOS_ROUNDTRIP_FILE", "/nonexistent"))
VARIABLE = "agoranomos_roundtrip"
# The env fixture says what is missing when there is no .env: the module still collects without it.
ENV_FILE = REPO_ROOT / ".env"
BACKUPS = REPO_ROOT / (
    (read_env(ENV_FILE).get("AGORANOMOS_BACKUP_DIR") if ENV_FILE.exists() else None) or "./backups"
)


def collection_names(metabase_url, session) -> set[str]:
    status, collections = http_json("GET", f"{metabase_url}/api/collection", headers=session)
    assert status == 200
    return {item["name"] for item in collections}


@pytest.mark.skipif(STEP != "before", reason="only in the first step of the round trip")
def test_before_the_backup_leave_a_mark_in_each_database(
    env, airflow_url, token, metabase_url, session
):
    mark = random_words.token_hex(8)
    create = "CREATE TABLE public.agoranomos_roundtrip (mark text)"
    insert = f"INSERT INTO public.agoranomos_roundtrip VALUES ('{mark}')"  # noqa: S608 (hexadecimal)
    warehouse_output(env, f"{create}; {insert}")
    status, _ = api(
        airflow_url, token, "POST", "/api/v2/variables", {"key": VARIABLE, "value": mark}
    )
    assert status in (200, 201)
    status, _ = http_json(
        "POST", f"{metabase_url}/api/collection", {"name": f"Round trip {mark}"}, headers=session
    )
    assert status == 200
    result = subprocess.run(
        [str(REPO_ROOT / "agoranomos"), "backup"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=900,
        check=False,
    )
    assert result.returncode == 0, result.stderr[-2000:]
    backup = json.loads((BACKUPS / "latest.json").read_text(encoding="utf-8"))["last_ok"]
    mart = (
        {key: str(value) for key, value in totals(env, "marts.ad_stats_campaign_daily").items()}
        if DEMO
        else {}
    )
    MARKS.write_text(json.dumps({"mark": mark, "backup": backup, "mart": mart}), encoding="utf-8")


@pytest.mark.skipif(STEP != "live", reason="only in the restore into the running stack")
def test_a_restore_of_metabase_into_the_running_stack_goes_back_to_the_backup(
    env, metabase_url, session
):
    saved = json.loads(MARKS.read_text(encoding="utf-8"))
    later = f"after-{saved['mark']}"
    status, _ = http_json(
        "POST", f"{metabase_url}/api/collection", {"name": f"Round trip {later}"}, headers=session
    )
    assert status == 200
    insert = f"INSERT INTO public.agoranomos_roundtrip VALUES ('{later}')"  # noqa: S608 (hexadecimal)
    warehouse_output(env, insert)
    result = subprocess.run(
        [str(REPO_ROOT / "agoranomos"), "restore", saved["backup"], "--only", "metabase", "--yes"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=1800,
        check=False,
    )
    assert result.returncode == 0, result.stderr[-2000:]
    # The sessions of Metabase are in its database: the one of this run came after the backup.
    status, body = http_json(
        "POST",
        f"{metabase_url}/api/session",
        {
            "username": env.get("AGORANOMOS_ADMIN_EMAIL") or "admin@example.com",
            "password": env["METABASE_ADMIN_PASSWORD"],
        },
    )
    assert status == 200, f"the admin could not sign in after the restore: HTTP {status}"
    again = SecretHeaders({"X-Metabase-Session": Secret(body["id"])})
    names = collection_names(metabase_url, again)
    assert f"Round trip {saved['mark']}" in names
    assert f"Round trip {later}" not in names
    # --only metabase leaves the warehouse as it was, with the row of after the backup.
    rows = warehouse_output(env, "SELECT mark FROM public.agoranomos_roundtrip").split()
    assert sorted(rows) == sorted([saved["mark"], later])


@pytest.mark.skipif(STEP != "after", reason="only in the second step of the round trip")
def test_after_the_restore_each_mark_is_back_and_the_admin_signs_in(
    env, airflow_url, token, metabase_url, session
):
    saved = json.loads(MARKS.read_text(encoding="utf-8"))
    assert warehouse_output(env, "SELECT mark FROM public.agoranomos_roundtrip") == saved["mark"]
    if DEMO:
        mart = {
            key: str(value) for key, value in totals(env, "marts.ad_stats_campaign_daily").items()
        }
        assert mart == saved["mart"]
    status, variable = api(airflow_url, token, "GET", f"/api/v2/variables/{VARIABLE}")
    assert status == 200
    assert variable["value"] == saved["mark"]
    assert f"Round trip {saved['mark']}" in collection_names(metabase_url, session)
