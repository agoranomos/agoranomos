"""Backups on a running stack (ADR-0020): ./agoranomos backup, what it writes, the backup service
of the ops profile, and ./agoranomos status. The tests take real backups by hand into the backups
folder of the checkout, which stay there, as any backup by hand, until someone deletes them. They
print names, sizes and modes, never what a dump holds.

One test is destructive by design: test_restore_refuses_a_damaged_backup_and_changes_nothing runs a
real ./agoranomos restore against the project that .env names, to prove that it stops before it
changes anything. If restore were wrong, that run would stop the services and drop the warehouse.
It runs only with AGORANOMOS_RESTORE_TESTS=1, which the server job of the Stack workflow sets and a
person sets only on a stack that may lose its data."""

import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import time
from datetime import UTC, datetime

import pytest
from services import OPS
from stack_helpers import (
    REPO_ROOT,
    compose,
    containers,
    docker,
    process_uids,
    read_env,
    warehouse_output,
)

# The env fixture says what is missing when there is no .env: the module still collects without it.
ENV_FILE = REPO_ROOT / ".env"
FOLDER = (read_env(ENV_FILE).get("AGORANOMOS_BACKUP_DIR") if ENV_FILE.exists() else None) or (
    "./backups"
)
BACKUPS = REPO_ROOT / FOLDER
FILES = {
    "airflow.dump",
    "extensions.tar.gz",
    "globals-platform-db.sql",
    "globals-warehouse-db.sql",
    "metabase.dump",
    "warehouse.dump",
    "SHA256SUMS",
    "manifest.json",
}
# A backup folder is named after the start of the backup in UTC.
NAME = re.compile(r"\d{8}T\d{6}Z")
# The opt-in of the test that runs a restore.
RESTORE_TESTS = os.environ.get("AGORANOMOS_RESTORE_TESTS") == "1"
# The name of the copy of a backup that a test damages: the oldest name there can be.
DAMAGED = "20000101T000000Z"


def agoranomos(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(REPO_ROOT / "agoranomos"), *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=900,
        check=False,
    )


def latest() -> dict:
    return json.loads((BACKUPS / "latest.json").read_text(encoding="utf-8"))


def images_of_the_checkout() -> set[str]:
    """The images that ./agoranomos hands the backup service, read the same way from the files."""
    found = set()
    for name in ("compose.yaml", "compose.celery.yaml", "airflow/Dockerfile", "caddy/Dockerfile"):
        text = (REPO_ROOT / name).read_text(encoding="utf-8")
        found |= set(re.findall(r"^\s*image:\s*(\S+)", text, re.M))
        found |= set(re.findall(r"^FROM\s+(\S+)", text, re.M))
    return {image for image in found if not image.endswith(":local")}


@pytest.fixture(scope="module")
def taken() -> str:
    """A backup taken now, by name."""
    result = agoranomos("backup")
    assert result.returncode == 0, result.stderr[-2000:]
    assert latest()["result"] == "ok"
    return latest()["last_ok"]


def test_a_backup_holds_every_file_and_belongs_to_the_owner_of_the_checkout(taken):
    folder = BACKUPS / taken
    assert {path.name for path in folder.iterdir()} == FILES
    assert stat.S_IMODE(folder.stat().st_mode) == 0o700
    for path in folder.iterdir():
        assert stat.S_IMODE(path.stat().st_mode) == 0o600, path.name
        assert path.stat().st_uid == os.getuid(), path.name


def test_the_manifest_records_the_images_of_the_checkout(taken):
    manifest = json.loads((BACKUPS / taken / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["kind"] == "manual"
    assert set(manifest["images"].split()) == images_of_the_checkout()


def test_each_dump_reads_to_its_end_in_the_image_of_the_databases(taken):
    result = compose(
        "run",
        "--rm",
        "--no-deps",
        "-T",
        "backup",
        "verify",
        taken,
        "warehouse",
        "airflow",
        "metabase",
        check=False,
    )
    assert result.returncode == 0, result.stderr[-2000:]


def test_status_names_the_last_backup_and_lists_it_as_one_by_hand(taken):
    output = agoranomos("status").stdout
    assert f"Backups: the last good backup is {taken}" in output
    assert re.search(rf"^  {taken}: \d+ KiB, 0 days old$", output, re.M)


@pytest.mark.skipif(not OPS, reason="the ops profile is off: no backup service runs")
def test_the_backup_service_runs_as_the_owner_of_the_checkout_and_is_healthy():
    container = containers()["backup"]
    assert container["State"] == "running"
    assert container["Health"] == "healthy"
    uids = compose("exec", "-T", "backup", "sh", "-c", process_uids("sh")).stdout.split()
    assert uids[:2] == ["Uid:", str(os.getuid())]


def scheduled_backups() -> list[str]:
    """The names of the scheduled backups in the backups folder, oldest first."""
    return [
        folder.name
        for folder in sorted(BACKUPS.iterdir())
        if NAME.fullmatch(folder.name)
        and '"kind": "scheduled"' in (folder / "manifest.json").read_text(encoding="utf-8")
    ]


def backup_time(name: str) -> datetime:
    """The start of a backup, from its name, in UTC."""
    return datetime.strptime(name, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)


def service_started_at() -> datetime:
    """When the container of the backup service last started, in UTC, to the second."""
    started = docker(
        "inspect", "--format", "{{.State.StartedAt}}", containers()["backup"]["ID"]
    ).stdout.strip()
    # Docker gives nanoseconds and a Z: fromisoformat reads them, and drops what is finer.
    return datetime.fromisoformat(started).astimezone(UTC).replace(microsecond=0)


@pytest.mark.skipif(not OPS, reason="the ops profile is off: no backup service runs")
def test_the_backup_service_takes_a_scheduled_backup_by_itself():
    # With no scheduled backup yet, the first is due at once (ADR-0020), so it comes within
    # minutes of the start, with no help from this test. It must be one that this container took:
    # a stand that has run before holds older ones, and the first is then not due at the start.
    started = service_started_at()
    if any(backup_time(name) < started for name in scheduled_backups()):
        pytest.skip(
            "a scheduled backup from before this start exists: the first is not due at once"
        )
    deadline = time.monotonic() + 300
    taken = []
    while time.monotonic() < deadline:
        taken = [name for name in scheduled_backups() if backup_time(name) >= started]
        if taken:
            break
        time.sleep(5)
    assert taken, "the backup service took no scheduled backup in five minutes after its start"
    folder = BACKUPS / taken[-1]
    assert {path.name for path in folder.iterdir()} == FILES
    assert stat.S_IMODE(folder.stat().st_mode) == 0o700
    assert folder.stat().st_uid == os.getuid()
    assert latest()["last_scheduled_local"]


@pytest.mark.skipif(
    not RESTORE_TESTS,
    reason="it runs a restore: set AGORANOMOS_RESTORE_TESTS=1 on a stack that may lose its data",
)
@pytest.mark.parametrize("damage", ["truncated", "truncated with its checksum"])
def test_restore_refuses_a_damaged_backup_and_changes_nothing(env, taken, damage):
    # A dump cut short passes pg_restore --list, and a restore from it would drop the database and
    # leave it empty: restore must stop before it stops a service or drops anything.
    damaged = BACKUPS / DAMAGED
    shutil.copytree(BACKUPS / taken, damaged)
    try:
        dump = damaged / "warehouse.dump"
        data = dump.read_bytes()
        dump.write_bytes(data[: len(data) * 3 // 5])
        if damage == "truncated with its checksum":
            sums = (damaged / "SHA256SUMS").read_text(encoding="utf-8").splitlines()
            digest = hashlib.sha256(dump.read_bytes()).hexdigest()
            (damaged / "SHA256SUMS").write_text(
                "".join(
                    f"{digest}  warehouse.dump\n"
                    if line.endswith(" warehouse.dump")
                    else f"{line}\n"
                    for line in sums
                ),
                encoding="utf-8",
            )
        tables = "SELECT count(*) FROM pg_class WHERE relnamespace = 'marts'::regnamespace"
        before = warehouse_output(env, tables)
        result = agoranomos("restore", DAMAGED, "--only", "warehouse", "--yes")
        assert result.returncode == 1
        assert "nothing was changed" in result.stderr
        expected = "is damaged" if damage == "truncated" else "does not read to its end"
        assert expected in result.stdout + result.stderr
        assert warehouse_output(env, tables) == before
        assert containers()["metabase"]["State"] == "running"
    finally:
        shutil.rmtree(damaged)
