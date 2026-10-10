"""Backups on a running stack (ADR-0020): ./agoranomos backup, what it writes, the backup service
of the ops profile, and ./agoranomos status. The tests take real backups by hand into the backups
folder of the checkout, which stay there, as any backup by hand, until someone deletes them. They
print names, sizes and modes, never what a dump holds."""

import json
import os
import re
import stat
import subprocess
import time

import pytest
from services import OPS
from stack_helpers import REPO_ROOT, compose, containers, process_uids, read_env

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


@pytest.mark.skipif(not OPS, reason="the ops profile is off: no backup service runs")
def test_the_backup_service_takes_a_scheduled_backup_by_itself():
    # With no scheduled backup yet, the first is due at once (ADR-0020), so it comes within
    # minutes of the start, with no help from this test.
    deadline = time.monotonic() + 300
    scheduled = []
    while time.monotonic() < deadline:
        scheduled = [
            folder.name
            for folder in sorted(BACKUPS.iterdir())
            if NAME.fullmatch(folder.name)
            and '"kind": "scheduled"' in (folder / "manifest.json").read_text(encoding="utf-8")
        ]
        if scheduled:
            break
        time.sleep(5)
    assert scheduled, "the backup service took no scheduled backup in five minutes"
    folder = BACKUPS / scheduled[-1]
    assert {path.name for path in folder.iterdir()} == FILES
    assert stat.S_IMODE(folder.stat().st_mode) == 0o700
    assert folder.stat().st_uid == os.getuid()
    assert latest()["last_scheduled_local"]
