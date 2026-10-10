"""Tests of postgres/backup.sh, the backups of the backup service (ADR-0020), in sh and in dash.

The script runs in the image of PostgreSQL. Here it runs on this machine, with stand-ins for
pg_dump, pg_dumpall, pg_restore and flock, which log their arguments and fail when FAKE_FAIL names
them, and for date, which answers for the time FAKE_NOW, in UTC. tar, sha256sum, awk, sed and the
rest are the real ones. A test of one function cuts it out of the script.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import signal
import stat
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "postgres" / "backup.sh"
# 2026-10-11 03:00:12 UTC, the start of the backup that most tests take.
NOW = int(datetime(2026, 10, 11, 3, 0, 12, tzinfo=UTC).timestamp())
NAME = "20261011T030012Z"
HOUR = 3600
DAY = 24 * HOUR
FILES = [
    "airflow.dump",
    "extensions.tar.gz",
    "globals-platform-db.sql",
    "globals-warehouse-db.sql",
    "metabase.dump",
    "warehouse.dump",
]
PASSWORD = "a-password-that-must-stay-out-of-the-arguments"
IMAGES = "metabase/metabase:v0.64.1@sha256:aaa postgres:18.6-trixie@sha256:bbb"


def available_shells() -> list[str]:
    """/bin/sh, and dash when it is installed and /bin/sh is not already dash."""
    shells = ["/bin/sh"]
    dash = shutil.which("dash")
    if dash and Path(dash).resolve() != Path("/bin/sh").resolve():
        shells.append(dash)
    return shells


SHELLS = available_shells()

FAKE_TOOL = r"""#!/bin/sh
# A stand-in for {name}: it logs its arguments, fails when FAKE_FAIL names it with the database or
# the file that it works on, such as {name}:metabase, and writes a small file where --file= says.
printf '{name} %s\n' "$*" >> "$FAKE_LOG"
target="" output=""
for argument in "$@"; do
  case "$argument" in
    --dbname=* | --database=*) target=${{argument#*=}} ;;
    --file=*) output=${{argument#--file=}} ;;
    -*) ;;
    *) target=${{argument##*/}} ;;
  esac
done
case " ${{FAKE_FAIL:-}} " in *" {name}:$target "*) exit 1 ;; esac
if [ -n "$output" ] && [ "$output" != /dev/null ]; then
  printf '{name} of %s\n' "$target" > "$output"
fi
"""

# flock: it logs its arguments, and with FAKE_FLOCK_BUSY another process holds the lock.
FAKE_FLOCK = """#!/bin/sh
printf 'flock %s\\n' "$*" >> "$FAKE_LOG"
[ -z "${FAKE_FLOCK_BUSY:-}" ]
"""

FAKE_DATE = '''#!{python}
"""A stand-in for date: +FORMAT, of the time FAKE_NOW (seconds since 1970) or of now, in UTC."""
import os
import sys
import time
from datetime import UTC, datetime

now = int(os.environ.get("FAKE_NOW") or time.time())
formats = [argument[1:] for argument in sys.argv[1:] if argument.startswith("+")]
pattern = (formats or ["%a %b %d %H:%M:%S UTC %Y"])[0].replace("%s", str(now))
print(datetime.fromtimestamp(now, UTC).strftime(pattern))
'''


def write_tool(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")
    path.chmod(0o755)


@pytest.fixture
def backups(tmp_path: Path) -> Path:
    root = tmp_path / "backups"
    root.mkdir(mode=0o700)
    dags = tmp_path / "extensions" / "dags"
    dags.mkdir(parents=True)
    (dags / "my_dag.py").write_text("# a DAG of the deployment\n", encoding="utf-8")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for name in ("pg_dump", "pg_dumpall", "pg_restore"):
        write_tool(bin_dir / name, FAKE_TOOL.format(name=name))
    write_tool(bin_dir / "flock", FAKE_FLOCK)
    write_tool(bin_dir / "date", FAKE_DATE.format(python=sys.executable))
    return root


def environment(backups: Path, **fake: str) -> dict[str, str]:
    tmp = backups.parent
    return {
        "PATH": f"{tmp / 'bin'}{os.pathsep}{os.environ['PATH']}",
        "BACKUP_ROOT": str(backups),
        "EXTENSIONS": str(tmp / "extensions"),
        "STARTED_FILE": str(tmp / "started"),
        "FAKE_LOG": str(tmp / "tools.log"),
        "FAKE_NOW": str(NOW),
        "TZ": "UTC",
        "PGUSER": "backup",
        "PGPASSWORD": PASSWORD,
        "AGORANOMOS_IMAGES": IMAGES,
        **fake,
    }


def run(shell: str, backups: Path, *args: str, **fake: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [shell, str(SCRIPT), *args],
        env=environment(backups, **fake),
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


def latest(backups: Path) -> dict:
    return json.loads((backups / "latest.json").read_text(encoding="utf-8"))


def manifest(backups: Path, name: str) -> dict:
    return json.loads((backups / name / "manifest.json").read_text(encoding="utf-8"))


def tool_calls(backups: Path) -> list[str]:
    log = backups.parent / "tools.log"
    return log.read_text(encoding="utf-8").splitlines() if log.exists() else []


def old_backup(backups: Path, moment: int, kind: str) -> str:
    """A backup folder of the time moment, as far as the retention reads it: its manifest names its
    kind, one field to a line, as the script writes it."""
    name = datetime.fromtimestamp(moment, UTC).strftime("%Y%m%dT%H%M%SZ")
    folder = backups / name
    folder.mkdir(mode=0o700)
    (folder / "manifest.json").write_text(
        f'{{\n  "format": 1,\n  "name": "{name}",\n  "kind": "{kind}",\n  "files": []\n}}\n',
        encoding="utf-8",
    )
    return name


def backup_names(backups: Path) -> list[str]:
    return sorted(path.name for path in backups.iterdir() if path.is_dir())


def shell_function(name: str) -> str:
    """The definition of a function of the script, from its first line to the next line }."""
    lines = SCRIPT.read_text(encoding="utf-8").splitlines()
    for start, line in enumerate(lines):
        if line.startswith(f"{name}() {{"):
            end = start if line.endswith("}") else lines.index("}", start)
            return "\n".join(lines[start : end + 1])
    raise AssertionError(f"postgres/backup.sh defines no function {name}")


@pytest.mark.parametrize("shell", SHELLS)
def test_a_backup_holds_each_database_the_globals_and_the_extensions(shell, backups):
    result = run(shell, backups, "now")
    assert result.returncode == 0, result.stderr
    folder = backups / NAME
    assert sorted(path.name for path in folder.iterdir()) == sorted(
        [*FILES, "SHA256SUMS", "manifest.json"]
    )
    assert not list(backups.glob(".partial-*"))
    calls = tool_calls(backups)
    dumps = [call for call in calls if call.startswith("pg_dump ")]
    assert [call.split("--dbname=")[1].split()[0] for call in dumps] == [
        "warehouse",
        "airflow",
        "metabase",
    ]
    for call in dumps:
        assert "--username=backup" in call
        assert "--format=custom --compress=zstd" in call
        assert "--lock-wait-timeout=5min" in call
    assert "--host=warehouse-db" in dumps[0]
    assert all("--host=platform-db" in call for call in dumps[1:])
    globals_ = [call for call in calls if call.startswith("pg_dumpall ")]
    assert all("--globals-only --no-role-passwords" in call for call in globals_)
    assert len(globals_) == 2
    # PUBLIC cannot connect to the database postgres, so each connects to a database of the stack.
    assert "--host=warehouse-db" in globals_[0]
    assert "--database=warehouse" in globals_[0]
    assert "--host=platform-db" in globals_[1]
    assert "--database=airflow" in globals_[1]
    archive = subprocess.run(
        ["tar", "-tzf", str(folder / "extensions.tar.gz")],
        capture_output=True,
        text=True,
        check=True,
    )
    assert "my_dag.py" in archive.stdout
    assert f"the manual backup {NAME} is complete" in result.stdout


@pytest.mark.parametrize("shell", SHELLS)
def test_a_backup_is_readable_by_its_owner_only_and_no_password_reaches_a_command_line(
    shell, backups
):
    assert run(shell, backups, "now").returncode == 0
    folder = backups / NAME
    assert stat.S_IMODE(folder.stat().st_mode) == 0o700
    for path in [*folder.iterdir(), backups / "latest.json"]:
        assert stat.S_IMODE(path.stat().st_mode) == 0o600, path.name
    assert not any(PASSWORD in call for call in tool_calls(backups))


@pytest.mark.parametrize("shell", SHELLS)
def test_the_manifest_names_the_kind_each_file_with_its_size_and_checksum_and_the_images(
    shell, backups
):
    assert run(shell, backups, "now").returncode == 0
    folder = backups / NAME
    found = manifest(backups, NAME)
    assert found["format"] == 1
    assert found["name"] == NAME
    assert found["kind"] == "manual"
    assert found["started"] == found["finished"] == "2026-10-11T03:00:12Z"
    assert found["finished_epoch"] == NOW
    assert found["images"] == IMAGES
    files = {item["name"]: item for item in found["files"]}
    assert sorted(files) == FILES
    for name, item in files.items():
        data = (folder / name).read_bytes()
        assert item["bytes"] == len(data), name
        assert item["sha256"] == hashlib.sha256(data).hexdigest(), name
    check = subprocess.run(
        ["sha256sum", "--check", "--quiet", "--strict", "SHA256SUMS"], cwd=folder, check=False
    )
    assert check.returncode == 0


@pytest.mark.parametrize("shell", SHELLS)
def test_latest_says_how_the_run_went_and_keeps_the_scheduled_one_apart(shell, backups):
    assert run(shell, backups, "scheduled").returncode == 0
    assert latest(backups) == {
        "result": "ok",
        "attempted": "2026-10-11T03:00:12Z",
        "step": "",
        "kind": "scheduled",
        "last_ok": NAME,
        "last_ok_epoch": NOW,
        "last_scheduled_local": "2026-10-11 03:00",
    }
    # A backup by hand an hour later: it is the last good backup, and the scheduled one stays.
    assert run(shell, backups, "now", FAKE_NOW=str(NOW + HOUR)).returncode == 0
    assert latest(backups)["kind"] == "manual"
    assert latest(backups)["last_ok"] == "20261011T040012Z"
    assert latest(backups)["last_scheduled_local"] == "2026-10-11 03:00"


@pytest.mark.parametrize("shell", SHELLS)
def test_a_failed_step_leaves_no_partial_backup_and_keeps_the_last_good_one(shell, backups):
    assert run(shell, backups, "now").returncode == 0
    result = run(shell, backups, "now", FAKE_NOW=str(NOW + DAY), FAKE_FAIL="pg_dump:metabase")
    assert result.returncode == 1
    assert result.stderr.splitlines() == [
        "backup: error: the backup 20261012T030012Z failed at: the dump of metabase"
    ]
    assert backup_names(backups) == [NAME]
    assert not list(backups.glob(".partial-*"))
    assert latest(backups)["result"] == "failed"
    assert latest(backups)["step"] == "the dump of metabase"
    assert latest(backups)["last_ok"] == NAME
    assert latest(backups)["last_ok_epoch"] == NOW


@pytest.mark.parametrize("shell", SHELLS)
def test_a_backup_by_hand_waits_for_one_that_runs_and_the_schedule_does_not(shell, backups):
    result = run(shell, backups, "now", FAKE_FLOCK_BUSY="1")
    assert result.returncode == 1
    assert "waiting for it, at most 10 minutes" in result.stdout
    assert "another backup still runs after 10 minutes" in result.stderr
    assert tool_calls(backups) == ["flock -n 9", "flock -w 600 9"]
    assert not (backups / "latest.json").exists()
    (backups.parent / "tools.log").unlink()
    result = run(shell, backups, "scheduled", FAKE_FLOCK_BUSY="1")
    assert result.returncode == 1
    assert "another backup is running: the schedule tries again in an hour" in result.stderr
    assert tool_calls(backups) == ["flock -n 9"]


@pytest.mark.parametrize("shell", SHELLS)
def test_a_folder_that_cannot_be_written_stops_the_backup(shell, backups):
    backups.chmod(0o500)
    try:
        result = run(shell, backups, "now")
    finally:
        backups.chmod(0o700)
    assert result.returncode == 1
    assert f"cannot write the backups folder {backups}" in result.stderr
    assert "AGORANOMOS_UID" in result.stderr


@pytest.mark.parametrize(
    ("setting", "value", "message"),
    [
        ("BACKUP_TIME", "3am", "BACKUP_TIME must be a time of day as HH:MM"),
        ("BACKUP_TIME", "24:00", "BACKUP_TIME must be a time of day as HH:MM"),
        ("BACKUP_KEEP_WEEKLY", "four", "must be whole numbers"),
        ("BACKUP_KEEP_DAILY", "0", "BACKUP_KEEP_DAILY must be at least 1"),
        ("TZ", "Nowhere/Town", "BACKUP_TZ is not a time zone of this image"),
    ],
)
def test_a_setting_that_the_script_cannot_use_stops_it_with_its_name(
    backups, setting, value, message
):
    result = run("/bin/sh", backups, "now", **{setting: value, "ZONEINFO": str(backups.parent)})
    assert result.returncode == 1
    assert message in result.stderr
    assert tool_calls(backups) == []


def kept_by_reference(names: list[str], daily: int, weekly: int, monthly: int) -> set[str]:
    """The retention of ADR-0020, written out with the calendar of Python: the newest backup of
    each of the last days, ISO weeks and months that have backups."""
    kept: set[str] = set()
    seen: dict[str, list] = {"day": [], "week": [], "month": []}
    limits = {"day": daily, "week": weekly, "month": monthly}
    for name in sorted(names, reverse=True):
        moment = datetime.strptime(name, "%Y%m%dT%H%M%SZ")
        keys = {
            "day": moment.date(),
            "week": moment.isocalendar()[:2],
            "month": (moment.year, moment.month),
        }
        for period, key in keys.items():
            if key not in seen[period]:
                seen[period].append(key)
                if len(seen[period]) <= limits[period]:
                    kept.add(name)
    return kept


@pytest.mark.parametrize("shell", SHELLS)
@pytest.mark.parametrize("keep", [(7, 4, 3), (1, 0, 0), (2, 1, 1), (30, 10, 12)])
def test_the_retention_keeps_the_newest_backup_of_each_day_week_and_month(shell, keep):
    start = datetime(2026, 3, 1, 3, 0, tzinfo=UTC)
    names = [
        (start + timedelta(days=offset, minutes=offset % 3)).strftime("%Y%m%dT%H%M%SZ")
        for offset in range(230)
        if offset % 11 != 5  # a day now and then without a backup
    ]
    names.append("20261016T140000Z")  # a second backup on the newest day
    expected = sorted(set(names) - kept_by_reference(names, *keep), reverse=True)
    program = shell_function("expired") + '\nexpired "$1" "$2" "$3"'
    result = subprocess.run(
        [shell, "-c", program, "sh", *map(str, keep)],
        input="".join(f"{name}\n" for name in sorted(names, reverse=True)),
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.split() == expected


@pytest.mark.parametrize("shell", SHELLS)
def test_a_scheduled_backup_removes_the_scheduled_ones_that_the_retention_no_longer_keeps(
    shell, backups
):
    for days_ago in range(1, 6):
        old_backup(backups, NOW - days_ago * DAY, "scheduled")
    (backups / ".partial-20261001T030000Z").mkdir()  # a run that was killed
    result = run(
        shell,
        backups,
        "scheduled",
        BACKUP_KEEP_DAILY="3",
        BACKUP_KEEP_WEEKLY="0",
        BACKUP_KEEP_MONTHLY="0",
    )
    assert result.returncode == 0, result.stderr
    assert backup_names(backups) == ["20261009T030012Z", "20261010T030012Z", NAME]


@pytest.mark.parametrize("shell", SHELLS)
def test_a_backup_by_hand_is_never_removed_by_the_retention(shell, backups):
    # Such as the backup before an upgrade of Metabase: the only way back from its migration.
    by_hand = [old_backup(backups, NOW - days * DAY - HOUR, "manual") for days in range(40)]
    result = run(
        shell,
        backups,
        "scheduled",
        BACKUP_KEEP_DAILY="1",
        BACKUP_KEEP_WEEKLY="0",
        BACKUP_KEEP_MONTHLY="0",
    )
    assert result.returncode == 0, result.stderr
    assert backup_names(backups) == sorted([*by_hand, NAME])


@pytest.mark.parametrize("shell", SHELLS)
def test_two_backups_on_one_day_both_stay_when_one_is_by_hand(shell, backups):
    keep_one = {"BACKUP_KEEP_DAILY": "1", "BACKUP_KEEP_WEEKLY": "0", "BACKUP_KEEP_MONTHLY": "0"}

    def take(kind: str, hours_later: int) -> None:
        result = run(shell, backups, kind, FAKE_NOW=str(NOW + hours_later * HOUR), **keep_one)
        assert result.returncode == 0, result.stderr

    # By hand before an upgrade, then the scheduled backup of the same UTC day.
    take("now", 0)
    take("scheduled", 2)
    assert backup_names(backups) == [NAME, "20261011T050012Z"]
    # A second backup by hand the same day keeps both of them too.
    take("now", 3)
    assert backup_names(backups) == [NAME, "20261011T050012Z", "20261011T060012Z"]
    # Two scheduled backups of one day: the newest stays.
    take("scheduled", 4)
    assert backup_names(backups) == [NAME, "20261011T060012Z", "20261011T070012Z"]


@pytest.mark.parametrize(
    ("last_ok_hours_ago", "started_hours_ago", "healthy"),
    [(1, 100, True), (26, 100, True), (27, 100, False), (None, 1, True), (None, 27, False)],
)
def test_the_service_is_healthy_while_the_last_good_backup_is_fresh(
    backups, last_ok_hours_ago, started_hours_ago, healthy
):
    (backups.parent / "started").write_text(f"{NOW - started_hours_ago * HOUR}\n", encoding="utf-8")
    if last_ok_hours_ago is not None:
        (backups / "latest.json").write_text(
            "{\n"
            '  "result": "ok",\n'
            '  "last_ok": "x",\n'
            f'  "last_ok_epoch": {NOW - last_ok_hours_ago * HOUR},\n'
            '  "last_scheduled_local": "x"\n'
            "}\n",
            encoding="utf-8",
        )
    result = run("/bin/sh", backups, "health")
    assert (result.returncode == 0) is healthy, result.stdout
    if not healthy:
        assert "hours" in result.stdout


@pytest.mark.parametrize(
    ("now", "last_scheduled_local", "due"),
    [
        ("2026-10-11T03:00:00", None, True),  # no scheduled backup yet: at once
        ("2026-10-11T02:59:00", "2026-10-10 03:00", False),  # before the time of the day
        ("2026-10-11T03:00:00", "2026-10-10 03:00", True),  # the time of the day has come
        ("2026-10-11T10:00:00", "2026-10-11 03:00", False),  # the backup of the day is done
        ("2026-10-11T10:00:00", "2026-10-09 03:00", True),  # the stack was down at 03:00
    ],
)
def test_a_scheduled_backup_is_due_once_a_day_at_its_time(backups, now, last_scheduled_local, due):
    if last_scheduled_local is not None:
        (backups / "latest.json").write_text(
            "{\n"
            '  "kind": "manual",\n'
            f'  "last_ok": "{NAME}",\n'
            f'  "last_scheduled_local": "{last_scheduled_local}"\n'
            "}\n",
            encoding="utf-8",
        )
    moment = int(datetime.fromisoformat(now).replace(tzinfo=UTC).timestamp())
    program = "\n".join([shell_function("field"), shell_function("due"), "due"])
    result = subprocess.run(
        ["/bin/sh", "-c", program],
        env=environment(backups, FAKE_NOW=str(moment), BACKUP_TIME="03:00"),
        capture_output=True,
        text=True,
        check=False,
    )
    assert (result.returncode == 0) is due, result.stderr


def test_a_backup_by_hand_does_not_stand_in_for_the_scheduled_one(backups):
    # By hand at 04:00 after a scheduled backup the day before: the one of today is still due.
    yesterday = NOW - DAY
    assert run("/bin/sh", backups, "scheduled", FAKE_NOW=str(yesterday)).returncode == 0
    assert run("/bin/sh", backups, "now", FAKE_NOW=str(NOW + HOUR)).returncode == 0
    program = "\n".join([shell_function("field"), shell_function("due"), "due"])
    result = subprocess.run(
        ["/bin/sh", "-c", program],
        env=environment(backups, FAKE_NOW=str(NOW + 2 * HOUR), BACKUP_TIME="03:00"),
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0


@pytest.mark.parametrize("shell", SHELLS)
def test_verify_passes_a_complete_backup_and_stops_on_a_damaged_one(shell, backups):
    assert run(shell, backups, "now").returncode == 0
    assert run(shell, backups, "verify", NAME, "warehouse", "airflow", "metabase").returncode == 0
    dump = backups / NAME / "warehouse.dump"
    data = dump.read_bytes()
    dump.write_bytes(data[: len(data) // 2])
    result = run(shell, backups, "verify", NAME, "warehouse")
    assert result.returncode == 1
    assert f"{NAME} is damaged" in result.stderr
    dump.write_bytes(data)
    # A dump cut short whose checksum fits it too: only a full read finds it.
    result = run(shell, backups, "verify", NAME, "metabase", FAKE_FAIL="pg_restore:metabase.dump")
    assert result.returncode == 1
    assert "metabase.dump does not read to its end" in result.stderr


@pytest.mark.parametrize("name", ["../x", "20261011", "20261011T030012Z/..", ""])
def test_verify_takes_only_the_name_of_a_backup(backups, name):
    result = run("/bin/sh", backups, "verify", name)
    assert result.returncode == 1
    assert "not the name of a backup" in result.stderr


def test_verify_stops_on_a_folder_without_its_manifest(backups):
    (backups / NAME).mkdir()
    result = run("/bin/sh", backups, "verify", NAME)
    assert result.returncode == 1
    assert "is not a complete backup" in result.stderr


def test_the_schedule_takes_the_first_backup_at_once_and_stops_on_sigterm(backups):
    # Its output goes nowhere: a sleep of the loop that outlived it would keep a pipe open.
    process = subprocess.Popen(
        ["/bin/sh", str(SCRIPT), "schedule"],
        env=environment(backups),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.monotonic() + 20
        while not (backups / "latest.json").exists() and time.monotonic() < deadline:
            time.sleep(0.1)
        assert latest(backups)["result"] == "ok"
        assert latest(backups)["kind"] == "scheduled"
        assert manifest(backups, NAME)["kind"] == "scheduled"
        assert (backups.parent / "started").read_text(encoding="utf-8").strip() == str(NOW)
        process.send_signal(signal.SIGTERM)
        assert process.wait(timeout=10) == 0
    finally:
        process.kill()
        process.wait(timeout=10)


def test_usage(backups):
    result = run("/bin/sh", backups, "restore")
    assert result.returncode == 1
    assert "usage: backup.sh schedule | now | health | verify" in result.stderr
