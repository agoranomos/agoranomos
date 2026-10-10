"""Tests of postgres/backup.sh, the backups of the backup service (ADR-0020), in sh and in dash.

The script runs in the image of PostgreSQL. Here it runs on this machine, with stand-ins for
pg_dump, pg_dumpall, pg_restore and flock, which log their arguments and fail when FAKE_FAIL names
them, for mkdir and wc, which fail when FAKE_FAIL names them and are the real ones otherwise, and
for date and sleep, which share one clock. The clock is FAKE_NOW, or the file FAKE_CLOCK when a test
runs the schedule over hours and days: date formats it with the date of this machine, in the zone
TZ names unless it gets -u, and sleep moves it forward and returns at once. tar, sha256sum, awk, sed
and the rest are the real ones. A test of one function cuts it out of the script.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import signal
import stat
import subprocess
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

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


def zone_files_exist() -> bool:
    """Whether this machine has the zone files that the tests of time zones read."""
    try:
        ZoneInfo("Europe/Berlin")
    except ZoneInfoNotFoundError:
        return False
    return Path("/usr/share/zoneinfo/Europe/Berlin").is_file()


needs_zone_files = pytest.mark.skipif(
    not zone_files_exist(), reason="the zone files of this machine have no Europe/Berlin"
)

FAKE_TOOL = r"""#!/bin/sh
# A stand-in for {name}: it logs its arguments, fails when FAKE_FAIL names it with the database or
# the file that it works on, such as {name}:metabase, and writes a small file where --file= says.
printf '{name} %s\n' "$*" >> "$FAKE_LOG"
target="" output="" listing=""
for argument in "$@"; do
  case "$argument" in
    --list) listing=1 ;;
    --dbname=* | --database=*) target=${{argument#*=}} ;;
    --file=*) output=${{argument#--file=}} ;;
    -*) ;;
    *) target=${{argument##*/}} ;;
  esac
done
# FAKE_FAIL_UNTIL: the failures stop when the clock FAKE_CLOCK reaches this time.
if [ -n "${{FAKE_FAIL_UNTIL:-}}" ]; then
  read -r clock < "$FAKE_CLOCK"
  if [ "$clock" -ge "$FAKE_FAIL_UNTIL" ]; then FAKE_FAIL=""; fi
fi
case " ${{FAKE_FAIL:-}} " in *" {name}:$target "*) exit 1 ;; esac
# FAKE_TRUNCATED: a dump cut short after its table of contents. pg_restore --list reads it to the
# end of the contents and passes it; a read of all of it, such as --file=/dev/null, fails.
case " ${{FAKE_TRUNCATED:-}} " in *" $target "*) [ -n "$listing" ] && exit 0; exit 1 ;; esac
# FAKE_SLOW: the work on this target takes FAKE_SLOW_SECONDS of real time.
case " ${{FAKE_SLOW:-}} " in *" {name}:$target "*) /bin/sleep "${{FAKE_SLOW_SECONDS:-1}}" ;; esac
if [ -n "$output" ] && [ "$output" != /dev/null ]; then
  printf '{name} of %s\n' "$target" > "$output"
fi
"""

# flock: it logs its arguments, and with FAKE_FLOCK_BUSY another process holds the lock.
FAKE_FLOCK = """#!/bin/sh
printf 'flock %s\\n' "$*" >> "$FAKE_LOG"
[ -z "${FAKE_FLOCK_BUSY:-}" ]
"""

# A tool that fails when FAKE_FAIL names it, and is the real one otherwise.
FAKE_PASSTHROUGH = """#!/bin/sh
case " ${FAKE_FAIL:-} " in *" @NAME@ "*) exit 1 ;; esac
exec @REAL@ "$@"
"""

# date: the real date of this machine, for the time of the clock: FAKE_CLOCK when that file exists,
# else FAKE_NOW. It formats in the zone of TZ, and in UTC with -u, as the date of the image does.
FAKE_DATE = """#!/bin/sh
now=${FAKE_NOW:-}
if [ -n "${FAKE_CLOCK:-}" ] && [ -f "$FAKE_CLOCK" ]; then read -r now < "$FAKE_CLOCK"; fi
if [ -z "$now" ]; then exec @REAL@ "$@"; fi
exec @REAL@ @AT@ "$@"
"""

# sleep: with a clock file it moves the clock forward by FAKE_STEP seconds (its own argument when
# that is not set) and returns at once; the sleep that brings the clock to FAKE_END sends TERM to
# the shell that started it, such as the schedule. Without a clock file it sleeps a little.
FAKE_SLEEP = """#!/bin/sh
if [ -z "${FAKE_CLOCK:-}" ]; then exec /bin/sleep 0.05; fi
read -r now < "$FAKE_CLOCK"
now=$((now + ${FAKE_STEP:-${1:-0}}))
echo "$now" > "$FAKE_CLOCK"
if [ -n "${FAKE_END:-}" ] && [ "$now" -ge "$FAKE_END" ]; then kill -TERM "$PPID"; fi
"""


def fake_date() -> str:
    """The stand-in for date. GNU date takes the time as -d @N, BSD date as -r N."""
    real = shutil.which("date")
    assert real, "this machine has no date"
    gnu = subprocess.run([real, "-d", "@0"], capture_output=True, check=False).returncode == 0
    return FAKE_DATE.replace("@REAL@", real).replace("@AT@", '-d "@$now"' if gnu else '-r "$now"')


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
    write_tool(bin_dir / "date", fake_date())
    write_tool(bin_dir / "sleep", FAKE_SLEEP)
    for name in ("mkdir", "wc"):
        real = shutil.which(name)
        assert real, f"this machine has no {name}"
        write_tool(bin_dir / name, FAKE_PASSTHROUGH.replace("@NAME@", name).replace("@REAL@", real))
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
        timeout=120,
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


def utc_epoch(text: str) -> int:
    """The epoch of a time in UTC, such as 2026-10-11 03:00."""
    return int(datetime.fromisoformat(text).replace(tzinfo=UTC).timestamp())


def berlin_epoch(text: str) -> int:
    """The epoch of a time on the clocks of Berlin (the first one, of a time that occurs twice)."""
    return int(datetime.fromisoformat(text).replace(tzinfo=ZoneInfo("Europe/Berlin")).timestamp())


def seed_latest(backups: Path, last_scheduled_local: str) -> None:
    """The latest.json of a deployment whose last good scheduled backup started at this time."""
    (backups / "latest.json").write_text(
        f'{{\n  "last_scheduled_local": "{last_scheduled_local}"\n}}\n', encoding="utf-8"
    )


def simulate(
    shell: str, backups: Path, start: int, end: int, step: int, **fake: str
) -> subprocess.CompletedProcess[str]:
    """Run the schedule on the fake clock from start to end (epochs). Each sleep of its loop moves
    the clock forward by step seconds, and the one that reaches end stops the schedule with TERM."""
    clock = backups.parent / "clock"
    clock.write_text(f"{start}\n", encoding="utf-8")
    return run(
        shell,
        backups,
        "schedule",
        FAKE_CLOCK=str(clock),
        FAKE_END=str(end),
        FAKE_STEP=str(step),
        **fake,
    )


def attempts(result: subprocess.CompletedProcess[str]) -> list[int]:
    """The start (epoch) of each scheduled backup that the schedule took, good or not, from the
    names in its output."""
    return [
        utc_epoch(datetime.strptime(name, "%Y%m%dT%H%M%SZ").isoformat())
        for name in re.findall(r"taking the scheduled backup (\d{8}T\d{6}Z)", result.stdout)
    ]


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


@pytest.mark.parametrize(("started_hours_ago", "healthy"), [(1, True), (26, True), (27, False)])
def test_the_service_counts_from_its_start_while_every_run_has_failed(
    backups, started_hours_ago, healthy
):
    # latest.json of runs that all failed has no good backup (epoch 0): that is not "a backup from
    # 1970", and the 26 hours count from the start of the service.
    (backups.parent / "started").write_text(f"{NOW - started_hours_ago * HOUR}\n", encoding="utf-8")
    (backups / "latest.json").write_text(
        "{\n"
        '  "result": "failed",\n'
        '  "attempted": "2026-10-11T03:00:12Z",\n'
        '  "step": "the dump of warehouse",\n'
        '  "kind": "scheduled",\n'
        '  "last_ok": "",\n'
        '  "last_ok_epoch": 0,\n'
        '  "last_scheduled_local": ""\n'
        "}\n",
        encoding="utf-8",
    )
    result = run("/bin/sh", backups, "health")
    assert (result.returncode == 0) is healthy, result.stdout
    if not healthy:
        assert "since the service started" in result.stdout


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
    assert "read to their end" in run(shell, backups, "verify", NAME, "metabase").stdout


@pytest.mark.parametrize("shell", SHELLS)
def test_verify_reads_a_dump_to_its_end_because_the_table_of_contents_alone_passes(shell, backups):
    # A dump cut short whose checksum fits it too: pg_restore --list reads only the table of
    # contents and passes it (R10), so verify must read all of it.
    assert run(shell, backups, "now").returncode == 0
    (backups.parent / "tools.log").unlink()
    result = run(shell, backups, "verify", NAME, "metabase", FAKE_TRUNCATED="metabase.dump")
    assert result.returncode == 1
    assert f"{NAME}: metabase.dump does not read to its end" in result.stderr
    assert f"pg_restore --file=/dev/null {backups / NAME / 'metabase.dump'}" in tool_calls(backups)
    # The stand-in is right about --list: it passes the same dump.
    assert (
        subprocess.run(
            [str(backups.parent / "bin" / "pg_restore"), "--list", "metabase.dump"],
            env=environment(backups, FAKE_TRUNCATED="metabase.dump"),
            check=False,
        ).returncode
        == 0
    )


@pytest.mark.parametrize("shell", SHELLS)
def test_verify_takes_a_shared_lock_so_that_a_retention_cannot_remove_the_backup(shell, backups):
    assert run(shell, backups, "now").returncode == 0
    (backups.parent / "tools.log").unlink()
    assert run(shell, backups, "verify", NAME, "warehouse").returncode == 0
    assert tool_calls(backups)[0] == "flock -s -n 9"
    # A backup that runs: it waits up to 10 minutes, as a backup by hand does.
    (backups.parent / "tools.log").unlink()
    result = run(shell, backups, "verify", NAME, "warehouse", FAKE_FLOCK_BUSY="1")
    assert result.returncode == 1
    assert "waiting for it, at most 10 minutes" in result.stdout
    assert "still runs after 10 minutes" in result.stderr
    assert tool_calls(backups) == ["flock -s -n 9", "flock -s -w 600 9"]


@pytest.mark.parametrize("shell", SHELLS)
def test_verify_opens_the_lock_to_read_so_that_a_read_only_folder_will_do(shell, backups):
    assert run(shell, backups, "now").returncode == 0
    (backups / ".lock").chmod(0o400)
    result = run(shell, backups, "verify", NAME, "warehouse")
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("shell", SHELLS)
def test_verify_with_no_database_says_that_it_checked_the_files_only(shell, backups):
    assert run(shell, backups, "now").returncode == 0
    (backups.parent / "tools.log").unlink()
    result = run(shell, backups, "verify", NAME)
    assert result.returncode == 0, result.stderr
    assert "checked the files only" in result.stdout
    assert "read to their end" not in result.stdout
    assert not [call for call in tool_calls(backups) if call.startswith("pg_restore")]


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
    assert tool_calls(backups) == []  # no lock file, so no backup was ever taken and none runs


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


@needs_zone_files
@pytest.mark.parametrize("shell", SHELLS)
def test_the_names_and_times_of_a_backup_are_utc_whatever_the_time_zone_is(shell, backups):
    # 03:00:12 UTC is 05:00:12 in Berlin in October. The folder, the manifest and latest.json
    # say UTC; only the time that the schedule reads says Berlin.
    result = run(shell, backups, "scheduled", TZ="Europe/Berlin")
    assert result.returncode == 0, result.stderr
    assert backup_names(backups) == [NAME]
    assert manifest(backups, NAME)["started"] == "2026-10-11T03:00:12Z"
    assert manifest(backups, NAME)["finished"] == "2026-10-11T03:00:12Z"
    assert latest(backups)["attempted"] == "2026-10-11T03:00:12Z"
    assert latest(backups)["last_scheduled_local"] == "2026-10-11 05:00"


@needs_zone_files
@pytest.mark.parametrize(
    ("zone", "moment", "due"),
    [
        ("UTC", "2026-10-11T01:30:00", False),  # 01:30, before the 03:00 of UTC
        ("Europe/Berlin", "2026-10-11T00:30:00", False),  # 02:30 in Berlin
        ("Europe/Berlin", "2026-10-11T01:00:00", True),  # 03:00 in Berlin
        ("Europe/Berlin", "2026-10-11T01:30:00", True),  # 03:30 in Berlin
    ],
)
def test_the_time_zone_decides_the_local_time_of_the_backup(backups, zone, moment, due):
    seed_latest(backups, "2026-10-10 03:00")
    program = "\n".join([shell_function("field"), shell_function("due"), "due"])
    result = subprocess.run(
        ["/bin/sh", "-c", program],
        env=environment(backups, FAKE_NOW=str(utc_epoch(moment)), BACKUP_TIME="03:00", TZ=zone),
        capture_output=True,
        text=True,
        check=False,
    )
    assert (result.returncode == 0) is due, result.stderr


# The days of the time change of 2026 in Berlin, a BACKUP_TIME, and the UTC time of the first run.
# Spring: at 01:00 UTC the clocks go from 02:00 to 03:00. Autumn: at 01:00 UTC they go from 03:00
# back to 02:00. The times 02:30 run in both shells, the others in the last one (dash, when it is
# installed), as a run of a day and a half of the schedule takes a second or two.
TIME_CHANGE_CASES = [
    ("2026-03-29", "01:30", "2026-03-29 00:30"),
    ("2026-03-29", "02:30", "2026-03-29 01:00"),  # a time that does not exist: it runs at 03:00
    ("2026-03-29", "03:00", "2026-03-29 01:00"),
    ("2026-10-25", "02:00", "2026-10-25 00:00"),
    ("2026-10-25", "02:30", "2026-10-25 00:30"),  # a time that happens twice: it runs the first
    ("2026-10-25", "03:00", "2026-10-25 02:00"),
]


def time_change_params() -> list:
    return [
        pytest.param(shell, *case, id=f"{case[0]}-{case[1]}-{Path(shell).name}")
        for case in TIME_CHANGE_CASES
        for shell in (SHELLS if case[1] == "02:30" else SHELLS[-1:])
    ]


@needs_zone_files
@pytest.mark.parametrize(("shell", "day", "backup_time", "first_run_utc"), time_change_params())
def test_the_schedule_takes_one_backup_per_local_day_also_on_the_days_of_the_time_change(
    shell, backups, day, backup_time, first_run_utc
):
    following = (datetime.fromisoformat(day) + timedelta(days=1)).date().isoformat()
    previous = (datetime.fromisoformat(day) - timedelta(days=1)).date().isoformat()
    seed_latest(backups, f"{previous} {backup_time}")
    result = simulate(
        shell,
        backups,
        berlin_epoch(f"{day} 00:00"),
        berlin_epoch(f"{following} {backup_time}") + HOUR,
        1800,
        TZ="Europe/Berlin",
        BACKUP_TIME=backup_time,
    )
    assert result.returncode == 0, result.stderr
    found = attempts(result)
    assert found == [utc_epoch(first_run_utc), berlin_epoch(f"{following} {backup_time}")]
    local_days = [
        datetime.fromtimestamp(moment, ZoneInfo("Europe/Berlin")).date() for moment in found
    ]
    assert [moment.isoformat() for moment in local_days] == [day, following]
    assert latest(backups)["result"] == "ok"


@pytest.mark.parametrize("shell", SHELLS)
def test_a_failed_scheduled_run_is_tried_again_an_hour_later_and_not_sooner(shell, backups):
    start = utc_epoch("2026-10-11 03:00")
    result = simulate(
        shell, backups, start, start + 4 * HOUR + 1800, 300, FAKE_FAIL="pg_dump:warehouse"
    )
    assert result.returncode == 0, result.stderr
    assert attempts(result) == [start + hours * HOUR for hours in range(5)]
    assert result.stdout.count("the schedule tries again in an hour") == 5
    assert latest(backups)["result"] == "failed"


@pytest.mark.parametrize("shell", SHELLS)
def test_a_failed_run_is_tried_again_an_hour_later_also_after_midnight(shell, backups):
    # A time of the evening: the run of 23:30 fails, and the retry of 00:30 is of the day after.
    seed_latest(backups, "2026-10-10 23:30")
    result = simulate(
        shell,
        backups,
        utc_epoch("2026-10-11 23:00"),
        utc_epoch("2026-10-12 02:00"),
        900,
        BACKUP_TIME="23:30",
        FAKE_FAIL="pg_dump:warehouse",
        FAKE_FAIL_UNTIL=str(utc_epoch("2026-10-12 00:15")),
    )
    assert result.returncode == 0, result.stderr
    assert attempts(result) == [utc_epoch("2026-10-11 23:30"), utc_epoch("2026-10-12 00:30")]
    assert latest(backups)["result"] == "ok"
    assert latest(backups)["last_scheduled_local"] == "2026-10-12 00:30"


@pytest.mark.parametrize("shell", SHELLS)
def test_two_failed_runs_then_a_good_one_and_the_next_run_comes_at_the_time_of_the_day(
    shell, backups
):
    seed_latest(backups, "2026-10-10 03:00")
    result = simulate(
        shell,
        backups,
        utc_epoch("2026-10-11 02:00"),
        utc_epoch("2026-10-12 04:00"),
        900,
        FAKE_FAIL="pg_dump:warehouse",
        FAKE_FAIL_UNTIL=str(utc_epoch("2026-10-11 04:30")),
    )
    assert result.returncode == 0, result.stderr
    assert attempts(result) == [
        utc_epoch("2026-10-11 03:00"),  # fails
        utc_epoch("2026-10-11 04:00"),  # fails
        utc_epoch("2026-10-11 05:00"),  # good: the schedule is not pending any more
        utc_epoch("2026-10-12 03:00"),  # the time of the next day
    ]
    assert latest(backups)["result"] == "ok"


@pytest.mark.parametrize("shell", SHELLS)
def test_a_stop_during_a_backup_is_held_until_the_backup_ends(shell, backups):
    process = subprocess.Popen(
        [shell, str(SCRIPT), "schedule"],
        env=environment(backups, FAKE_SLOW="pg_dump:warehouse", FAKE_SLOW_SECONDS="2"),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        log = backups.parent / "tools.log"
        deadline = time.monotonic() + 20
        while not (log.exists() and "pg_dump" in log.read_text()) and time.monotonic() < deadline:
            time.sleep(0.02)
        process.send_signal(signal.SIGTERM)
        time.sleep(0.5)
        assert process.poll() is None  # the dump still runs, and the shell waits for its child
        assert not (backups / "latest.json").exists()
        assert process.wait(timeout=20) == 0  # it stops when the backup has ended
        assert latest(backups)["result"] == "ok"
        assert latest(backups)["kind"] == "scheduled"
    finally:
        process.kill()
        process.wait(timeout=10)


@pytest.mark.parametrize("shell", SHELLS)
def test_a_folder_without_a_readable_manifest_is_never_removed_by_the_retention(shell, backups):
    # A backup by hand whose manifest was lost, or came back from another machine with CRLF line
    # ends: only a manifest that says scheduled lets the retention remove a folder.
    (backups / "20260901T030000Z").mkdir(mode=0o700)
    crlf = backups / "20260902T030000Z"
    crlf.mkdir(mode=0o700)
    (crlf / "manifest.json").write_bytes(b'{\r\n  "kind": "manual",\r\n  "files": []\r\n}\r\n')
    result = run(
        shell,
        backups,
        "scheduled",
        BACKUP_KEEP_DAILY="1",
        BACKUP_KEEP_WEEKLY="0",
        BACKUP_KEEP_MONTHLY="0",
    )
    assert result.returncode == 0, result.stderr
    assert backup_names(backups) == ["20260901T030000Z", "20260902T030000Z", NAME]


@pytest.mark.parametrize("shell", SHELLS)
def test_a_lock_file_that_cannot_be_opened_stops_the_backup_with_one_line(shell, backups):
    lock = backups / ".lock"
    lock.touch()
    lock.chmod(0o400)
    result = run(shell, backups, "now")
    assert result.returncode == 1
    lines = result.stderr.splitlines()
    assert len(lines) == 1, result.stderr
    assert lines[0].startswith(f"backup: error: cannot open the lock file {lock}")
    assert "AGORANOMOS_UID" in lines[0]
    assert tool_calls(backups) == []


@pytest.mark.parametrize("shell", SHELLS)
def test_a_folder_that_cannot_be_made_is_recorded_as_a_failed_step(shell, backups):
    result = run(shell, backups, "now", FAKE_FAIL="mkdir")
    assert result.returncode == 1
    assert result.stderr.splitlines() == [
        f"backup: error: the backup {NAME} failed at: the folder of the backup"
    ]
    assert latest(backups)["result"] == "failed"
    assert latest(backups)["step"] == "the folder of the backup"
    assert not [call for call in tool_calls(backups) if call.startswith("pg_")]


@pytest.mark.parametrize("shell", SHELLS)
def test_an_old_partial_backup_that_cannot_be_removed_is_recorded_as_a_failed_step(shell, backups):
    stuck = backups / ".partial-20261001T030000Z" / "locked"
    stuck.mkdir(parents=True)
    (stuck / "file").write_text("x", encoding="utf-8")
    stuck.chmod(0o500)  # its file cannot be removed
    try:
        result = run(shell, backups, "now")
    finally:
        stuck.chmod(0o700)
    assert result.returncode == 1
    step = "the removal of the partial backups of earlier runs"
    assert result.stderr.splitlines()[-1] == f"backup: error: the backup {NAME} failed at: {step}"
    assert latest(backups)["step"] == step
    assert not [call for call in tool_calls(backups) if call.startswith("pg_")]


@pytest.mark.parametrize("shell", SHELLS)
def test_a_name_that_is_taken_stops_the_backup_and_leaves_that_folder_as_it_is(shell, backups):
    taken = backups / NAME
    taken.mkdir(mode=0o700)
    (taken / "keep").write_text("x", encoding="utf-8")
    result = run(shell, backups, "now")
    assert result.returncode == 1
    step = "the check that no backup has this name"
    assert result.stderr.splitlines() == [f"backup: error: the backup {NAME} failed at: {step}"]
    assert latest(backups)["step"] == step
    assert [path.name for path in taken.iterdir()] == ["keep"]
    assert not list(backups.glob(".partial-*"))


@pytest.mark.parametrize("shell", SHELLS)
def test_a_command_that_fails_inside_the_manifest_fails_its_step(shell, backups):
    # set -e is in effect in the functions that write the files, not only in the commands
    # that step runs: the failure of wc inside write_manifest is not swallowed by a pipeline.
    result = run(shell, backups, "now", FAKE_FAIL="wc")
    assert result.returncode == 1
    assert (
        result.stderr.splitlines()[-1]
        == f"backup: error: the backup {NAME} failed at: the manifest"
    )
    assert latest(backups)["step"] == "the manifest"
    assert backup_names(backups) == []
    assert not list(backups.glob(".partial-*"))


@pytest.mark.parametrize("shell", SHELLS)
def test_a_retention_that_cannot_remove_a_backup_warns_and_the_good_backup_stays_good(
    shell, backups
):
    stuck = old_backup(backups, NOW - 3 * DAY, "scheduled")
    locked = backups / stuck / "locked"
    locked.mkdir()
    (locked / "file").write_text("x", encoding="utf-8")
    locked.chmod(0o500)  # its file cannot be removed
    try:
        result = run(
            shell,
            backups,
            "scheduled",
            BACKUP_KEEP_DAILY="1",
            BACKUP_KEEP_WEEKLY="0",
            BACKUP_KEEP_MONTHLY="0",
        )
    finally:
        locked.chmod(0o700)
    assert result.returncode == 0, result.stderr
    assert f"the scheduled backup {NAME} is complete" in result.stdout
    assert f"backup: warning: could not remove the scheduled backup {stuck}" in result.stderr
    assert "backup: error" not in result.stderr
    assert latest(backups)["result"] == "ok"
    assert latest(backups)["last_ok"] == NAME


def test_usage(backups):
    result = run("/bin/sh", backups, "restore")
    assert result.returncode == 1
    assert "usage: backup.sh schedule | now | health | verify" in result.stderr
