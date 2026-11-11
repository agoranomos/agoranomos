"""watcher: what the DAG agoranomos_watcher checks every 15 minutes, which no failed task shows, and
the alerts that it sends of it through Apprise (ADR-0045):

- quarantine: each account and reason of ops.ops_quarantine, which the last run of the transform
  kept out of the core (ADR-0035), by the code of the reason, the platform and the account;
- late: each active account of a source whose last day that the core pinned, the largest
  window_to of its loaded loads in core.loads, is before the day that the lag of its platform
  expects: a day ends at midnight in the time zone of the account, and the core is to hold it
  LAG_DAYS later;
- backup: the status that the backup service of the ops profile publishes in the volume
  backup-status, as its health check judges it: backup_failed when its last run failed, and
  backup_late when its last good backup is more than 26 hours old, or there is none 26 hours after
  the service started. A status older than six hours means that no backup service runs, as when
  the ops profile is off, and nothing is checked;
- disk: disk_low when the disk of the volume of the logs of Airflow, the disk of the data of
  Docker, has less free space than ./agoranomos up needs to start, 10 GiB.

Each problem is a row of ops.alert_state, by its kind, its subject (the platform and the account, or
nothing) and its reason. The watcher sends one message for a run that finds something to say: the
problems that are new, those that have gone away, and once a day, when the last message about them
is a day old, every problem that remains. A message that does not go out leaves the rows as they
were, so the next run sends it again; a recovery that has not gone out in a day is dropped, and so
is a problem that went away before any message named it. A check that fails leaves the rows of its
kind as they were, and the task fails once the other checks have run. With no address in
AGORANOMOS_ALERT_URLS, the rows are kept and nothing is sent: the problems that remain go out with
the first run after an address is set.

A message holds codes, IDs and dates, never an amount and never text that came from a platform:
detail of ops.ops_quarantine, which holds what the check refused, is not read.

The watcher connects as transformer, which owns the schema ops and creates the table there on its
first run (table_sql): no role gets a new right. The table holds codes, IDs and times, and no
grant: metabase_reader and ai_reader read nothing of it.
"""

from __future__ import annotations

import json
import logging
import shutil
from collections.abc import Callable, Iterable
from contextlib import closing
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from agoranomos_pipelines import maintenance

log = logging.getLogger(__name__)

TABLE = "ops.alert_state"
# The codes of the reasons of quarantine, as quarantine_reasons() of dbt/macros/quarantine.sql
# lists them; tests/unit/test_watcher.py compares the two. Another code shows as quarantine.
QUARANTINE_REASONS = frozenset(
    {
        "load_failed",
        "unknown_account",
        "duplicate_key",
        "missing_key",
        "unknown_currency",
        "missing_rate",
        "unknown_time_zone",
        "too_many_decimals",
        "missing_measure",
        "negative_measure",
        "date_outside_window",
        "totals_differ",
        "empty_window",
        "spend_fall",
        "source_stopped",
    }
)
OTHER_QUARANTINE_REASON = "quarantine"
# How many days after a day ends the core is to hold it, by source: each source of the base
# package loads once a day, the day before in the time zone of each account, and the transform
# pins the load. An account is late when the core still lacks the day that ended this many days
# ago, so a load that missed one day shows within one to two days. A source of a deployment that
# this list does not name takes DEFAULT_LAG_DAYS.
LAG_DAYS = {
    "demo_fx": 1,
    "demo_search": 1,
    "demo_social": 1,
    "demo_video": 1,
    "demo_web": 1,
    "ecb": 1,
    "google_ads": 1,
}
DEFAULT_LAG_DAYS = 1
# The status of the backups that the backup service publishes (postgres/backup.sh), where
# compose.yaml mounts the volume backup-status in the Airflow services, read-only.
BACKUP_STATUS = Path("/opt/agoranomos/backup-status/status.json")
# As the health check of the backup service counts (postgres/backup.sh): a daily backup that is up
# to two hours late is still fresh.
BACKUP_FRESH = timedelta(hours=26)
# A status older than this says that no backup service runs.
BACKUP_STATUS_STALE = timedelta(hours=6)
# The free space below which ./agoranomos up stops (MIN_DISK_GB of the script), in GiB.
DISK_MIN_GIB = 10
# How often a problem that remains is named again, and how long a recovery tries to go out.
REMINDER = timedelta(days=1)
RECOVERY_TRIES = timedelta(days=1)
# How many problems a section of a message names; the rest it counts.
SECTION_LINES = 20
# A statement of the watcher waits at most this long for a lock, and runs at most this long, so
# that it never holds back a run of dbt, which waits a minute for a lock (postgres/warehouse.sql).
SESSION_OPTIONS = "-c lock_timeout=10s -c statement_timeout=30s"


@dataclass(frozen=True)
class Problem:
    """What a check found: its kind and reason, what it is about (a platform and an account, or
    nothing), since when, and a detail of dates and codes only."""

    kind: str
    subject: str
    reason: str
    since: datetime
    detail: str = ""

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.kind, self.subject, self.reason)


@dataclass(frozen=True)
class Row:
    """A row of ops.alert_state."""

    kind: str
    subject: str
    reason: str
    since: datetime
    notified_at: datetime | None
    recovered_at: datetime | None
    detail: str = ""

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.kind, self.subject, self.reason)


@dataclass
class Plan:
    """What a run does with the problems that it found and the rows that it holds: the rows to
    insert, those to mark recovered or to take back from a recovery, those to delete without a
    message, and the problems that its message names."""

    insert: list[Problem] = field(default_factory=list)
    recover: list[Row] = field(default_factory=list)
    revive: list[Row] = field(default_factory=list)
    forget: list[Row] = field(default_factory=list)
    new: list[Problem] = field(default_factory=list)
    still: list[Problem] = field(default_factory=list)
    recovered: list[Row] = field(default_factory=list)

    @property
    def says_something(self) -> bool:
        return bool(self.new or self.still or self.recovered)


def account_subject(platform: object, account_id: object) -> str:
    from agoranomos_alerts import plain

    return f"{plain(platform, 40)} {plain(account_id)}"


def quarantine_problems(rows: Iterable[tuple[Any, ...]], now: datetime) -> list[Problem]:
    """The problems of the rows of ops.ops_quarantine: (platform, account_id, reason, since)."""
    found = []
    for platform, account_id, reason, since in rows:
        code = reason if reason in QUARANTINE_REASONS else OTHER_QUARANTINE_REASON
        found.append(
            Problem("quarantine", account_subject(platform, account_id), code, since or now)
        )
    return found


def local_today(now: datetime, time_zone: object) -> date:
    """The day of now in the time zone of an account, in UTC when it has none that zoneinfo
    knows."""
    try:
        zone = ZoneInfo(str(time_zone)) if time_zone else UTC
    except (ZoneInfoNotFoundError, ValueError):
        zone = UTC
    return now.astimezone(zone).date()


def expected_day(source: str, today: date) -> date:
    """The last day that the core is to hold for an account of a source, on a day of the account:
    the day that ended LAG_DAYS ago."""
    return today - timedelta(days=1 + LAG_DAYS.get(source, DEFAULT_LAG_DAYS))


def late_problems(rows: Iterable[tuple[Any, ...]], now: datetime) -> list[Problem]:
    """The late accounts among the rows (source, account_id, last day pinned, time zone)."""
    found = []
    for source, account_id, last_day, time_zone in rows:
        expected = expected_day(str(source), local_today(now, time_zone))
        if last_day is not None and last_day < expected:
            found.append(
                Problem(
                    "late",
                    account_subject(source, account_id),
                    "late",
                    now,
                    f"last day {last_day.isoformat()}, expected {expected.isoformat()}",
                )
            )
    return found


def epoch(value: object) -> datetime | None:
    """A number of seconds since 1970 as a time, or None for anything else and for 0."""
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        return None
    return datetime.fromtimestamp(value, UTC)


def read_backup_status(path: Path = BACKUP_STATUS) -> dict[str, Any] | None:
    """The status that the backup service published, or None when there is none to read."""
    try:
        status = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError):
        return None
    return status if isinstance(status, dict) else None


def backup_problems(status: dict[str, Any] | None, now: datetime) -> list[Problem]:
    """The problems of the backups, as the health check of the backup service judges them, while
    the service publishes its status."""
    if status is None:
        return []
    checked = epoch(status.get("checked_epoch"))
    if checked is None or now - checked > BACKUP_STATUS_STALE:
        return []
    found = []
    last_ok = epoch(status.get("last_ok_epoch"))
    if status.get("result") == "failed":
        found.append(Problem("backup", "", "backup_failed", now, "the last run failed"))
    if last_ok is not None:
        if now - last_ok > BACKUP_FRESH:
            detail = f"last good backup {last_ok.strftime('%Y-%m-%d %H:%M UTC')}"
            found.append(Problem("backup", "", "backup_late", now, detail))
    else:
        started = epoch(status.get("started_epoch"))
        if started is not None and now - started > BACKUP_FRESH:
            found.append(Problem("backup", "", "backup_late", now, "no good backup yet"))
    return found


def disk_problems(free_bytes: int, now: datetime) -> list[Problem]:
    if free_bytes < DISK_MIN_GIB * 1024**3:
        detail = f"less than {DISK_MIN_GIB} GiB free on the disk of Docker"
        return [Problem("disk", "", "disk_low", now, detail)]
    return []


def plan(current: Iterable[Problem], rows: Iterable[Row], checked: set[str], now: datetime) -> Plan:
    """What a run does, from the problems that the checks found, the rows of ops.alert_state, and
    the kinds whose check ran: a row of a kind whose check failed stays as it is."""
    found = {problem.key: problem for problem in current}
    held = {row.key: row for row in rows}
    result = Plan()
    remaining = []
    for key, problem in found.items():
        row = held.get(key)
        if row is None:
            result.insert.append(problem)
            result.new.append(problem)
            continue
        if row.recovered_at is not None:
            result.revive.append(row)
        if row.notified_at is None:
            result.new.append(Problem(row.kind, row.subject, row.reason, row.since, problem.detail))
        else:
            remaining.append((row, problem))
    if remaining and min(row.notified_at for row, _ in remaining) <= now - REMINDER:
        result.still = [
            Problem(row.kind, row.subject, row.reason, row.since, problem.detail)
            for row, problem in remaining
        ]
    for key, row in held.items():
        if key in found or row.kind not in checked:
            continue
        if row.notified_at is None:
            result.forget.append(row)
        elif row.recovered_at is None:
            result.recover.append(row)
            result.recovered.append(row)
        elif row.recovered_at <= now - RECOVERY_TRIES:
            result.forget.append(row)
        else:
            result.recovered.append(row)
    return result


def line(problem: Problem | Row, at: str) -> str:
    """One line of a message: what, about what, and since or at when."""
    from agoranomos_alerts import moment

    words = [f"{problem.kind} {problem.reason}" if problem.kind == "quarantine" else problem.reason]
    if problem.subject:
        words.append(problem.subject)
    when = problem.since if at == "since" else problem.recovered_at
    text = f"- {', '.join(words)}, {at} {moment(when)}"
    detail = getattr(problem, "detail", "")
    return f"{text} ({detail})" if detail and at == "since" else text


def section(heading: str, items: list[Problem] | list[Row], at: str) -> list[str]:
    if not items:
        return []
    ordered = sorted(items, key=lambda item: (item.kind, item.subject, item.reason))
    lines = [heading, *(line(item, at) for item in ordered[:SECTION_LINES])]
    if len(ordered) > SECTION_LINES:
        lines.append(f"- and {len(ordered) - SECTION_LINES} more")
    return [*lines, ""]


def count(number: int, word: str) -> str:
    return f"{number} {word}" if number == 1 else f"{number} {word}s"


def message(result: Plan, now: datetime) -> tuple[str, str, str]:
    """The title, the body and the kind of the message of a run."""
    from agoranomos_alerts import FOOTER

    recovered = [
        Row(r.kind, r.subject, r.reason, r.since, r.notified_at, r.recovered_at or now, r.detail)
        for r in result.recovered
    ]
    parts = []
    if result.new:
        parts.append(count(len(result.new), "new problem"))
    if result.still:
        parts.append(f"{count(len(result.still), 'problem')} still open")
    if recovered:
        parts.append(f"{count(len(recovered), 'problem')} gone")
    title = "agoranomos: " + ", ".join(parts)
    body = [
        *section("New:", result.new, "since"),
        *section("Still open:", result.still, "since"),
        *section("Gone:", recovered, "at"),
        FOOTER,
    ]
    kind = "failure" if result.new else "warning" if result.still else "success"
    return title, "\n".join(body), kind


TABLE_SQL = (
    f"CREATE TABLE IF NOT EXISTS {TABLE} ("
    " kind text NOT NULL,"
    " subject text NOT NULL,"
    " reason text NOT NULL,"
    " since timestamptz NOT NULL,"
    " notified_at timestamptz,"
    " recovered_at timestamptz,"
    " PRIMARY KEY (kind, subject, reason))",
    f"COMMENT ON TABLE {TABLE} IS 'The problems that the DAG agoranomos_watcher sees and the"
    " alerts that it sent about them (ADR-0045): one row for each kind, subject and reason. The"
    " watcher writes it as transformer. It holds codes, IDs and times only.'",
    f"COMMENT ON COLUMN {TABLE}.kind IS 'What the check looks at: quarantine, late, backup or"
    " disk.'",
    f"COMMENT ON COLUMN {TABLE}.subject IS 'The platform and the ID of the account, separated by"
    " a space, or empty for the backups and the disk.'",
    f"COMMENT ON COLUMN {TABLE}.reason IS 'The code of the reason, such as unknown_currency,"
    " late, backup_late or disk_low.'",
    f"COMMENT ON COLUMN {TABLE}.since IS 'When the problem began, as the journal of quarantine"
    " says for an account in quarantine, and else when the watcher first saw it.'",
    f"COMMENT ON COLUMN {TABLE}.notified_at IS 'When the last alert that named the problem went"
    " out; none while none has gone out.'",
    f"COMMENT ON COLUMN {TABLE}.recovered_at IS 'When the watcher found the problem gone, while"
    " the alert of that has not gone out; none while the problem remains.'",
)

QUARANTINE_SQL = "SELECT platform, account_id, reason, since FROM ops.ops_quarantine"
LATE_SQL = (
    "SELECT l.source, l.account_id, max(l.window_to), a.time_zone"
    " FROM core.loads AS l"
    " LEFT JOIN core.dim_account AS a ON a.platform = l.source AND a.account_id = l.account_id"
    " WHERE l.outcome = 'loaded' AND coalesce(a.is_active, true)"
    " GROUP BY l.source, l.account_id, a.time_zone"
)


def exists(cursor: Any, relation: str) -> bool:
    cursor.execute("SELECT to_regclass(%s) IS NOT NULL", (relation,))
    return bool(cursor.fetchone()[0])


def rows_of(cursor: Any, relations: tuple[str, ...], sql: str) -> list[tuple[Any, ...]]:
    """The rows of a query, or none while one of the relations that it reads does not exist yet,
    before the first run of the transform has built it."""
    if not all(exists(cursor, relation) for relation in relations):
        return []
    cursor.execute(sql)
    return list(cursor.fetchall())


def held_rows(cursor: Any) -> list[Row]:
    cursor.execute(
        f"SELECT kind, subject, reason, since, notified_at, recovered_at FROM {TABLE}"  # noqa: S608 (a constant)
    )
    return [Row(*values) for values in cursor.fetchall()]


def watch(
    connect: Callable[..., Any] | None = None,
    sender: Callable[[str, str, str], bool | None] | None = None,
    now: datetime | None = None,
    logs: str = maintenance.LOGS,
    backup_status: Path = BACKUP_STATUS,
) -> Plan:
    """One run of the watcher: the checks, the rows of ops.alert_state, the message and the rows
    again, in a transaction before the message and one after it. Raises RuntimeError at the end
    when a check failed."""
    if connect is None:
        import psycopg2

        connect = psycopg2.connect
    if sender is None:
        from agoranomos_alerts import send as sender
    connection = connect(
        **maintenance.transformer_credentials(),
        connect_timeout=10,
        application_name="agoranomos_watcher",
        options=SESSION_OPTIONS,
    )
    failed: list[str] = []
    with closing(connection):
        with connection, connection.cursor() as cursor:
            if not exists(cursor, TABLE):
                for statement in TABLE_SQL:
                    cursor.execute(statement)
            cursor.execute("SELECT now()")
            now = now or cursor.fetchone()[0]
        checks: dict[str, Callable[[Any], list[Problem]]] = {
            "quarantine": lambda cursor: quarantine_problems(
                rows_of(cursor, ("ops.ops_quarantine",), QUARANTINE_SQL), now
            ),
            "late": lambda cursor: late_problems(
                rows_of(cursor, ("core.loads", "core.dim_account"), LATE_SQL), now
            ),
            "backup": lambda _: backup_problems(read_backup_status(backup_status), now),
            "disk": lambda _: disk_problems(shutil.disk_usage(logs).free, now),
        }
        found: list[Problem] = []
        checked: set[str] = set()
        for kind, check in checks.items():
            try:
                with connection, connection.cursor() as cursor:
                    found += check(cursor)
                checked.add(kind)
            except Exception as error:
                failed.append(kind)
                log.warning("The check %s failed: %s", kind, type(error).__name__)
        with connection, connection.cursor() as cursor:
            result = plan(found, held_rows(cursor), checked, now)
            for problem in result.insert:
                cursor.execute(
                    f"INSERT INTO {TABLE} (kind, subject, reason, since)"  # noqa: S608 (a constant)
                    " VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                    (problem.kind, problem.subject, problem.reason, problem.since),
                )
            for row, value in [(row, now) for row in result.recover] + [
                (row, None) for row in result.revive
            ]:
                cursor.execute(
                    f"UPDATE {TABLE} SET recovered_at = %s"  # noqa: S608 (a constant)
                    " WHERE kind = %s AND subject = %s AND reason = %s",
                    (value, *row.key),
                )
            delete(cursor, result.forget)
        log.info(
            "The watcher found %d problems: %d new, %d still open, %d gone",
            len(found),
            len(result.new),
            len(result.still),
            len(result.recovered),
        )
        if result.says_something:
            title, body, kind = message(result, now)
            outcome = sender(title, body, kind)
            if outcome is None:
                log.info("AGORANOMOS_ALERT_URLS is empty: no alert was sent")
            elif outcome:
                with connection, connection.cursor() as cursor:
                    for problem in [*result.new, *result.still]:
                        cursor.execute(
                            f"UPDATE {TABLE} SET notified_at = %s"  # noqa: S608 (a constant)
                            " WHERE kind = %s AND subject = %s AND reason = %s",
                            (now, *problem.key),
                        )
                    delete(cursor, result.recovered)
            else:
                log.warning("The alert did not go out: the next run sends it again")
    if failed:
        raise RuntimeError(f"these checks of the watcher failed: {', '.join(failed)}")
    return result


def delete(cursor: Any, rows: Iterable[Row]) -> None:
    for row in rows:
        cursor.execute(
            f"DELETE FROM {TABLE} WHERE kind = %s AND subject = %s AND reason = %s",  # noqa: S608 (a constant)
            row.key,
        )
