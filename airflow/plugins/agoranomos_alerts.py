"""A plugin of Airflow that sends an alert through Apprise when a run of a DAG fails, and the code
of sending that the DAG agoranomos_watcher shares (ADR-0045).

The addresses are AGORANOMOS_ALERT_URLS of .env, one or more addresses of Apprise, such as
tgram://<token>/<chat>, separated by spaces or commas. Only the scheduler and the service that runs
the tasks get it (compose.yaml), and with no value nothing is sent and Apprise is not imported. The
addresses are secrets: no line of this module names one, the log of Apprise goes through a filter
that puts *** in place of every address and of every long part of one, and a task masks them in its
log through Airflow (mask_addresses).

The listener on_dag_run_failed runs in the scheduler, which calls it when a run of any DAG ends
failed, those of extensions/ too, also when the kernel killed the process of a task, which a
callback of the task does not see (ADR-0045). It never waits for the network: it puts the message
on a queue, and one thread of the scheduler sends the messages one at a time, each address within
the timeouts of Apprise, 4 seconds to connect and 4 to read, which an address can change with cto=
and rto=. Apprise is imported, and the addresses read, once, in the main thread, at the first
failed run (prepared). A full queue drops the message and says so in the log. A DAG that fails again within an
hour of its last message sends no new one: the next message after the hour counts the failures that
it held back. The listener does nothing in the other processes of Airflow, such as the API server,
which calls it for a run that a person marks failed and holds no address.

A message holds codes, IDs and times, never an amount and never text of a platform, which anyone
can write: the reason of a failed run as Airflow gives it (task_failure, timed_out or
all_tasks_deadlocked), the ID of the DAG, and when the run ended. An ID that is not plain, letters,
digits, dots, hyphens and underscores, is not shown.

The image installs this folder where only root may write (airflow/Dockerfile), and Airflow puts it
on sys.path in each of its processes, so the task of the watcher imports this module by name.
"""

from __future__ import annotations

import logging
import os
import queue
import re
import sys
import threading
from datetime import UTC, datetime, timedelta
from typing import ClassVar

from airflow.listeners import hookimpl
from airflow.plugins_manager import AirflowPlugin

log = logging.getLogger(__name__)

# The setting of .env with the addresses of Apprise.
SETTING = "AGORANOMOS_ALERT_URLS"
# What Airflow passes as msg to on_dag_run_failed from the scheduler, as of Airflow 3.3.2
# (airflow/models/dagrun.py and airflow/jobs/scheduler_job_runner.py): the reasons that a message
# names. Another reason shows as dag_run_failed.
RUN_REASONS = frozenset({"task_failure", "timed_out", "all_tasks_deadlocked"})
OTHER_RUN_REASON = "dag_run_failed"
# How long a DAG that failed sends no second message, and how many messages wait for the thread.
QUIET = timedelta(hours=1)
QUEUE_SIZE = 50
# What a message shows in place of an ID that is not plain.
NOT_SHOWN = "(an ID that is not shown)"
PLAIN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*")
# Where one address of a list ends: spaces or commas before the scheme of the next, as Apprise
# splits a list (URL_DETECTION_RE of apprise/utils/parse.py, Apprise 1.13.1), so that a comma
# inside an address, such as in a list of recipients, stays in it.
SEPARATOR = re.compile(r"[\s,]+(?=[A-Za-z][A-Za-z0-9+.-]{0,31}://)")
# The parts of an address between its delimiters that the filter of the log of Apprise also hides:
# tokens, chat IDs, hosts and paths of four characters or more, as Apprise names a chat or a host
# in a line that says that a send failed.
PART = re.compile(r"[^\s/:@?&=#,;]{4,}")
# The kinds of a message, as Apprise names its types of notification.
KINDS = {"failure": "failure", "warning": "warning", "success": "success", "info": "info"}
FOOTER = (
    "Airflow shows the run and the logs of its tasks, and Pipeline health, in the collection "
    "agoranomos / Ops of Metabase, the state of the pipeline."
)


def plain(value: object, limit: int = 64) -> str:
    """An ID as a message may show it: plain text of at most limit characters, or NOT_SHOWN."""
    text = "" if value is None else str(value)
    if len(text) <= limit and PLAIN.fullmatch(text):
        return text
    return NOT_SHOWN


def moment(value: datetime | None) -> str:
    """A time as a message shows it, to the minute, in UTC: 2026-11-07 09:30 UTC."""
    when = value if value is not None else datetime.now(UTC)
    if when.tzinfo is None:
        when = when.replace(tzinfo=UTC)
    return when.astimezone(UTC).strftime("%Y-%m-%d %H:%M UTC")


def setting() -> str:
    """The addresses of .env as one text, empty when there are none."""
    return os.environ.get(SETTING, "").strip()


def addresses(value: str) -> list[str]:
    """The addresses of a value of the setting, one by one."""
    found = []
    for part in SEPARATOR.split(value.strip()):
        address = part.strip(" \t\r\n,")
        if address:
            found.append(address)
    return found


def secret_parts(value: str) -> list[str]:
    """What the log must not show of a value of the setting: the value, each address, and each
    long part of an address after its scheme, the longest first."""
    parts = {value.strip()}
    for address in addresses(value):
        parts.add(address)
        parts.update(PART.findall(address.split("://", 1)[-1]))
    return sorted((part for part in parts if part), key=len, reverse=True)


class Redact(logging.Filter):
    """The filter of the log of Apprise: *** in place of each secret part, and no traceback, whose
    exceptions can name an address."""

    def __init__(self, parts: list[str]) -> None:
        super().__init__()
        self.parts = parts

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            message = record.getMessage()
        except Exception:
            message = str(record.msg)
        redacted = message
        for part in self.parts:
            redacted = redacted.replace(part, "***")
        record.msg, record.args = redacted, None
        record.exc_info, record.exc_text, record.stack_info = None, None, None
        return True


GUARD_LOCK = threading.Lock()
GUARDED: dict[str, Redact] = {}


def guard_the_log(value: str) -> None:
    """Hide the addresses from the log of this process, once for each value: the filter on the
    logger of Apprise, which all its plugins write to, and the debug lines of Apprise and of
    urllib3 off, which name the address that they send to. Airflow masks them in the log of a
    task as well (mask_addresses)."""
    with GUARD_LOCK:
        if value in GUARDED:
            return
        apprise_log = logging.getLogger("apprise")
        for old in GUARDED.values():
            apprise_log.removeFilter(old)
        GUARDED.clear()
        redact = Redact(secret_parts(value))
        apprise_log.addFilter(redact)
        GUARDED[value] = redact
        if apprise_log.getEffectiveLevel() < logging.INFO:
            apprise_log.setLevel(logging.INFO)
        urllib3_log = logging.getLogger("urllib3")
        if urllib3_log.getEffectiveLevel() < logging.WARNING:
            urllib3_log.setLevel(logging.WARNING)


def mask_addresses() -> None:
    """In a task: have Airflow mask the setting and each address in the log of the task. Outside
    Airflow, or without a value, it does nothing."""
    value = setting()
    if not value:
        return
    try:
        from airflow.sdk.log import mask_secret
    except ImportError:
        return
    for part in [value, *addresses(value)]:
        mask_secret(part)


PREPARE_LOCK = threading.Lock()
PREPARED: dict[str, object] = {}


def prepared(value: str) -> object:
    """The Apprise of the addresses of a value, made once in a process: the import of Apprise and
    the reading of the addresses, which loads the plugins of Apprise. The listener has it made in
    the main thread of the scheduler, before the first message goes to the thread that sends: the
    scheduler forks its workers from the main thread, and a fork while another thread imports
    could leave a child with the lock of an import that never ends."""
    with PREPARE_LOCK:
        if value not in PREPARED:
            guard_the_log(value)
            import apprise

            alerts = apprise.Apprise(
                asset=apprise.AppriseAsset(app_id="agoranomos", app_desc="agoranomos")
            )
            alerts.add(value)
            PREPARED[value] = alerts
        return PREPARED[value]


def send(title: str, body: str, kind: str = "failure") -> bool | None:
    """Send one message to every address of the setting through Apprise, and wait for it: True
    when every address took it, False when one did not or none could be read, None when the
    setting is empty and nothing was sent. The log names no address and no error of Apprise in
    full, only how many addresses there are and the class of an exception."""
    value = setting()
    if not value:
        return None
    alerts = prepared(value)
    count = len(addresses(value))
    if len(alerts) == 0:
        log.warning(
            "%s holds no address that Apprise can read (%d given): no alert was sent",
            SETTING,
            count,
        )
        return False
    if len(alerts) < count:
        log.warning(
            "Apprise could read %d of the %d addresses of %s: the others get no alert",
            len(alerts),
            count,
            SETTING,
        )
    try:
        outcome = alerts.notify(
            body=body,
            title=title,
            notify_type=KINDS.get(kind, "info"),
            body_format="text",
        )
    except Exception as error:
        log.warning("Apprise failed to send an alert: %s", type(error).__name__)
        return False
    if outcome:
        log.info("Sent an alert to %d addresses: %s", len(alerts), title)
        return True
    log.warning("An alert did not reach every address of %s: %s", SETTING, title)
    return False


class Sender:
    """The thread of a process that sends the messages of the listener, one at a time, from a queue,
    so that the listener never waits for the network. A child that the process forks starts a
    thread and a queue of its own: threads do not cross a fork."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.jobs: queue.Queue[tuple[str, str, str] | None] | None = None
        self.thread: threading.Thread | None = None
        self.pid: int | None = None

    def submit(self, title: str, body: str, kind: str) -> bool:
        """Put a message on the queue, and start the thread if it is not running; False when the
        queue is full and the message is dropped."""
        with self.lock:
            if self.pid != os.getpid() or self.thread is None or not self.thread.is_alive():
                self.jobs = queue.Queue(maxsize=QUEUE_SIZE)
                self.thread = threading.Thread(
                    target=self.run, args=(self.jobs,), name="agoranomos-alerts", daemon=True
                )
                self.pid = os.getpid()
                self.thread.start()
            try:
                self.jobs.put_nowait((title, body, kind))
            except queue.Full:
                log.warning("The queue of alerts is full: an alert was dropped: %s", title)
                return False
            return True

    def stop(self, timeout: float = 5) -> None:
        """End the thread once it has sent what waits, for the tests: the scheduler never stops
        it, and it ends with the process."""
        with self.lock:
            jobs, thread = self.jobs, self.thread
        if jobs is not None and thread is not None and thread.is_alive():
            jobs.put(None, timeout=timeout)
            thread.join(timeout)

    @staticmethod
    def run(jobs: queue.Queue[tuple[str, str, str] | None]) -> None:
        while True:
            job = jobs.get()
            if job is None:
                jobs.task_done()
                return
            title, body, kind = job
            try:
                send(title, body, kind)
            except Exception as error:
                log.warning("Could not send an alert (%s): %s", type(error).__name__, title)
            finally:
                jobs.task_done()


def is_scheduler() -> bool:
    """Whether this process is the scheduler of Airflow, airflow scheduler."""
    return sys.argv[1:2] == ["scheduler"]


def failed_run_message(
    dag_id: object, reason: object, ended: datetime | None, held_back: int = 0
) -> tuple[str, str]:
    """The title and the body of the message of a failed run of a DAG."""
    code = str(reason) if reason in RUN_REASONS else OTHER_RUN_REASON
    lines = [f"failed: {code}, DAG {plain(dag_id, 100)}, at {moment(ended)}"]
    if held_back:
        lines.append(
            f"The DAG failed {held_back} more times in the hour before, with no message of each."
        )
    lines += ["", FOOTER]
    return "agoranomos: a run of a DAG failed", "\n".join(lines)


class FailedRuns:
    """The listener: Airflow calls on_dag_run_failed when a run of a DAG ends failed."""

    def __init__(self) -> None:
        self.sender = Sender()
        # By DAG: when its last message went to the queue, and how many failures came since.
        self.recent: dict[str, tuple[datetime, int]] = {}

    @hookimpl
    def on_dag_run_failed(self, dag_run: object, msg: str) -> None:
        try:
            self.alert(dag_run, msg)
        except Exception as error:
            # The scheduler goes on: only the alert is lost.
            log.warning("Could not prepare the alert of a failed run (%s)", type(error).__name__)

    def alert(self, dag_run: object, msg: str, now: datetime | None = None) -> None:
        if not is_scheduler() or not setting():
            return
        now = now or datetime.now(UTC)
        dag_id = str(getattr(dag_run, "dag_id", ""))
        last, held_back = self.recent.get(dag_id, (None, 0))
        if last is not None and now - last < QUIET:
            self.recent[dag_id] = (last, held_back + 1)
            return
        title, body = failed_run_message(
            dag_id, msg, getattr(dag_run, "end_date", None) or now, held_back
        )
        # Here, in the main thread, the first time: see prepared.
        prepared(setting())
        if self.sender.submit(title, body, "failure"):
            self.recent[dag_id] = (now, 0)


class AgoranomosAlertsPlugin(AirflowPlugin):
    name = "agoranomos_alerts"
    listeners: ClassVar[list[object]] = [FailedRuns()]
