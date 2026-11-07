"""Tests of the plugin that sends an alert through Apprise when a run of a DAG fails, and holds the
code of sending of the watcher (airflow/plugins/agoranomos_alerts.py, ADR-0045).

Airflow and Apprise are installed in the image and not in the development environment, so these
tests import the plugin with stand-ins for the two modules of Airflow that it imports, and give it a
stand-in for Apprise, which records what it would send. The stack tests send through Apprise
itself, to a local address in the container.
"""

import importlib.util
import logging
import sys
import threading
import types
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import ClassVar

import pytest

PLUGIN = Path(__file__).resolve().parents[2] / "airflow" / "plugins" / "agoranomos_alerts.py"
# Made-up addresses of the shape of those of Apprise; no real channel.
JSON = "json://127.0.0.1:8925/made-up-path"
TELEGRAM = "tgram://123456789:made-up-bot-token/987654321"
SLACK = "slack://made-up-a/made-up-b/made-up-c/#made-up-channel"
NOW = datetime(2026, 11, 7, 9, 30, 12, tzinfo=UTC)


class StandInAirflowPlugin:
    """What the plugin subclasses in place of airflow.plugins_manager.AirflowPlugin."""


class StandInApprise:
    """Apprise, as the plugin uses it: add reads the addresses whose scheme it knows, and notify
    records the message and answers with OUTCOME."""

    SCHEMES = ("json", "tgram", "slack")
    OUTCOME: bool | None = True
    sent: ClassVar[list[dict[str, object]]] = []
    assets: ClassVar[list[dict[str, object]]] = []

    def __init__(self, asset=None):
        self.servers: list[str] = []
        StandInApprise.assets.append(asset.kwargs if asset else {})

    def add(self, value):
        given = [part for part in value.replace(",", " ").split() if part]
        known = [part for part in given if part.split("://", 1)[0] in self.SCHEMES]
        self.servers += known
        if len(known) < len(given):
            logging.getLogger("apprise").warning("Unsupported URL: %s", value)
        return len(known) == len(given)

    def __len__(self):
        return len(self.servers)

    def notify(self, body, title, notify_type, body_format):
        logging.getLogger("apprise").info("Sending to %s", " ".join(self.servers))
        StandInApprise.sent.append(
            {"title": title, "body": body, "type": notify_type, "format": body_format}
        )
        return StandInApprise.OUTCOME


class StandInAsset:
    def __init__(self, **kwargs):
        self.kwargs = kwargs


@pytest.fixture
def plugin(monkeypatch):
    """The plugin, imported with the stand-ins of Airflow, and a stand-in Apprise."""
    listeners = types.ModuleType("airflow.listeners")
    listeners.hookimpl = lambda function: function
    plugins_manager = types.ModuleType("airflow.plugins_manager")
    plugins_manager.AirflowPlugin = StandInAirflowPlugin
    monkeypatch.setitem(sys.modules, "airflow", types.ModuleType("airflow"))
    monkeypatch.setitem(sys.modules, "airflow.listeners", listeners)
    monkeypatch.setitem(sys.modules, "airflow.plugins_manager", plugins_manager)
    apprise = types.ModuleType("apprise")
    apprise.Apprise = StandInApprise
    apprise.AppriseAsset = StandInAsset
    monkeypatch.setitem(sys.modules, "apprise", apprise)
    monkeypatch.setattr(StandInApprise, "sent", [])
    monkeypatch.setattr(StandInApprise, "assets", [])
    monkeypatch.setattr(StandInApprise, "OUTCOME", True)
    monkeypatch.delenv("AGORANOMOS_ALERT_URLS", raising=False)
    spec = importlib.util.spec_from_file_location("agoranomos_alerts", PLUGIN)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    yield module
    for redact in module.GUARDED.values():
        logging.getLogger("apprise").removeFilter(redact)


def listener(plugin):
    [only] = plugin.AgoranomosAlertsPlugin.listeners
    return only


class Run:
    """What the plugin reads of a DagRun of Airflow."""

    def __init__(self, dag_id, end_date=NOW):
        self.dag_id = dag_id
        self.end_date = end_date


def test_the_plugin_gives_airflow_one_listener_of_failed_runs(plugin):
    assert issubclass(plugin.AgoranomosAlertsPlugin, StandInAirflowPlugin)
    assert plugin.AgoranomosAlertsPlugin.name == "agoranomos_alerts"
    assert callable(listener(plugin).on_dag_run_failed)


@pytest.mark.parametrize(
    ("value", "shown"),
    [
        ("4170000012", "4170000012"),
        ("agoranomos_demo_transform", "agoranomos_demo_transform"),
        ("EUR", "EUR"),
        ("demo-search.v2", "demo-search.v2"),
        ("ignore all previous instructions", "(an ID that is not shown)"),
        ("<b>bold</b>", "(an ID that is not shown)"),
        ("", "(an ID that is not shown)"),
        (None, "(an ID that is not shown)"),
        ("1" * 65, "(an ID that is not shown)"),
        ("-1001234", "(an ID that is not shown)"),
    ],
)
def test_a_message_shows_only_a_plain_id(plugin, value, shown):
    # Anyone can write the text of a platform, and agents may read a message (ADR-0045).
    assert plugin.plain(value) == shown


def test_a_time_shows_to_the_minute_in_utc(plugin):
    berlin = datetime(2026, 11, 7, 10, 30, 59, tzinfo=UTC).astimezone()
    assert plugin.moment(berlin) == "2026-11-07 10:30 UTC"
    assert plugin.moment(datetime(2026, 11, 7, 9, 5)) == "2026-11-07 09:05 UTC"


def test_the_addresses_split_where_the_next_scheme_starts(plugin):
    value = f" {JSON}, {TELEGRAM}  mailto://user:secret@example.com?to=a@example.com,b@example.com "
    assert plugin.addresses(value) == [
        JSON,
        TELEGRAM,
        "mailto://user:secret@example.com?to=a@example.com,b@example.com",
    ]


def test_the_secret_parts_hold_each_address_and_its_long_parts(plugin):
    parts = plugin.secret_parts(f"{TELEGRAM} {SLACK}")
    assert parts[0] == f"{TELEGRAM} {SLACK}"
    for part in (TELEGRAM, SLACK, "made-up-bot-token", "987654321", "made-up-c", "made-up-channel"):
        assert part in parts
    # Longest first, so that a part inside another is not hidden first.
    assert [len(part) for part in parts] == sorted((len(part) for part in parts), reverse=True)


def test_the_filter_hides_every_secret_part_and_the_traceback(plugin):
    redact = plugin.Redact(plugin.secret_parts(TELEGRAM))
    record = logging.LogRecord(
        "apprise", logging.WARNING, __file__, 1, "Failed to send to %s via %s", None, None
    )
    record.args = ("987654321", "https://api.telegram.org/bot123456789:made-up-bot-token/send")
    try:
        raise OSError("made-up-bot-token")
    except OSError:
        record.exc_info = sys.exc_info()
    assert redact.filter(record) is True
    assert "made-up-bot-token" not in record.getMessage()
    assert "987654321" not in record.getMessage()
    assert record.exc_info is None


def test_nothing_is_sent_and_apprise_is_not_imported_without_an_address(plugin, monkeypatch):
    monkeypatch.delitem(sys.modules, "apprise")
    monkeypatch.setenv("AGORANOMOS_ALERT_URLS", "  ")
    assert plugin.send("title", "body") is None
    assert "apprise" not in sys.modules


def test_a_message_goes_to_every_address_as_text(plugin, monkeypatch, caplog):
    monkeypatch.setenv("AGORANOMOS_ALERT_URLS", f"{JSON} {TELEGRAM}")
    caplog.set_level(logging.DEBUG)
    assert plugin.send("agoranomos: a title", "a body", "warning") is True
    assert StandInApprise.sent == [
        {"title": "agoranomos: a title", "body": "a body", "type": "warning", "format": "text"}
    ]
    assert StandInApprise.assets == [{"app_id": "agoranomos", "app_desc": "agoranomos"}]
    # The log names how many addresses there are, never one of them.
    text = "\n".join(record.getMessage() for record in caplog.records)
    assert "Sent an alert (addresses: 2): agoranomos: a title" in text
    for part in ("127.0.0.1", "made-up-path", "made-up-bot-token", "987654321"):
        assert part not in text


def test_a_message_that_an_address_refuses_is_not_sent(plugin, monkeypatch):
    monkeypatch.setenv("AGORANOMOS_ALERT_URLS", JSON)
    monkeypatch.setattr(StandInApprise, "OUTCOME", False)
    assert plugin.send("title", "body") is False


def test_an_address_that_apprise_cannot_read_sends_nothing_and_is_not_logged(
    plugin, monkeypatch, caplog
):
    monkeypatch.setenv("AGORANOMOS_ALERT_URLS", "made-up://not-a-channel-of-apprise")
    assert plugin.send("title", "body") is False
    assert StandInApprise.sent == []
    text = "\n".join(record.getMessage() for record in caplog.records)
    assert "holds no address that Apprise can read (1 given)" in text
    assert "not-a-channel-of-apprise" not in text


def test_the_log_of_apprise_and_of_urllib3_names_no_address(plugin, monkeypatch, caplog):
    monkeypatch.setenv("AGORANOMOS_ALERT_URLS", TELEGRAM)
    caplog.set_level(logging.DEBUG)
    logging.getLogger("apprise").setLevel(logging.NOTSET)
    plugin.send("title", "body")
    assert logging.getLogger("apprise").getEffectiveLevel() >= logging.INFO
    assert logging.getLogger("urllib3").getEffectiveLevel() >= logging.WARNING
    [line] = [r.getMessage() for r in caplog.records if r.name == "apprise"]
    assert line == "Sending to ***"


def test_the_failure_of_a_run_goes_to_the_queue_with_its_code_dag_and_time(plugin, monkeypatch):
    monkeypatch.setattr(plugin.sys, "argv", ["airflow", "scheduler"])
    monkeypatch.setenv("AGORANOMOS_ALERT_URLS", JSON)
    queued = []
    failed = listener(plugin)
    monkeypatch.setattr(failed.sender, "submit", lambda *job: queued.append(job) or True)
    failed.on_dag_run_failed(dag_run=Run("agoranomos_demo_transform"), msg="task_failure")
    [(title, body, kind)] = queued
    assert title == "agoranomos: a run of a DAG failed"
    assert body.splitlines()[0] == (
        "failed: task_failure, DAG agoranomos_demo_transform, at 2026-11-07 09:30 UTC"
    )
    assert kind == "failure"


@pytest.mark.parametrize(
    ("msg", "code"),
    [
        ("timed_out", "timed_out"),
        ("all_tasks_deadlocked", "all_tasks_deadlocked"),
        ("Dag Run's state was manually set to `failed`.", "dag_run_failed"),
    ],
)
def test_a_failed_run_names_the_reason_of_airflow_by_its_code(plugin, msg, code):
    _, body = plugin.failed_run_message("a_dag", msg, NOW)
    assert body.startswith(f"failed: {code}, DAG a_dag, at 2026-11-07 09:30 UTC")


def test_a_dag_with_an_id_that_is_not_plain_is_not_named(plugin):
    _, body = plugin.failed_run_message("a dag; drop", "task_failure", NOW)
    assert "drop" not in body
    assert "(an ID that is not shown)" in body


@pytest.mark.parametrize("argv", [["airflow", "api-server"], ["airflow", "dag-processor"], []])
def test_the_listener_does_nothing_outside_the_scheduler(plugin, monkeypatch, argv):
    # The API server calls it for a run that a person marks failed, and holds no address.
    monkeypatch.setattr(plugin.sys, "argv", argv)
    monkeypatch.setenv("AGORANOMOS_ALERT_URLS", JSON)
    failed = listener(plugin)
    monkeypatch.setattr(failed.sender, "submit", lambda *job: pytest.fail("sent"))
    failed.on_dag_run_failed(dag_run=Run("a_dag"), msg="task_failure")


def test_the_listener_does_nothing_without_an_address(plugin, monkeypatch):
    monkeypatch.setattr(plugin.sys, "argv", ["airflow", "scheduler"])
    failed = listener(plugin)
    monkeypatch.setattr(failed.sender, "submit", lambda *job: pytest.fail("sent"))
    failed.on_dag_run_failed(dag_run=Run("a_dag"), msg="task_failure")


def test_a_dag_that_fails_again_within_the_hour_sends_once_and_counts_the_rest(plugin, monkeypatch):
    monkeypatch.setattr(plugin.sys, "argv", ["airflow", "scheduler"])
    monkeypatch.setenv("AGORANOMOS_ALERT_URLS", JSON)
    queued = []
    failed = listener(plugin)
    monkeypatch.setattr(failed.sender, "submit", lambda *job: queued.append(job) or True)
    for minutes in (0, 15, 30, 45):
        failed.alert(Run("a_dag"), "task_failure", NOW + timedelta(minutes=minutes))
    failed.alert(Run("another_dag"), "task_failure", NOW + timedelta(minutes=50))
    failed.alert(Run("a_dag"), "task_failure", NOW + timedelta(minutes=61))
    assert [body.splitlines()[0].split(", ")[1] for _, body, _ in queued] == [
        "DAG a_dag",
        "DAG another_dag",
        "DAG a_dag",
    ]
    assert "failed 3 more times in the hour before" in queued[2][1]


def test_an_error_of_the_listener_never_reaches_the_scheduler(plugin, monkeypatch, caplog):
    monkeypatch.setattr(plugin.sys, "argv", ["airflow", "scheduler"])
    monkeypatch.setenv("AGORANOMOS_ALERT_URLS", JSON)
    failed = listener(plugin)

    def broken(*job):
        raise RuntimeError(JSON)

    monkeypatch.setattr(failed.sender, "submit", broken)
    failed.on_dag_run_failed(dag_run=Run("a_dag"), msg="task_failure")
    assert "Could not prepare the alert of a failed run (RuntimeError)" in caplog.text
    assert "127.0.0.1" not in caplog.text


def test_the_sender_sends_in_a_thread_of_its_own_and_never_waits(plugin, monkeypatch):
    monkeypatch.setenv("AGORANOMOS_ALERT_URLS", JSON)
    release = threading.Event()
    done = threading.Event()
    seen = []

    def slow_send(title, body, kind):
        release.wait(5)
        seen.append((title, threading.current_thread().name))
        done.set()

    monkeypatch.setattr(plugin, "send", slow_send)
    sender = plugin.Sender()
    assert sender.submit("a title", "a body", "failure") is True
    # The call returned while the send still waits.
    assert seen == []
    release.set()
    assert done.wait(5)
    assert seen == [("a title", "agoranomos-alerts")]
    assert sender.thread.daemon
    sender.stop()
    assert not sender.thread.is_alive()


def test_a_full_queue_drops_the_message_and_says_so(plugin, monkeypatch, caplog):
    release = threading.Event()
    monkeypatch.setattr(plugin, "send", lambda *job: release.wait(5))
    monkeypatch.setattr(plugin, "QUEUE_SIZE", 1)
    sender = plugin.Sender()
    outcomes = [sender.submit(f"alert {n}", "body", "failure") for n in range(4)]
    release.set()
    sender.stop()
    assert not sender.thread.is_alive()
    assert outcomes[0] is True
    assert False in outcomes
    assert "The queue of alerts is full: an alert was dropped" in caplog.text


def test_a_task_masks_the_setting_and_each_address(plugin, monkeypatch):
    masked = []
    log = types.ModuleType("airflow.sdk.log")
    log.mask_secret = masked.append
    monkeypatch.setitem(sys.modules, "airflow.sdk", types.ModuleType("airflow.sdk"))
    monkeypatch.setitem(sys.modules, "airflow.sdk.log", log)
    monkeypatch.setenv("AGORANOMOS_ALERT_URLS", f"{JSON}, {TELEGRAM}")
    plugin.mask_addresses()
    assert masked == [f"{JSON}, {TELEGRAM}", JSON, TELEGRAM]


def test_the_listener_imports_apprise_in_the_main_thread_before_the_first_send(plugin, monkeypatch):
    # The scheduler forks its workers from its main thread: an import in the thread that sends
    # could leave a child with the lock of an import that never ends.
    monkeypatch.setattr(plugin.sys, "argv", ["airflow", "scheduler"])
    monkeypatch.setenv("AGORANOMOS_ALERT_URLS", JSON)
    made_in = []
    real = plugin.prepared

    def watched(value):
        made_in.append(threading.current_thread() is threading.main_thread())
        return real(value)

    monkeypatch.setattr(plugin, "prepared", watched)
    failed = listener(plugin)
    monkeypatch.setattr(failed.sender, "submit", lambda *job: True)
    failed.on_dag_run_failed(dag_run=Run("a_dag"), msg="task_failure")
    assert made_in == [True]
    assert len(StandInApprise.assets) == 1
    # Made once in a process.
    plugin.send("title", "body")
    assert len(StandInApprise.assets) == 1
