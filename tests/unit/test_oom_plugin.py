"""Tests of the plugin that makes each task process the first that the kernel stops at the memory
limit of its container (airflow/plugins/agoranomos_oom.py).

Airflow is installed in the image and not in the development environment, so these tests import
the plugin with stand-ins for the two modules of Airflow that it imports. The stack tests check the
plugin in Airflow itself, in the process of a task.
"""

import errno
import importlib.util
import logging
import sys
import types
from pathlib import Path

import pytest

PLUGIN = Path(__file__).resolve().parents[2] / "airflow" / "plugins" / "agoranomos_oom.py"
TASK_RUNNER = "airflow.sdk.execution_time.task_runner"
# From <linux/prctl.h>.
PR_GET_DUMPABLE = 3
PR_SET_DUMPABLE = 4


class StandInAirflowPlugin:
    """What the plugin subclasses in place of airflow.plugins_manager.AirflowPlugin."""


def component(module: str, name: str) -> object:
    """An instance of a class of this module and name, such as Airflow passes to on_starting."""
    return type(name, (), {"__module__": module, "__qualname__": name})()


class Kernel:
    """The two things of the kernel that the plugin depends on. Airflow marks the process of a task
    non-dumpable, and for such a process the kernel gives the files of /proc/self to root, so that
    a process of another user cannot write its oom_score_adj."""

    def __init__(self, dumpable: int = 0):
        self.dumpable = dumpable
        self.calls: list[tuple[int, int]] = []

    def prctl(self, option: int, value: int = 0) -> int:
        self.calls.append((option, value))
        if option == PR_SET_DUMPABLE:
            self.dumpable = value
        return self.dumpable if option == PR_GET_DUMPABLE else 0


class Score:
    """Stands in for /proc/self/oom_score_adj: a file in tmp_path, which only a dumpable process
    may write."""

    def __init__(self, path: Path, kernel: Kernel):
        self.path = path
        self.kernel = kernel

    def write_text(self, text: str, encoding: str) -> int:
        if not self.kernel.dumpable:
            raise PermissionError(errno.EACCES, "Permission denied", str(self.path))
        return self.path.write_text(text, encoding=encoding)

    def read_text(self, encoding: str) -> str:
        return self.path.read_text(encoding=encoding)

    def __str__(self) -> str:
        return str(self.path)


@pytest.fixture
def kernel():
    """A process that Airflow has marked non-dumpable, as the process of a task is."""
    return Kernel()


ABSENT = None  # the file of Yama does not exist, as on a kernel without Yama
UNREADABLE = object()  # the path of Yama is there, but cannot be read


@pytest.fixture
def plugin(monkeypatch, tmp_path, kernel):
    """The plugin, imported with the stand-ins, writing to a file in tmp_path instead of /proc."""
    listeners = types.ModuleType("airflow.listeners")
    listeners.hookimpl = lambda function: function
    plugins_manager = types.ModuleType("airflow.plugins_manager")
    plugins_manager.AirflowPlugin = StandInAirflowPlugin
    monkeypatch.setitem(sys.modules, "airflow", types.ModuleType("airflow"))
    monkeypatch.setitem(sys.modules, "airflow.listeners", listeners)
    monkeypatch.setitem(sys.modules, "airflow.plugins_manager", plugins_manager)
    spec = importlib.util.spec_from_file_location("agoranomos_oom", PLUGIN)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert (module.PR_GET_DUMPABLE, module.PR_SET_DUMPABLE) == (PR_GET_DUMPABLE, PR_SET_DUMPABLE)
    score = tmp_path / "oom_score_adj"
    score.write_text("0\n", encoding="ascii")
    monkeypatch.setattr(module, "OOM_SCORE_ADJ", Score(score, kernel))
    monkeypatch.setattr(module, "prctl", kernel.prctl)
    # A host whose kernel restricts ptrace with Yama, unless a test says otherwise.
    monkeypatch.setattr(module, "YAMA_PTRACE_SCOPE", yama_file(tmp_path, "1\n"))
    return module


def yama_file(folder: Path, content: str | object | None) -> Path:
    """The stand-in for /proc/sys/kernel/yama/ptrace_scope: a file with this content (as the
    kernel writes it, with a newline), no file, or a path that cannot be read."""
    path = folder / "ptrace_scope"
    if path.is_dir():
        path.rmdir()
    path.unlink(missing_ok=True)
    if content is UNREADABLE:
        path.mkdir()
    elif content is not ABSENT:
        path.write_text(content, encoding="ascii")
    return path


@pytest.fixture
def host_yama(plugin, monkeypatch, tmp_path):
    """Sets what the kernel of the host says about Yama."""

    def set_to(content):
        monkeypatch.setattr(plugin, "YAMA_PTRACE_SCOPE", yama_file(tmp_path, content))

    return set_to


def listener(plugin):
    [only] = plugin.AgoranomosOomPlugin.listeners
    return only


def test_the_plugin_gives_airflow_one_listener(plugin):
    assert issubclass(plugin.AgoranomosOomPlugin, StandInAirflowPlugin)
    assert plugin.AgoranomosOomPlugin.name == "agoranomos_oom"
    assert callable(listener(plugin).on_starting)


def test_a_task_process_goes_first(plugin):
    listener(plugin).on_starting(component=component(TASK_RUNNER, "TaskRunnerMarker"))
    assert plugin.OOM_SCORE_ADJ.read_text(encoding="ascii") == "1000"


@pytest.mark.parametrize("scope", ["1\n", "2\n", "3\n"])
def test_a_non_dumpable_process_is_dumpable_only_while_it_writes_the_score(
    plugin, kernel, host_yama, scope
):
    # The kernel gives the files of /proc/self to root while the process is non-dumpable, as the
    # process of a task is: so the plugin makes it dumpable for the write, and non-dumpable again.
    # It does so only where Yama keeps the other processes of the user from opening its memory in
    # the meantime: ptrace_scope 1, 2 or 3.
    host_yama(scope)
    listener(plugin).on_starting(component=component(TASK_RUNNER, "TaskRunnerMarker"))
    assert plugin.OOM_SCORE_ADJ.read_text(encoding="ascii") == "1000"
    assert kernel.calls == [
        (PR_GET_DUMPABLE, 0),
        (PR_SET_DUMPABLE, 1),
        (PR_SET_DUMPABLE, 0),
    ]
    assert kernel.dumpable == 0


# What a host without the restriction of Yama gives, and what the plugin reports as found.
NOT_RESTRICTED = [
    pytest.param(ABSENT, "absent", id="no-yama"),
    pytest.param(UNREADABLE, "absent", id="unreadable"),
    pytest.param("0\n", "0", id="scope-0"),
    pytest.param("", "empty", id="empty"),
    pytest.param("banana\n", "banana", id="garbage"),
    pytest.param("-1\n", "-1", id="negative"),
]


@pytest.mark.parametrize(("content", "found"), NOT_RESTRICTED)
def test_a_non_dumpable_process_is_left_as_it_is_where_yama_does_not_restrict_ptrace(
    plugin, kernel, host_yama, caplog, content, found
):
    # Without Yama, a process of the same user could open the memory of the task while the mark is
    # lifted, and keep it open: so the mark and the score stay, and the log says so, once, at INFO.
    host_yama(content)
    with caplog.at_level(logging.INFO):
        listener(plugin).on_starting(component=component(TASK_RUNNER, "TaskRunnerMarker"))
    assert plugin.OOM_SCORE_ADJ.read_text(encoding="ascii") == "0\n"
    assert kernel.calls == [(PR_GET_DUMPABLE, 0)]
    assert kernel.dumpable == 0
    [record] = caplog.records
    assert record.levelno == logging.INFO
    assert "keeps its place" in record.getMessage()
    assert "Yama" in record.getMessage()
    assert record.getMessage().count(found) >= 1
    assert f"ptrace_scope {found}" in record.getMessage()


@pytest.mark.parametrize(
    "content", [pytest.param(ABSENT, id="no-yama"), pytest.param("0\n", id="scope-0"), "1\n", "3\n"]
)
def test_a_dumpable_process_is_written_whatever_yama_says(
    plugin, kernel, host_yama, caplog, content
):
    host_yama(content)
    kernel.dumpable = 1
    with caplog.at_level(logging.INFO):
        listener(plugin).on_starting(component=component(TASK_RUNNER, "TaskRunnerMarker"))
    assert plugin.OOM_SCORE_ADJ.read_text(encoding="ascii") == "1000"
    assert kernel.calls == [(PR_GET_DUMPABLE, 0)]
    assert kernel.dumpable == 1
    assert caplog.records == []


@pytest.mark.parametrize(
    ("module", "name"),
    [
        # The job of the scheduler, the triggerer or the DAG processor.
        ("airflow.jobs.job", "Job"),
        ("airflow.jobs.scheduler_job_runner", "SchedulerJobRunner"),
        # The name alone is not enough, and neither is the module.
        ("some_dag", "TaskRunnerMarker"),
        (TASK_RUNNER, "RuntimeTaskInstance"),
    ],
)
def test_every_other_process_keeps_its_place(plugin, kernel, module, name):
    listener(plugin).on_starting(component=component(module, name))
    assert plugin.OOM_SCORE_ADJ.read_text(encoding="ascii") == "0\n"
    assert kernel.calls == []


def test_a_score_that_cannot_be_written_stops_no_task(
    plugin, kernel, monkeypatch, tmp_path, caplog
):
    missing = tmp_path / "no-such-folder" / "oom_score_adj"
    monkeypatch.setattr(plugin, "OOM_SCORE_ADJ", Score(missing, kernel))
    with caplog.at_level(logging.WARNING):
        listener(plugin).on_starting(component=component(TASK_RUNNER, "TaskRunnerMarker"))
    [record] = caplog.records
    assert record.levelno == logging.WARNING
    assert str(missing) in record.getMessage()
    assert "before this task" in record.getMessage()
    # The process is non-dumpable again, whatever happened to the write.
    assert kernel.dumpable == 0


def test_a_process_that_cannot_be_made_dumpable_stops_no_task(plugin, kernel, monkeypatch, caplog):
    def refuse(option: int, value: int = 0) -> int:
        if option == PR_SET_DUMPABLE and value == 1:
            raise OSError(errno.EPERM, "Operation not permitted")
        return kernel.prctl(option, value)

    monkeypatch.setattr(plugin, "prctl", refuse)
    with caplog.at_level(logging.WARNING):
        listener(plugin).on_starting(component=component(TASK_RUNNER, "TaskRunnerMarker"))
    [record] = caplog.records
    assert record.levelno == logging.WARNING
    assert "before this task" in record.getMessage()
    assert plugin.OOM_SCORE_ADJ.read_text(encoding="ascii") == "0\n"
    assert kernel.dumpable == 0


def refuse_to_put_the_mark_back(kernel):
    """A prctl that works, except that it cannot make the process non-dumpable again."""

    def prctl(option: int, value: int = 0) -> int:
        if option == PR_SET_DUMPABLE and value == 0:
            raise OSError(errno.EPERM, "Operation not permitted")
        return kernel.prctl(option, value)

    return prctl


def test_a_mark_that_cannot_be_put_back_is_an_error_and_stops_no_task(
    plugin, kernel, monkeypatch, caplog
):
    monkeypatch.setattr(plugin, "prctl", refuse_to_put_the_mark_back(kernel))
    with caplog.at_level(logging.INFO):
        listener(plugin).on_starting(component=component(TASK_RUNNER, "TaskRunnerMarker"))
    assert plugin.OOM_SCORE_ADJ.read_text(encoding="ascii") == "1000"
    [record] = caplog.records
    assert record.levelno == logging.ERROR
    assert "dumpable" in record.getMessage()
    assert "non-dumpable" in record.getMessage()
    # The process is left as it is: the log says so.
    assert kernel.dumpable == 1


def test_a_mark_that_cannot_be_put_back_after_a_failed_write_gives_both_lines(
    plugin, kernel, monkeypatch, tmp_path, caplog
):
    monkeypatch.setattr(plugin, "prctl", refuse_to_put_the_mark_back(kernel))
    missing = tmp_path / "no-such-folder" / "oom_score_adj"
    monkeypatch.setattr(plugin, "OOM_SCORE_ADJ", Score(missing, kernel))
    with caplog.at_level(logging.INFO):
        listener(plugin).on_starting(component=component(TASK_RUNNER, "TaskRunnerMarker"))
    assert sorted(record.levelno for record in caplog.records) == [logging.WARNING, logging.ERROR]


# The children of the scheduler. The kernel of Linux 7.0 can stop a second process while the task
# that it stopped first still frees its memory, and without the plugin that process is the
# scheduler, the largest of the rest; so the plugin ranks two children of the scheduler between the
# tasks and the scheduler: serve-logs, then the health check. The LocalExecutor workers keep their
# place.
SCHEDULER_ARGV = ["/home/airflow/.local/bin/airflow", "scheduler"]
JOB = ("airflow.jobs.job", "Job")


@pytest.fixture
def after_fork(monkeypatch):
    """Stands in for multiprocessing.util.register_after_fork: records each (object, function) that
    the plugin registers, for the test to call as multiprocessing would in a child."""
    from multiprocessing import util

    registered: list[tuple[object, object]] = []
    monkeypatch.setattr(
        util, "register_after_fork", lambda obj, func: registered.append((obj, func))
    )
    return registered


@pytest.fixture
def scheduler(monkeypatch, kernel):
    """The process of the scheduler, as airflow scheduler starts it: the processes that it forks
    are dumpable, unlike those of the tasks."""
    monkeypatch.setattr(sys, "argv", SCHEDULER_ARGV)
    kernel.dumpable = 1


def named(name: str):
    """A function of this name, such as multiprocessing runs in a child that it starts."""

    def target() -> None:
        """Does nothing."""

    target.__name__ = name
    return target


def in_child(monkeypatch, registered, target) -> None:
    """Runs the registered hooks as multiprocessing runs them in a child that runs target."""
    import multiprocessing

    monkeypatch.setattr(
        multiprocessing, "current_process", lambda: types.SimpleNamespace(_target=target)
    )
    for obj, func in registered:
        func(obj)


def test_the_scheduler_registers_one_hook_for_its_children_and_keeps_its_place(
    plugin, kernel, scheduler, after_fork
):
    listener(plugin).on_starting(component=component(*JOB))
    listener(plugin).on_starting(component=component(*JOB))
    assert len(after_fork) == 1
    assert plugin.OOM_SCORE_ADJ.read_text(encoding="ascii") == "0\n"
    assert kernel.calls == []


@pytest.mark.parametrize(("target", "rank"), [("serve_logs", "800"), ("serve_health_check", "700")])
def test_serve_logs_and_the_health_check_go_after_the_tasks_and_before_the_scheduler(
    plugin, scheduler, after_fork, monkeypatch, target, rank
):
    listener(plugin).on_starting(component=component(*JOB))
    in_child(monkeypatch, after_fork, named(target))
    assert plugin.OOM_SCORE_ADJ.read_text(encoding="ascii") == rank


@pytest.mark.parametrize("target", ["_run_worker", "serve_logs_too", "run", None])
def test_the_workers_and_any_other_child_of_the_scheduler_keep_their_place(
    plugin, scheduler, after_fork, monkeypatch, target
):
    # An idle LocalExecutor worker that the kernel stops while it holds the read lock of the queue
    # of the executor stops every task: such a worker must not go before the scheduler.
    listener(plugin).on_starting(component=component(*JOB))
    in_child(monkeypatch, after_fork, None if target is None else named(target))
    assert plugin.OOM_SCORE_ADJ.read_text(encoding="ascii") == "0\n"


@pytest.mark.parametrize(("content", "found"), NOT_RESTRICTED)
def test_no_child_of_the_scheduler_moves_where_yama_does_not_restrict_ptrace(
    plugin, scheduler, after_fork, host_yama, content, found
):
    # There the tasks keep 0, and serve-logs or the health check would go before them.
    host_yama(content)
    listener(plugin).on_starting(component=component(*JOB))
    assert after_fork == []


@pytest.mark.parametrize(
    ("argv", "module", "name"),
    [
        # The triggerer and the DAG processor pass their jobs too.
        (["/home/airflow/.local/bin/airflow", "triggerer"], *JOB),
        (["/home/airflow/.local/bin/airflow", "dag-processor"], *JOB),
        (["airflow"], *JOB),
        # In the scheduler, only its job counts.
        (SCHEDULER_ARGV, "airflow.jobs.scheduler_job_runner", "SchedulerJobRunner"),
        (SCHEDULER_ARGV, "some_dag", "Job"),
    ],
)
def test_no_other_process_ranks_its_children(
    plugin, kernel, after_fork, monkeypatch, argv, module, name
):
    monkeypatch.setattr(sys, "argv", argv)
    listener(plugin).on_starting(component=component(module, name))
    assert after_fork == []
    assert plugin.OOM_SCORE_ADJ.read_text(encoding="ascii") == "0\n"


def test_a_rank_that_cannot_be_written_in_a_child_stops_nothing(
    plugin, kernel, scheduler, after_fork, monkeypatch, tmp_path, caplog
):
    missing = tmp_path / "no-such-folder" / "oom_score_adj"
    monkeypatch.setattr(plugin, "OOM_SCORE_ADJ", Score(missing, kernel))
    listener(plugin).on_starting(component=component(*JOB))
    with caplog.at_level(logging.WARNING):
        in_child(monkeypatch, after_fork, named("serve_logs"))
    [record] = caplog.records
    assert record.levelno == logging.WARNING
    assert str(missing) in record.getMessage()


def test_the_ranks_of_the_children_stay_between_the_scheduler_and_the_tasks(plugin):
    # The kernel ranks a process by its memory, in pages, plus its oom_score_adj times a thousandth
    # of the pages of the limit of its container. A task at 1000 has at least the pages of the
    # limit; a child of the scheduler has about an eighth of them, so up to 875 it stays after
    # every task, and from 1 up before the scheduler at 0.
    assert plugin.CHILD_RANKS
    assert all(0 < int(rank) < 875 for rank in plugin.CHILD_RANKS.values())


def serve_logs() -> None:
    """Has the name of the function that Airflow runs in the child of serve-logs, and does
    nothing."""


@pytest.mark.skipif(
    "fork" not in __import__("multiprocessing").get_all_start_methods(), reason="no fork here"
)
def test_a_child_that_multiprocessing_forks_gets_its_rank(plugin, scheduler, monkeypatch):
    # The hook runs in the child once multiprocessing knows what the child runs, as Python 3.13
    # does it, with the start method of the scheduler of the image (fork).
    import multiprocessing
    from multiprocessing import util

    listener(plugin).on_starting(component=component(*JOB))
    try:
        child = multiprocessing.get_context("fork").Process(target=serve_logs)
        child.start()
        child.join(timeout=30)
        assert child.exitcode == 0
    finally:
        # Take the hook back, so that no later fork of this process runs it.
        for key, value in list(util._afterfork_registry.items()):
            if getattr(value, "__module__", None) == plugin.__name__:
                del util._afterfork_registry[key]
    assert plugin.OOM_SCORE_ADJ.read_text(encoding="ascii") == "800"
