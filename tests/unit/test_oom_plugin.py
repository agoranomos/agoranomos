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
    return module


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


def test_a_non_dumpable_process_is_dumpable_only_while_it_writes_the_score(plugin, kernel):
    # The kernel gives the files of /proc/self to root while the process is non-dumpable, as the
    # process of a task is: so the plugin makes it dumpable for the write, and non-dumpable again.
    listener(plugin).on_starting(component=component(TASK_RUNNER, "TaskRunnerMarker"))
    assert kernel.calls == [
        (PR_GET_DUMPABLE, 0),
        (PR_SET_DUMPABLE, 1),
        (PR_SET_DUMPABLE, 0),
    ]
    assert kernel.dumpable == 0


def test_a_dumpable_process_is_left_as_it_is(plugin, kernel):
    kernel.dumpable = 1
    listener(plugin).on_starting(component=component(TASK_RUNNER, "TaskRunnerMarker"))
    assert plugin.OOM_SCORE_ADJ.read_text(encoding="ascii") == "1000"
    assert kernel.calls == [(PR_GET_DUMPABLE, 0)]
    assert kernel.dumpable == 1


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
