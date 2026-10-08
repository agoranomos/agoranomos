"""A plugin of Airflow that makes each task process the first that the kernel stops when the
container of the task reaches its memory limit.

The tasks run as processes of the service that runs them: the scheduler with the LocalExecutor
(ADR-0015), and the worker with the CeleryExecutor. When that container reaches its memory limit,
the kernel stops the process that it ranks first, and by memory alone that can be the scheduler or
the worker itself. As each task starts, before it parses the file of its DAG, this plugin writes
1000, the highest value, to /proc/self/oom_score_adj of the task process, so that the kernel stops
the task first. The child processes of the task, such as dbt, inherit the value. Linux lets a
process raise its own value without any privilege.

One thing stands in the way: Airflow marks the process of a task non-dumpable (prctl with
PR_SET_DUMPABLE, in airflow/sdk/execution_time/supervisor.py as of Airflow 3.3.2), so that task code
cannot read the memory of the other processes of its user. For such a process the kernel gives the
files of /proc/self to root, and the write fails with "Permission denied". So the plugin makes the
process dumpable for the write alone, and non-dumpable again right after it, before any code of a
DAG loads.

The image installs this folder where only root may write, and points Airflow at it
(airflow/Dockerfile).
"""

from __future__ import annotations

import ctypes
import logging
import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import ClassVar

from airflow.listeners import hookimpl
from airflow.plugins_manager import AirflowPlugin

log = logging.getLogger(__name__)

# How the kernel ranks this process when its container reaches its memory limit: from -1000 to
# 1000, and the highest goes first.
OOM_SCORE_ADJ = Path("/proc/self/oom_score_adj")
FIRST = "1000"
# From <linux/prctl.h>.
PR_GET_DUMPABLE = 3
PR_SET_DUMPABLE = 4
# What the task runner passes to on_starting, as of Airflow 3.3.2; the scheduler, the triggerer and
# the DAG processor pass their jobs instead. The class is matched by its module and name, so that
# the services that run no task do not import the task runner.
TASK_RUNNER = ("airflow.sdk.execution_time.task_runner", "TaskRunnerMarker")


def prctl(option: int, value: int = 0) -> int:
    """Call prctl(2) of the C library, and raise OSError when it fails."""
    result = ctypes.CDLL(None, use_errno=True).prctl(option, value, 0, 0, 0)
    if result < 0:
        number = ctypes.get_errno()
        raise OSError(number, os.strerror(number))
    return result


@contextmanager
def dumpable() -> Iterator[None]:
    """Make a non-dumpable process dumpable for the block, and non-dumpable again after it."""
    if prctl(PR_GET_DUMPABLE) != 0:
        yield
        return
    prctl(PR_SET_DUMPABLE, 1)
    try:
        yield
    finally:
        prctl(PR_SET_DUMPABLE, 0)


def is_task_runner(component: object) -> bool:
    """Whether the task runner called on_starting, in the process of a task."""
    kind = type(component)
    return (kind.__module__, kind.__qualname__) == TASK_RUNNER


class TasksFirst:
    """The listener: Airflow calls on_starting in each of its processes that loads the plugins."""

    @hookimpl
    def on_starting(self, component: object) -> None:
        if not is_task_runner(component):
            return
        try:
            with dumpable():
                OOM_SCORE_ADJ.write_text(FIRST, encoding="ascii")
        except OSError as error:
            # The task runs on; only the choice of the kernel at the memory limit stays as it was.
            log.warning(
                "%s could not be written (%s): at the memory limit, the kernel may stop the "
                "scheduler or the worker before this task",
                OOM_SCORE_ADJ,
                error,
            )


class AgoranomosOomPlugin(AirflowPlugin):
    name = "agoranomos_oom"
    listeners: ClassVar[list[object]] = [TasksFirst()]
