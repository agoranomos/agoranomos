"""A plugin of Airflow that makes each task process the first that the kernel stops when the
container of the task reaches its memory limit, and serve-logs and the health check of the scheduler
the next, before the scheduler.

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

It does so only where the kernel of the host restricts ptrace with Yama
(/proc/sys/kernel/yama/ptrace_scope reads 1, 2 or 3), as the kernel of Ubuntu does. While the mark
is lifted, a process of the same user could open /proc/<pid>/mem of the task and keep it open after
the mark is back; Yama forbids that to every process that is not an ancestor of the task. Where
the file is missing or reads 0, as on Docker Desktop, the plugin changes nothing and says so in
one line of the log: the task keeps its place at the memory limit.

The kernel can stop a second process right after the task. Since Linux 7.0 (commit a72f73c4dd9b,
"cgroup: Don't expose dead tasks in cgroup"), the scan of the memory cgroup no longer sees a
process once it starts to exit (css_task_iter_advance in kernel/cgroup/cgroup.c, which
mem_cgroup_scan_tasks uses), so while the stopped task still frees its memory, the container stays
at its limit, and any process that asks for memory then makes the kernel stop another one: by
memory alone, the scheduler. So in the scheduler, the plugin ranks two of its children between the
tasks and itself: serve-logs at 800, which the stack can do without, since the API server reads
the logs from their shared volume and asks serve-logs only for the tasks that run, and then the
health check at 700, whose loss leaves the container unhealthy until the scheduler restarts, while
the tasks go on. The scheduler forks each of them once, so they cover the first two such rounds in
its life. The LocalExecutor workers keep 0, like the scheduler: an idle worker blocks in the get of
a multiprocessing.SimpleQueue, which holds the read lock of the queue while it waits (as of
Airflow 3.3.2 and Python 3.13), and if the kernel stops the worker that holds it, the other workers
wait for that lock for ever, and no task runs until the scheduler restarts.

The plugin learns what a child of the scheduler runs from multiprocessing
(multiprocessing.util.register_after_fork), which calls the hook in the child once the child knows
the function that it runs; os.register_at_fork calls its hooks earlier, while the child still
takes itself for the scheduler. This works with the start method fork, which the scheduler uses on
Python 3.13. With forkserver, the default of Python 3.14 on Linux, a child does not run the hooks
of the scheduler and keeps 0; the stack tests check the values. Like the tasks, the children move
only where Yama is on: elsewhere the tasks keep 0, and the children would go before them.

The image installs this folder where only root may write, and points Airflow at it
(airflow/Dockerfile).
"""

from __future__ import annotations

import ctypes
import logging
import os
import sys
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
# The restriction of ptrace by Yama: the file exists only in a kernel that has Yama, and reads 0
# (no restriction), 1, 2 or 3.
YAMA_PTRACE_SCOPE = Path("/proc/sys/kernel/yama/ptrace_scope")
# What the task runner passes to on_starting, as of Airflow 3.3.2; the scheduler, the triggerer and
# the DAG processor pass their jobs instead. The class is matched by its module and name, so that
# the services that run no task do not import the task runner.
TASK_RUNNER = ("airflow.sdk.execution_time.task_runner", "TaskRunnerMarker")
# What the scheduler passes to on_starting: its job, from Job.__init__, before it forks serve-logs,
# its health check and the LocalExecutor workers (airflow/jobs/job.py and
# airflow/cli/commands/scheduler_command.py, as of Airflow 3.3.2). The triggerer and the DAG
# processor pass a job too, so the command line tells the scheduler apart.
JOB = ("airflow.jobs.job", "Job")
SCHEDULER_COMMAND = "scheduler"
# The rank of two children of the scheduler, by the name of the function that multiprocessing runs
# in each (airflow.utils.serve_logs and airflow.utils.scheduler_health): after every task, at 1000,
# and before the scheduler, at 0. The kernel adds a thousandth of the pages of the limit for each
# point, so a task at 1000 gets at least the pages of the limit, more than a child below 875 with
# its own pages added.
CHILD_RANKS = {"serve_logs": "800", "serve_health_check": "700"}


def prctl(option: int, value: int = 0) -> int:
    """Call prctl(2) of the C library, and raise OSError when it fails."""
    result = ctypes.CDLL(None, use_errno=True).prctl(option, value, 0, 0, 0)
    if result < 0:
        number = ctypes.get_errno()
        raise OSError(number, os.strerror(number))
    return result


def ptrace_scope() -> str:
    """What the kernel says about the restriction of ptrace by Yama, as text: the value, or
    "absent" where the file is missing or cannot be read."""
    try:
        return YAMA_PTRACE_SCOPE.read_text(encoding="ascii").strip()[:32] or "empty"
    except (OSError, UnicodeError):
        return "absent"


def restricts_ptrace(scope: str) -> bool:
    """Whether the value says that Yama is on: a number from 1 up."""
    return scope.isascii() and scope.isdecimal() and int(scope) >= 1


@contextmanager
def dumpable() -> Iterator[None]:
    """Make a non-dumpable process dumpable for the block, and non-dumpable again after it. If
    that last step fails, the log says that the process stays dumpable; nothing is raised."""
    prctl(PR_SET_DUMPABLE, 1)
    try:
        yield
    finally:
        try:
            prctl(PR_SET_DUMPABLE, 0)
        except OSError as error:
            log.error(
                "the task process could not be made non-dumpable again (%s): it stays dumpable",
                error,
            )


def rank_first() -> None:
    """Write FIRST to the oom_score_adj of this process, or say why it stays as it is."""
    if prctl(PR_GET_DUMPABLE) != 0:
        OOM_SCORE_ADJ.write_text(FIRST, encoding="ascii")
        return
    scope = ptrace_scope()
    if not restricts_ptrace(scope):
        log.info(
            "this task keeps its place at the memory limit: the kernel of this host does not "
            "restrict ptrace (Yama, ptrace_scope %s), so the plugin does not lift the "
            "non-dumpable mark of the task to write %s; at the limit, the kernel may stop the "
            "scheduler or the worker before this task",
            scope,
            OOM_SCORE_ADJ,
        )
        return
    with dumpable():
        OOM_SCORE_ADJ.write_text(FIRST, encoding="ascii")


def is_task_runner(component: object) -> bool:
    """Whether the task runner called on_starting, in the process of a task."""
    kind = type(component)
    return (kind.__module__, kind.__qualname__) == TASK_RUNNER


def is_scheduler(component: object) -> bool:
    """Whether the scheduler called on_starting, with its job."""
    kind = type(component)
    return (kind.__module__, kind.__qualname__) == JOB and sys.argv[1:2] == [SCHEDULER_COMMAND]


class SchedulerChildren:
    """The hook that ranks serve-logs and the health check, in the children of the scheduler."""

    def __init__(self) -> None:
        self.registered = False

    def register(self) -> None:
        """Have multiprocessing call rank in each child that this process forks from now on. The
        registry of multiprocessing holds the hook by a weak reference, so the module keeps it."""
        if self.registered:
            return
        from multiprocessing import util

        util.register_after_fork(self, SchedulerChildren.rank)
        self.registered = True

    def rank(self) -> None:
        """In a child of the scheduler: write its rank, if it runs serve-logs or the health check.
        The children of the scheduler are dumpable, so the write needs no lifting of a mark."""
        import multiprocessing

        target = getattr(multiprocessing.current_process(), "_target", None)
        rank = CHILD_RANKS.get(getattr(target, "__name__", ""))
        if rank is None:
            return
        try:
            OOM_SCORE_ADJ.write_text(rank, encoding="ascii")
        except OSError as error:
            # The child runs on; only the choice of the kernel at the memory limit stays as it was.
            log.warning(
                "%s could not be written in %s (%s): at the memory limit, the kernel may stop the "
                "scheduler before this process",
                OOM_SCORE_ADJ,
                target.__name__,
                error,
            )


SCHEDULER_CHILDREN = SchedulerChildren()


class TasksFirst:
    """The listener: Airflow calls on_starting in each of its processes that loads the plugins."""

    @hookimpl
    def on_starting(self, component: object) -> None:
        if is_scheduler(component):
            if restricts_ptrace(ptrace_scope()):
                SCHEDULER_CHILDREN.register()
            return
        if not is_task_runner(component):
            return
        try:
            rank_first()
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
