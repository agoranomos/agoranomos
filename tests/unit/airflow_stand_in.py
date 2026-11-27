"""A stand-in of the parts of airflow.sdk that the DAG files of the loads import, for the unit tests
that import a DAG file: Airflow is not in the development environment.

The decorators keep the function of each task with its arguments, so that a test calls it with a
context of its own, and a call of a task in the function of a DAG records the tasks that it was
given, the XComArgs of Airflow, as the upstream tasks of the call."""

from __future__ import annotations

import importlib
import sys
import types


class Node:
    """The result of a task in the function of a DAG, an XComArg in Airflow, which >> chains."""

    def __init__(self, task: StandInTask) -> None:
        self.task = task

    def __rshift__(self, other):
        other.task.upstream.append(self.task.arguments["task_id"])
        return other


class StandInTask:
    """A task of the stand-in of airflow.sdk: the decorated function, with its arguments, and the
    IDs of the tasks upstream of it in the DAG."""

    def __init__(self, function, kind, arguments):
        self.function, self.kind, self.arguments = function, kind, arguments
        self.upstream: list[str] = []

    def __call__(self, *given):
        self.upstream += [node.task.arguments["task_id"] for node in given]
        return Node(self)


def stand_in_sdk(made: dict, context: dict) -> dict[str, types.ModuleType]:
    """The modules of airflow.sdk that the DAG files and the modules of the package import. Each
    task goes into made["tasks"] by its ID, the arguments of the DAG into made["dag"], and each
    secret that a task masks into made["masked"]; get_current_context gives context."""
    sdk = types.ModuleType("airflow.sdk")

    class Asset:
        def __init__(self, uri):
            self.uri = uri

    sdk.Asset = Asset
    sdk.AssetAll = sdk.AssetAny = lambda *assets: assets

    def task(**arguments):
        return lambda function: made["tasks"].setdefault(
            arguments["task_id"], StandInTask(function, "task", arguments)
        )

    def short_circuit(**arguments):
        return lambda function: made["tasks"].setdefault(
            arguments["task_id"], StandInTask(function, "short_circuit", arguments)
        )

    task.short_circuit = short_circuit
    sdk.task = task

    def dag(**arguments):
        def decorate(function):
            def build():
                made["dag"] = arguments
                function()

            return build

        return decorate

    sdk.dag = dag
    sdk.CronTriggerTimetable = lambda cron, timezone: ("cron", cron, timezone)
    sdk.Param = lambda default, **schema: default
    sdk.Variable = types.SimpleNamespace(name="Variable")
    sdk.get_current_context = lambda: context
    exceptions = types.ModuleType("airflow.sdk.exceptions")

    class AirflowFailException(Exception):
        pass

    class AirflowSkipException(Exception):
        pass

    exceptions.AirflowFailException = AirflowFailException
    exceptions.AirflowSkipException = AirflowSkipException
    log = types.ModuleType("airflow.sdk.log")
    log.mask_secret = made["masked"].append
    return {
        "airflow": types.ModuleType("airflow"),
        "airflow.sdk": sdk,
        "airflow.sdk.exceptions": exceptions,
        "airflow.sdk.log": log,
    }


# The modules of the package that import airflow.sdk, which each import of a DAG file is to import
# again with the stand-in of that import.
WITH_AIRFLOW = ("agoranomos_pipelines.assets", "agoranomos_pipelines.outcome")


def forget(monkeypatch, names) -> None:
    """Make the next import of each module of names import it again: out of sys.modules, and out
    of its package, from which `from package import module` would take it."""
    for name in names:
        sys.modules.pop(name, None)
        parent, _, child = name.rpartition(".")
        if parent and parent in sys.modules:
            monkeypatch.delattr(sys.modules[parent], child, raising=False)


def import_dag(monkeypatch, name: str, made: dict, context: dict) -> types.ModuleType:
    """Import the DAG file name with the stand-in of airflow.sdk, the modules of the package that
    import it again too."""
    for module_name, module in stand_in_sdk(made, context).items():
        monkeypatch.setitem(sys.modules, module_name, module)
    importlib.import_module("agoranomos_pipelines")
    forget(monkeypatch, (name, *WITH_AIRFLOW))
    return importlib.import_module(name)
