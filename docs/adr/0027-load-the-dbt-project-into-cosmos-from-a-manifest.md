---
status: proposed
date: 2026-10-12
decision-makers: Andrei Gromov
---

# Load the dbt project into Cosmos from a manifest

## Context and Problem Statement

Cosmos renders each model of the dbt project as a task of Airflow ([ADR-0005](0005-transform-with-dbt-core.md)). In v0.1 it reads the project with `dbt ls`, each time that the DAG processor parses the transform DAG and again when each task of that DAG starts, because the caches of Cosmos are kept in Airflow Variables, which the Op role of Airflow may edit, so the stack keeps them off: such a Variable could change what a task of dbt runs. For the two models of v0.1, `dbt ls` takes 1.2 to 2 seconds in the DAG processor and 1.3 to 1.7 seconds at the start of each task. The model of v0.2.0 has more than thirty models and four seeds (ADR-0025), and a run of the transform has more than seventy tasks, each of which would parse the project again before it runs dbt. The DAG processor already parses an unchanged file only every five minutes, which saves most of its own cost.

Each task of dbt is also a process of dbt of its own, and with the LocalExecutor the tasks share the container of the scheduler and its memory limit of 1,280 MiB ([ADR-0015](0015-run-localexecutor-by-default.md)), where a task that takes the container to its limit can stop the scheduler as well. On a stand in October 2026, with a project of 28 models in the shape of the model of v0.2.0, four tasks of dbt at once, which `AIRFLOW_PARALLELISM` allows, took that container to 887 MiB, and two at once to between 666 and 826 MiB. Four fit, with less room for the peaks of CI, which have been higher than those of a Mac, and for another task at the same time. Server mode of the default variant is at its whole memory budget ([ADR-0022](0022-budget-the-memory-by-variant-and-mode.md)), so no new service or job fits. And the volume that `metadata-export` writes is closed to Airflow ([ADR-0016](0016-keep-the-metadata-of-the-marts-as-code.md)).

How does Airflow learn the structure of the dbt project, and how many tasks of dbt run at once?

## Decision Drivers

* Neither the DAG processor nor each task of dbt runs `dbt ls`.
* Nothing that the Op role of Airflow or a DAG of the extensions folder can edit decides what a task of dbt runs.
* No new service and no new one-shot job: the memory budget stays as it is.
* What runs is the dbt project of the checkout, read-only.
* The tasks of dbt fit the memory limit of the container that runs them.
* A dbt project that dbt cannot read fails loudly.

## Considered Options

* A manifest that the existing `metadata-export` writes into a second volume, which Airflow mounts read-only, and a pool of two slots for the tasks of dbt
* `dbt ls`, as in v0.1, until it takes longer than a threshold
* The cache of `dbt ls` in Airflow Variables
* A remote cache of Cosmos in object storage
* A one-shot job of its own that runs `dbt parse`
* The watcher execution mode of Cosmos now

## Decision Outcome

Chosen option: "A manifest that the existing `metadata-export` writes into a second volume, which Airflow mounts read-only, and a pool of two slots for the tasks of dbt", because Airflow then learns the project without running dbt, only the checkout decides what a task runs, and no service or job is added.

In practice this means:

* **The manifest.** On every start, `metadata-export` runs `dbt ls` for the metadata of the marts, and dbt writes `manifest.json` into its target folder as it does so. The job copies that file, in one step, into the volume `dbt-manifest`, which only it can write and which holds no secret. Every Airflow service mounts the volume read-only at `/opt/agoranomos/manifest`. The volume of the export of the metadata stays closed to Airflow.
* **Cosmos** renders the tasks from the manifest (`LoadMode.DBT_MANIFEST`) and runs dbt on the project of the checkout, which every Airflow service mounts read-only, as before: one process of dbt for each model and each set of tests (`ExecutionMode.LOCAL`), and the tests of a model right after it.
* **Order.** The DAG processor and the scheduler, and with the CeleryExecutor the worker, start after `metadata-export` has finished, so their first parse finds the manifest; a dbt project that dbt cannot read stops them, with the error of `metadata-export`.
* **Changes.** A change of the dbt project reaches Airflow at the next `./agoranomos up`. A missing or broken manifest fails the import of the transform DAG, which Airflow shows as an import error.
* **The pool.** The tasks of dbt run in the pool `dbt` of two slots, which `airflow-init` sets on every start, so an edit of the pool in the Airflow UI is reverted at the next start. Other tasks are not in it.
* **No new service.** The memory budget of ADR-0022 does not change.
* **The local mode in v0.2.0.** v0.2.0 runs dbt in the local mode of Cosmos, also with the elementary package for data quality that it is planned to add. The hooks of that package run with every process of dbt, so they are expected to make each task of dbt about twice as long and the transform about two thirds longer: a stand measured 65 %. That cost is accepted.
* **The watcher mode** is measured on the model of v0.2.0 and decided in a later 0.2.x release.

The other options were not chosen for these reasons:

* **`dbt ls` until a threshold** reaches its threshold with the model of v0.2.0, and each of more than seventy tasks would still parse the project before it runs.
* **The cache in Airflow Variables** would let the Op role change what a task of dbt runs.
* **A remote cache** needs object storage, which the stack does not have, and adds seconds to each parse.
* **A job of its own** would take at least 128 MiB of a budget that server mode has used up.
* **The watcher mode now** has open bugs in its retries in Cosmos 1.15.1, as of October 2026 ([#3001](https://github.com/astronomer/astronomer-cosmos/issues/3001), [#3023](https://github.com/astronomer/astronomer-cosmos/issues/3023)). Its sensors, which by default wait in the triggerer, also failed on a stand, and it keeps the states of the models in Airflow Variables for its retries. It is measured and decided in a later 0.2.x release.

### Consequences

* Good, because the DAG processor and the tasks of dbt no longer run `dbt ls`, and a task of dbt starts sooner.
* Good, because what a task of dbt runs comes from the checkout only.
* Good, because at most two processes of dbt share the memory of the container of the tasks.
* Bad, because a change of the dbt project reaches Airflow only at the next `./agoranomos up`, where in v0.1 it came within five minutes.
* Bad, because with a manifest, Cosmos selects and excludes models with logic of its own, which its documentation says "may not be as robust as dbt's".
* Bad, because a dbt project that dbt cannot read now keeps the DAG processor and the scheduler from starting, not only the setup of Metabase.
* Bad, because two tasks of dbt at a time make the transform slower than four would: by about a quarter on the stand.
* Bad, because in the local mode the hooks of the elementary package run with every task of dbt, which makes the transform about two thirds longer than without them.

## More Information

* **Implementation status, as of October 2026:** planned for v0.2.0; none of it is in place.
* The facts behind this record, read on 2026-10-12: the documentation of Cosmos 1.15.1 ([the parsing methods](https://github.com/astronomer/astronomer-cosmos/blob/astronomer-cosmos-v1.15.1/docs/guides/translate_dbt_to_airflow/parsing-methods.rst); [memory optimization](https://github.com/astronomer/astronomer-cosmos/blob/astronomer-cosmos-v1.15.1/docs/optimize_performance/memory_optimization.rst): "Use DBT_MANIFEST load mode — Impact: High — Avoids running `dbt ls` subprocess"; [the watcher execution mode](https://github.com/astronomer/astronomer-cosmos/blob/astronomer-cosmos-v1.15.1/docs/guides/run_dbt/airflow-worker/watcher-execution-mode.rst), whose producer backs up the states of the models to an Airflow Variable for its retries), the open issues [#3001](https://github.com/astronomer/astronomer-cosmos/issues/3001) and [#3023](https://github.com/astronomer/astronomer-cosmos/issues/3023) of the retries of the watcher mode, the source of dbt-core 1.12.5 (`core/dbt/cli/main.py`: the command `ls` requires a manifest without `write=False`; `core/dbt/parser/manifest.py`: the manifest is written `if write and write_json`), and measurements on a stand: `dbt ls` took 1.84 seconds for a project of 26 nodes, and 3.46 seconds with the packages of elementary.
* The measurements of the transform on that stand, a Mac with the LocalExecutor, `AIRFLOW_PARALLELISM=4` and the limits of October 2026, with the project of 28 models: the DAG processor parsed it in a median of 1.28 seconds with `dbt ls` and of 0.23 seconds from a manifest; the median task of dbt took 3.3 seconds with `dbt ls` and 2.0 seconds from a manifest; a run took 93 seconds with four tasks of dbt at once and `dbt ls`, 118 seconds with two and `dbt ls`, and 75 seconds with two from a manifest, and the container of the tasks peaked at 887, 666 and 774 MiB. With the hooks of elementary 0.26.1, the last run took 124 seconds, 65 % longer, and peaked at 826 MiB, with a median task of dbt of 3.3 to 3.6 seconds instead of 2.0; on the dbt project of v0.1, a single `dbt run` or `dbt test` of one model took about twice as long with the package, 4.1 to 4.5 seconds instead of 2.0 to 2.1. With the sensors deferred to the triggerer, which is its default, the watcher mode failed twice, and its sensors took the triggerer to 576 MiB of its limit of 640 MiB.
* Related decisions: [ADR-0002](0002-orchestrate-with-airflow-3.md) (Airflow), [ADR-0005](0005-transform-with-dbt-core.md) (dbt through Cosmos), [ADR-0016](0016-keep-the-metadata-of-the-marts-as-code.md) (`metadata-export` and its volume), [ADR-0022](0022-budget-the-memory-by-variant-and-mode.md) (the memory budget).
