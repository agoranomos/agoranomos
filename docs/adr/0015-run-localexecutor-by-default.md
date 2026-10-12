---
status: accepted
date: 2026-10-05
decision-makers: Andrei Gromov
---

# Run LocalExecutor by default

## Context and Problem Statement

[ADR-0002](0002-orchestrate-with-airflow-3.md) chose Airflow with the CeleryExecutor and a Valkey broker. Celery runs tasks in separate worker containers, which can be spread over several machines. Most deployments of Agoranomos, however, run on one machine, a laptop or a server with 8 GB, and there Celery adds a broker and a worker to the memory budget of [ADR-0010](0010-multi-arch-and-memory-budget.md), and more parts to start, upgrade and debug. The LocalExecutor runs tasks as processes of the scheduler, on the same machine, with no broker.

Which executor does the stack run by default, and how does a deployment that needs separate workers get them?

## Decision Drivers

* The stack fits the memory budget of ADR-0010 with room for the pipelines.
* A single-machine deployment has few parts to start, upgrade and debug.
* A deployment that needs workers on other machines can have them without a fork of the stack.
* Measurements on the stack decide, not defaults copied from other projects.

## Considered Options

* LocalExecutor by default, and CeleryExecutor with Valkey as an overlay file
* CeleryExecutor with Valkey by default, as ADR-0002 decided
* Both executors as profiles of one Compose file

## Decision Outcome

Chosen option: "LocalExecutor by default, and CeleryExecutor with Valkey as an overlay file", because on one machine it saves the memory of a broker and a worker and has fewer parts, while separate workers stay one setting away.

In practice this means:

* **The default.** `compose.yaml` runs Airflow with the LocalExecutor: the scheduler container runs the tasks as its own processes. `AIRFLOW_PARALLELISM` in `.env`, 4 by default, caps the tasks that run at the same time; without it Airflow would start 32 worker processes at once.
* **The overlay.** `compose.celery.yaml` adds the Valkey broker and a separate `airflow-worker`, and switches the executor to Celery. A deployment selects it with `COMPOSE_FILE=compose.yaml:compose.celery.yaml` in `.env`, which `./agoranomos` and plain `docker compose` both read. CI tests both variants.
* **Conditions of the default.** Pipelines are idempotent and their tasks have retries, because a restart of the scheduler stops the tasks it runs. The scheduler container gets a memory limit that covers its tasks, and the documentation repeats Airflow's warning that a task out of memory can restart the scheduler.
* **Measurements decide.** Idle and peak memory, pipeline time, restarts of the scheduler, the worker and the broker, a task out of memory, and the secrets that task code can see are measured on the stack and on the demo pipeline, for both executors. If the results show that the default does not hold, a new record replaces this one.

The other options were not chosen for these reasons:

* **Celery by default.** Every deployment would run a broker and at least one worker, including the many that never need a second machine.
* **Profiles of one file.** The two variants exclude each other, and a profile cannot change the executor of services outside it, so the file would need conditional settings and dependencies on services that may not run.

### Consequences

* Good, because a single-machine deployment runs two fewer services and keeps more of its memory budget for the pipelines.
* Good, because separate workers stay available through one setting, and CI tests that variant too.
* Bad, because tasks share the scheduler's container: a task that uses too much memory can take the scheduler down, and a restart of the scheduler stops the tasks it runs.
* Bad, because task code runs next to the scheduler's settings, including the password of the Airflow metadata database. Celery workers can be given fewer secrets, but only with a configuration of their own, so neither variant isolates task code by default ([ADR-0013](0013-keep-extensions-apart-from-the-base-package.md)).
* Bad, because two variants have to be maintained and tested.

## More Information

* **Implementation status, as of October 2026:** in place for v0.1.0: `compose.yaml` runs the LocalExecutor, `compose.celery.yaml` is the CeleryExecutor overlay, and CI tests both. Measured, for both executors: idle and peak memory, with a memory limit for the service that runs the tasks, and a task that runs out of memory ([System requirements](../reference/system-requirements.md)); the secrets that task code can see ([ADR-0013](0013-keep-extensions-apart-from-the-base-package.md)). Measured with the LocalExecutor: a task out of memory once took the scheduler down with it, and the task that had been running in it ran again after the scheduler restarted, when the new scheduler reset it as orphaned. As the source code of Airflow 3.3.2 shows, every task that was running in the scheduler runs again after such a restart, whatever its retries ([System requirements](../reference/system-requirements.md#when-a-service-reaches-its-limit)). Since then, a plugin of the Airflow image makes each task process the first that the kernel stops at the memory limit, on a host whose kernel restricts ptrace with Yama, such as Ubuntu: in five runs of the same task with the LocalExecutor on a Mac, with a version of the plugin that acted on every host, the kernel stopped the task each time, and the scheduler went on. Docker Desktop on macOS has no Yama, so there the plugin now leaves the task as it is, and the scheduler can still be stopped. On an Ubuntu server with Linux 7.0 (11 October 2026), where the plugin acts, the kernel stopped the task each time, but in 5 of 39 runs it then stopped the scheduler too: since Linux 7.0 the kernel no longer sees a stopped task once it starts to exit, so a process that asks for memory while the task still frees its own makes the kernel stop a second one. Since then, the plugin also ranks serve-logs and the health check of the scheduler after the tasks and before the scheduler, on a host with Yama; in 48 runs with these ranks, the scheduler restarted once, after both had been stopped ([System requirements](../reference/system-requirements.md#when-a-service-reaches-its-limit)). Not measured yet: the time of the pipeline, and what a restart of the worker or the broker does to the tasks that it runs. From v0.1.5 the Celery worker holds neither the connection string of the metadata database of Airflow nor the key that signs its tokens, as the security model of Airflow 3.3.2 asks, and the results of Celery are in a database and a role of their own on `platform`, `celery_results` ([ADR-0003](0003-two-postgres-instances.md)), so that the worker holds no password of the metadata database; it keeps the Fernet key, the secret key of its log server, and the passwords of the warehouse roles and of the broker. This narrows what the worker holds, but it does not keep the code of a task away from the scheduler: the database backend of Celery 5.6.3 keeps each result as a pickle (a `PickleType` column of `celery_taskmeta`), and the scheduler loads the results of its tasks from that database (providers-celery 3.24.0), which unpickles them. So a task that writes a crafted result with the password of `celery_results`, which the worker holds, can run code in the scheduler, which holds the metadata database and the signing key.
* This record replaces the choice of the executor in ADR-0002. Airflow 3, the official image and Valkey as the broker of the Celery variant stay as ADR-0002 decided.
* Airflow's documentation: the [LocalExecutor](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/local.html) runs tasks "by spawning processes in a controlled fashion on the scheduler node" and warns that in containers a task out of memory can restart the scheduler; the [overview of executors](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/index.html); the [CeleryExecutor](https://airflow.apache.org/docs/apache-airflow-providers-celery/stable/celery_executor.html).
* Airflow's own Compose file runs the CeleryExecutor and asks for at least 4 GB of memory for Docker, ideally 8 GB, without a warehouse or a BI tool ([Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)).
