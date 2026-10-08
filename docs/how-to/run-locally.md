# Run Agoranomos locally

This guide starts Agoranomos on your own machine in local mode: the warehouse, Airflow and Metabase, with the demo, reachable from this machine only ([ADR-0011](../adr/0011-local-and-server-modes.md)). It is for engineers who want to try the stack or work on it.

!!! note "Pre-release"

    Local mode runs from the `main` branch. The first release, v0.1.0, adds server mode and backups. The repository is private until v0.3, so cloning it needs an invitation.

## Before you begin

- Docker with at least 6 GB of memory, 2 CPUs and 10 GB of free disk, and 8 GB, 4 CPUs and 20 GB recommended: Docker Desktop on macOS, Docker Desktop with WSL2 on Windows, or Docker Engine with the Compose plugin on Linux. [System requirements](../reference/system-requirements.md) has the details.
- git.
- Free ports 8080 and 3000 on `127.0.0.1`, or other ports that you set in `.env`.
- Access to the internet for the first start, which downloads the images, and the Python packages of the Airflow image from PyPI and GitHub.

## Start the stack

```bash
git clone https://github.com/agoranomos/agoranomos.git
cd agoranomos
./agoranomos up
```

`up` creates `.env` with new secrets, checks the machine, builds the Airflow image, starts the stack, and waits until every service is healthy and the demo is ready. The first start takes a few minutes; later starts take the image from the cache of Docker. When it is done, it prints the addresses:

- Airflow: <http://127.0.0.1:8080>
- Metabase: <http://127.0.0.1:3000>

## Sign in

```bash
./agoranomos credentials
```

It shows the address of Airflow and of Metabase, the user name of the Airflow admin, `admin`, the email of the Metabase admin, from `AGORANOMOS_ADMIN_EMAIL`, and the settings of `.env` that hold their passwords, `AIRFLOW_ADMIN_PASSWORD` and `METABASE_ADMIN_PASSWORD`. It never prints a password: open `.env`, which only you can read, and copy it from there.

Keep a copy of `.env` in a password manager. It holds the keys without which the connections that Airflow and Metabase save cannot be read back, for example from a backup.

## Look around

- In Metabase, open the collection **Agoranomos demo** and its dashboard **Ad spend (demo)**: [See the demo](see-the-demo.md) explains what they show.
- In Airflow, open the DAGs `agoranomos_demo_load` and `agoranomos_demo_transform`, which loaded and built the demo data. To check the executor, unpause the DAG `agoranomos_selfcheck` and trigger it: its one task runs where the executor runs tasks.

## Add your own DAG

Put a DAG file into `extensions/dags/`. Airflow shows it within about a minute, in the `extensions` bundle, paused until you unpause it. The folder is yours: releases of Agoranomos never change it ([ADR-0013](../adr/0013-keep-extensions-apart-from-the-base-package.md)). A task of your DAG runs with the keys of Airflow and the passwords of its metadata database and of the warehouse roles in its environment, so review what you put there as you would any code that touches your data.

## Stop and start again

```bash
./agoranomos down     # stop the stack; the data stays
./agoranomos up       # start it again with the same data
./agoranomos status   # the state and health of every service
```

To delete the stack together with its data, run `docker compose down --volumes` in the checkout. This cannot be undone.

## Update to the latest `main`

```bash
git pull
./agoranomos up
```

`up` adds to `.env` the settings that the new version brings, with their defaults or new secrets, and changes no value that is set. It builds the Airflow image again and starts again the services that changed; the data stays. Before an update that moves Metabase to a new major version, back up its database: [Back up Metabase before an upgrade](../reference/configuration.md#back-up-metabase-before-an-upgrade).

In earlier versions, the one-shot job `db-init` had an anonymous volume, and a stack that one of them started keeps it after an update, until the next `./agoranomos down` and `./agoranomos up`. The volume then stays behind, used by no container. To remove it, find its name first, in the root of the checkout, then start the stack again and remove the volume by that name:

```bash
docker inspect --format '{{range .Mounts}}{{if eq .Type "volume"}}{{.Name}}{{end}}{{end}}' "$(docker compose ps --all --quiet db-init)"
./agoranomos down
./agoranomos up
docker volume rm <the name that the first command printed>
```

When the first command prints nothing, `db-init` has no volume, and there is nothing to remove. Do not use `docker volume prune` instead: it also removes the unused volumes of the other projects on this machine.

## Run the tasks on a separate worker

By default the scheduler of Airflow runs the tasks itself, with the LocalExecutor ([ADR-0015](../adr/0015-run-localexecutor-by-default.md)). The CeleryExecutor variant runs them on a separate worker, as a deployment that grows to more than one machine needs: it adds the Valkey broker and the `airflow-worker` service, and it needs the recommended 8 GB of memory ([System requirements](../reference/system-requirements.md)).

To switch a running stack to it:

1. In Airflow, wait until no DAG run is running or queued: the scheduler starts again, and stops the tasks that it runs.
2. In `.env`, set `COMPOSE_FILE=compose.yaml:compose.celery.yaml`.
3. Run `./agoranomos up`. It starts the broker and the worker, and starts the Airflow services again with the CeleryExecutor. The DAGs, their runs and the data stay.

`./agoranomos logs airflow-worker` shows what the worker does.

To go back, wait again until no DAG run is running or queued, set `COMPOSE_FILE=compose.yaml`, and run `./agoranomos up`. It removes the worker, with any task that it still runs, and the broker, which keeps nothing on disk, with any task in its queue, and starts the Airflow services again with the LocalExecutor.

## Give a service more memory

Every service has a memory limit, set from measurements on the demo. Pipelines heavier than the demo need more, above all in the service that runs the tasks. Raise its setting in `.env`, for example `AIRFLOW_TASKS_MEMORY=2048m`, and run `./agoranomos up`, which starts that service again with the new limit. With the LocalExecutor, `AIRFLOW_PARALLELISM` tasks run in the scheduler at the same time, so raise `AIRFLOW_TASKS_MEMORY` with it. Each limit that you raise needs memory on the machine too ([System requirements](../reference/system-requirements.md#memory-limits)). [Configuration](../reference/configuration.md#memory-limits) lists the settings.

## If something goes wrong

- **A port is in use.** `preflight` names the port and its setting. Set another port in `.env`, for example `AIRFLOW_PORT=18080`, and run `./agoranomos up` again.
- **Docker has too little memory.** In Docker Desktop, raise it in Settings → Resources → Memory, then run `./agoranomos up` again.
- **A service failed.** `up` names it and the command that shows its logs, for example `./agoranomos logs airflow-init`.
- **A task failed with no error of its own, or a service restarted.** It may have reached its memory limit: [System requirements](../reference/system-requirements.md#when-a-service-reaches-its-limit) says how that shows, and [Give a service more memory](#give-a-service-more-memory) raises the limit. With the LocalExecutor, a task at its limit can take the scheduler down with it, and the scheduler runs the task again once it is back, whatever its retries, so write tasks that are safe to run twice. With the CeleryExecutor, the tasks run in the container of the worker, apart from the scheduler ([above](#run-the-tasks-on-a-separate-worker)).
- **The demo is not ready.** [See the demo](see-the-demo.md) says what `demo-init` does and how to read its log.

[Command line](../reference/command-line.md) and [Configuration](../reference/configuration.md) describe every command and setting.
