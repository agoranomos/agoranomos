# Run Agoranomos locally

This guide starts Agoranomos on your own machine in local mode: the warehouse, Airflow and Metabase, with the demo, reachable from this machine only ([ADR-0011](../adr/0011-local-and-server-modes.md)). It is for engineers who want to try the stack or work on it.

!!! note "Pre-release"

    Local mode runs from the `main` branch. The first release, v0.1.0, adds server mode and backups. The repository is private until v0.3, so cloning it needs an invitation.

## Before you begin

- Docker with at least 6 GB of memory, 2 CPUs and 10 GB of free disk, and 8 GB, 4 CPUs and 20 GB recommended: Docker Desktop on macOS, Docker Desktop with WSL2 on Windows, or Docker Engine with the Compose plugin on Linux. [System requirements](../reference/system-requirements.md) has the details.
- On Windows, run every command of this guide in a WSL2 terminal, such as Ubuntu, in which Docker Desktop's WSL integration is on (Settings → Resources → WSL Integration; it is on for the default distribution). Clone the repository into the Linux file system of WSL, such as your home folder there, not under `/mnt/c`: by default, `chmod` on a file of the Windows file system cannot make it readable only by you, so `.env` would not be private there, and the containers reach files there more slowly (as of October 2026, Microsoft's and Docker's documentation).
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

Put a DAG file into `extensions/dags/`. Airflow shows it within about a minute, in the `extensions` bundle, paused until you unpause it. The folder is yours: releases of Agoranomos never change it ([ADR-0013](../adr/0013-keep-extensions-apart-from-the-base-package.md)). A task of your DAG runs with the keys of Airflow and the passwords of its metadata database and of the warehouse roles in its environment, and with the CeleryExecutor also with the password of the broker, so review what you put there as you would any code that touches your data.

## Stop and start again

```bash
./agoranomos down     # stop the stack; the data stays
./agoranomos up       # start it again with the same data
./agoranomos status   # the state and health of every service
```

To delete the stack together with its data, run `docker compose down --volumes` in the checkout. This cannot be undone.

## Update to the latest `main`

First, in the root of the checkout, see whether the update changes the version of Metabase:

```bash
git fetch
git diff HEAD origin/main -- compose.yaml | grep '^[-+].*metabase/metabase:'
```

When it prints nothing, the version stays. When it prints the old and the new image of Metabase, and the second number of the version changes, such as from `v0.64.1` to `v0.65.0`, the update moves Metabase to a new major version: the next `up` migrates the database of Metabase, and only a backup made before it can take that back. Then back up Metabase first, as [Back up Metabase before an upgrade](../reference/configuration.md#back-up-metabase-before-an-upgrade) says, and note the commit that you come from, which `git rev-parse HEAD` prints, to go back to.

Then update, and let `init` add the settings that the new version brings to `.env`:

```bash
git pull
./agoranomos init
```

`init` names the settings that it adds, with their defaults or new secrets, and changes no value that is set: an `.env` keeps the values it has. Look at them before you start the stack: an update can bring limits, such as the memory limits that every service got in October 2026, whose defaults fit the demo. A default that an update raises does not reach a setting that `.env` already has, which keeps its old value until you raise it: compare the `*_MEMORY` settings of `.env` with those of `.env.example`. In October 2026 the default of `AIRFLOW_TASKS_MEMORY` went from `1152m` to `1280m`, and that of `AIRFLOW_SCHEDULER_MEMORY`, which only the CeleryExecutor variant uses, from `512m` to `640m`. If your own DAGs, a higher `AIRFLOW_PARALLELISM` or the questions and dashboards of your Metabase need more, raise the limits now ([Give a service more memory](#give-a-service-more-memory)). With its limit, Metabase gives half of `METABASE_MEMORY` to its heap, 1,088 MiB by default; without one, Java gave the heap a quarter of the memory of Docker, about 1.9 GiB on 8 GB. Then start the stack:

```bash
./agoranomos up
```

`up` builds the Airflow image again and starts again the services that changed; the data stays. A new key, such as the key that signs the sessions of Metabase, which came with the same update, signs everyone out of Metabase once: sign in again.

In earlier versions, the one-shot job `db-init` had an anonymous volume, and a stack that one of them started keeps it after an update, until the next `./agoranomos down` and `./agoranomos up`. The volume then stays behind, used by no container. It does no harm and does not grow, so removing it is up to you. To remove it, find its name first, in the root of the checkout, then start the stack again and remove the volume by that name:

```bash
docker inspect --format '{{range .Mounts}}{{if eq .Type "volume"}}{{.Name}}{{end}}{{end}}' "$(docker compose ps --all --quiet db-init)"
./agoranomos down
./agoranomos up
docker volume rm <the name that the first command printed>
```

When the first command prints nothing, `db-init` has no volume, and there is nothing to remove; when there is no container of `db-init` at all, it prints an error instead. The volumes that each `down` and `up` of those earlier versions left behind carry no name or label of the project, so they cannot be told apart from the unused volumes of other projects: leave them. For the same reason, do not use `docker volume prune`: it also removes the unused volumes of the other projects on this machine.

## Run the tasks on a separate worker

By default the scheduler of Airflow runs the tasks itself, with the LocalExecutor ([ADR-0015](../adr/0015-run-localexecutor-by-default.md)). The CeleryExecutor variant runs them on a separate worker, as a deployment that grows to more than one machine needs: it adds the Valkey broker and the `airflow-worker` service, and 12 GB of memory is recommended for it ([System requirements](../reference/system-requirements.md)).

To switch a running stack to it:

1. In Airflow, wait until no DAG run is running or queued: the scheduler starts again, and stops the tasks that it runs.
2. In `.env`, set `COMPOSE_FILE=compose.yaml:compose.celery.yaml`.
3. Run `./agoranomos up`. It starts the broker and the worker, and starts the Airflow services again with the CeleryExecutor. The DAGs, their runs and the data stay.

`./agoranomos logs airflow-worker` shows what the worker does.

To go back, wait again until no DAG run is running or queued, set `COMPOSE_FILE=compose.yaml`, and run `./agoranomos up`. It removes the worker, with any task that it still runs, and the broker, which keeps nothing on disk, with any task in its queue, and starts the Airflow services again with the LocalExecutor.

## Give a service more memory

Every service has a memory limit, set from measurements on the demo. Pipelines heavier than the demo need more, above all in the service that runs the tasks. Raise its setting in `.env`, for example `AIRFLOW_TASKS_MEMORY=2048m`, and run `./agoranomos up`, which starts that service again with the new limit. With the LocalExecutor, up to `AIRFLOW_PARALLELISM` tasks run in the scheduler at the same time, so raise `AIRFLOW_TASKS_MEMORY` with it. Each limit that you raise needs memory on the machine too ([System requirements](../reference/system-requirements.md#memory-limits)). [Configuration](../reference/configuration.md#memory-limits) lists the settings. The one-shot setup jobs, such as `airflow-init`, have their limits fixed in `compose.yaml`, with no setting in `.env`: [System requirements](../reference/system-requirements.md#when-a-service-reaches-its-limit) says what to do when one reaches its limit.

## If something goes wrong

- **A port is in use.** `preflight` names the port and its setting. Set another port in `.env`, for example `AIRFLOW_PORT=18080`, and run `./agoranomos up` again.
- **Docker has too little memory or too few CPUs.** In Docker Desktop on macOS, raise them in Settings → Resources → Advanced, Memory limit and CPU limit. On Windows with WSL2, Docker Desktop leaves them to WSL, which gives its virtual machine half of the memory of Windows by default: set `memory=` and `processors=` in the `[wsl2]` section of `.wslconfig` in your Windows user folder, such as `memory=8GB`, and create the file if there is none, which is the default; then restart WSL with `wsl --shutdown` in PowerShell, not in the WSL2 terminal. On Linux with Docker Engine, Docker has the memory and the CPUs of the machine. Then run `./agoranomos up` again. (As of October 2026, Docker's and Microsoft's documentation.)
- **A service failed.** `up` names it and the command that shows its logs, for example `./agoranomos logs airflow-init`. When the line says exit code 137, or Docker Compose says `exit 137`, or the log of a job says that a process of it exited with -9, the service was killed, most often at its memory limit: [System requirements](../reference/system-requirements.md#when-a-service-reaches-its-limit) says what to do.
- **A task failed with no error of its own, or a service restarted.** It may have reached its memory limit: [System requirements](../reference/system-requirements.md#when-a-service-reaches-its-limit) says how that shows, and [Give a service more memory](#give-a-service-more-memory) raises the limit. At the limit, the kernel stops a task before the service that runs it on a host whose kernel restricts ptrace with Yama, such as Ubuntu; on Docker Desktop for macOS, which has no Yama, it can stop the scheduler instead. When the scheduler of the LocalExecutor restarts, whatever the cause, it resets the tasks that were running in it as orphaned, about five minutes later on the stack that was measured, and runs them again, whatever their retries (as of Airflow 3.3.2, October 2026), so write tasks that are safe to run twice. With the CeleryExecutor, the tasks run in the container of the worker, apart from the scheduler ([above](#run-the-tasks-on-a-separate-worker)).
- **The demo is not ready.** [See the demo](see-the-demo.md) says what `demo-init` does and how to read its log.

[Command line](../reference/command-line.md) and [Configuration](../reference/configuration.md) describe every command and setting.
