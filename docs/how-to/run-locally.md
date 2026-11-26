# Run agoranomos locally

This guide starts agoranomos on your own machine in local mode: the warehouse, Airflow and Metabase, with the demo, reachable from this machine only ([ADR-0011](../adr/0011-local-and-server-modes.md)), or through an SSH tunnel when the machine is another one of yours ([below](#run-it-on-another-machine-of-yours)). It is for engineers who want to try the stack or work on it. To be guided through the demo step by step instead, follow the tutorial [Run agoranomos locally with demo data](../tutorials/run-locally-with-demo-data.md).

!!! note "Pre-alpha"

    This guide installs v0.6.0, the latest release. <!-- x-release-please-version -->
    Each release has its notes on [the releases](https://github.com/agoranomos/agoranomos/releases), and the `main` branch holds what comes next.

## Before you begin

- Docker with at least 6 GB of memory, 2 CPUs and 10 GB of free disk, and 8 GB, 4 CPUs and 20 GB recommended: Docker Desktop on macOS, Docker Desktop with WSL2 on Windows, or Docker Engine with the Compose plugin on Linux. [System requirements](../reference/system-requirements.md) has the details.
- On Windows, run every command of this guide in a WSL2 terminal, such as Ubuntu, in which Docker Desktop's WSL integration is on (Settings → Resources → WSL Integration; it is on for the default distribution). Clone the repository into the Linux file system of WSL, such as your home folder there, not under `/mnt/c`: by default, `chmod` on a file of the Windows file system cannot make it readable only by you, so `.env` would not be private there, and the containers reach files there more slowly (as of October 2026, Microsoft's and Docker's documentation).
- git.
- Free ports 8080 and 3000 on `127.0.0.1`, or other ports that you set in `.env`.
- Access to the internet for the first start, which downloads the images, the Python packages of the Airflow image from PyPI and GitHub, and the packages of the dbt project through the hub of dbt, hub.getdbt.com.

## Start the stack

<!-- x-release-please-start-version -->

```bash
git clone --branch v0.6.0 https://github.com/agoranomos/agoranomos.git
cd agoranomos
./agoranomos up
```

<!-- x-release-please-end -->

`--branch` takes the tag of the release, so git says that the checkout is in a detached `HEAD`: that is expected. To work on agoranomos itself, clone without `--branch`, which gives the `main` branch.

`up` creates `.env` with new secrets, checks the machine, builds the Airflow image, starts the stack, and waits until every service is healthy and the demo is ready. Among the one-shot jobs that set the stack up on every start, `marts-init` builds the views of the marts, which Metabase reads, over the core of the warehouse, after the migrations of `db-init` and before `metabase-init` describes them in Metabase ([The views of the marts](../reference/configuration.md#the-views-of-the-marts)). The first start takes a few minutes, and up to half an hour on a slow machine, where the first transform of the demo takes most of it; later starts take the image from the cache of Docker. When it is done, it prints the addresses:

- Airflow: <http://127.0.0.1:8080>
- Metabase: <http://127.0.0.1:3000>

## Sign in

```bash
./agoranomos credentials
```

It shows the address of Airflow and of Metabase, the user name of the Airflow admin, `admin`, the email of the Metabase admin, from `AGORANOMOS_ADMIN_EMAIL`, and the settings of `.env` that hold their passwords, `AIRFLOW_ADMIN_PASSWORD` and `METABASE_ADMIN_PASSWORD`. It never prints a password: open `.env`, which only you can read, and copy it from there.

Keep a copy of `.env` in a password manager. It holds the keys without which the connections that Airflow and Metabase save cannot be read back, for example from a backup.

## Run it on another machine of yours

Local mode also suits a machine of your own that you reach over SSH, such as a spare computer at home, when nothing is to be open to the network: Airflow and Metabase listen on `127.0.0.1` of that machine only, so its firewall can keep every port but SSH closed. Start the stack there as [above](#start-the-stack), then open an SSH tunnel from your own machine and keep it open while you work (`-N` runs no command on the other machine, so the session is only the tunnel):

```bash
ssh -N -L 3000:127.0.0.1:3000 -L 8080:127.0.0.1:8080 <user>@<machine>
```

Then open `http://127.0.0.1:3000` and `http://127.0.0.1:8080` on your own machine. Give the tunnel the same port at both ends: Metabase builds its links from `METABASE_PORT`, as `http://localhost:<METABASE_PORT>` ([Configuration](../reference/configuration.md#server-mode)), and `./agoranomos ai connect` prints the address of its MCP server on that port too, so with another port on your side they point at the wrong address. If your own machine uses these ports already, such as for a stack of its own, `ssh` only warns that it cannot listen on them, and the addresses open what listens there. Then set other ports in the `.env` of the other machine, such as `METABASE_PORT=13000` and `AIRFLOW_PORT=18080`, run `./agoranomos up` there, and use them at both ends:

```bash
ssh -N -L 13000:127.0.0.1:13000 -L 18080:127.0.0.1:18080 <user>@<machine>
```

The backups stay on that machine: add `ops` to `COMPOSE_PROFILES` for one every day ([Back up every day](back-up-and-restore.md#back-up-every-day)), and copy them elsewhere if its data matters ([Copy the backups to another machine](back-up-and-restore.md#copy-the-backups-to-another-machine)).

## Look around

- In Metabase, open the collection **agoranomos / Demo** and its dashboard **Advertising overview (demo)**, and the dashboard **Web analytics** of the collection **agoranomos / Web analytics**, which shows the sessions of the demo: [See the demo](see-the-demo.md) explains what they show.
- In Airflow, open the DAGs `agoranomos_demo_history` and `agoranomos_demo_transform`, which loaded the demo data and built the model from it, `agoranomos_demo_daily`, which brings each new day, and `agoranomos_demo_intraday`, which brings the current day every few hours. To check the executor, unpause the DAG `agoranomos_selfcheck` and trigger it: its one task runs where the executor runs tasks.

## Add your own DAG

Put a DAG file into `extensions/dags/`. Airflow shows it within about a minute, in the `extensions` bundle, paused until you unpause it. The folder is yours: releases of agoranomos never change it ([ADR-0013](../adr/0013-keep-extensions-apart-from-the-base-package.md)). A task of your DAG runs with keys of Airflow and the passwords of the warehouse roles in its environment, with the LocalExecutor also with the password of the metadata database of Airflow, and with the CeleryExecutor with the passwords of the broker and of the database of the results of Celery instead, so review what you put there as you would any code that touches your data.

## Stop and start again

```bash
./agoranomos down     # stop the stack; the data stays
./agoranomos up       # start it again with the same data
./agoranomos status   # the state and health of every service
```

To delete the stack together with its data, run `docker compose down --volumes` in the checkout. This cannot be undone.

## Update to a new release

A release upgrades from the newest release of the previous minor version, the second number of the version, and from an earlier release of its own minor version: v0.7.0 upgrades from v0.6.0, and from no release before it ([ADR-0059](../adr/0059-upgrade-from-the-previous-minor-release-only.md)). The test of the upgrade runs that path only. From an older release, such as v0.5.0 or v0.4.5, update one minor version at a time, as [Update from an older release](#update-from-an-older-release) says.

First, in the root of the checkout, get the tags of the new releases, and see whether the release that you update to changes the version of Metabase. In the commands of this section, put the tag of that release in place of `<tag>`, such as `v0.6.0`; `git tag --sort=-v:refname | head -n 1` names the newest. A checkout of the `main` branch uses `origin/main` in place of `<tag>`, and `git pull` in place of `git checkout <tag>`. <!-- x-release-please-version -->

```bash
git fetch --tags
git diff HEAD <tag> -- compose.yaml | grep '^[-+].*metabase/metabase:'
```

When it prints nothing, the version stays. When it prints the old and the new image of Metabase, and the second number of the version changes, such as from `v0.64.1` to `v0.65.0`, the update moves Metabase to a new major version: the next `up` migrates the database of Metabase, and only a backup made before it can take that back. Then back up Metabase first, as [Back up Metabase before an upgrade](../reference/configuration.md#back-up-metabase-before-an-upgrade) says, and note the commit that you come from, which `git rev-parse HEAD` prints, to go back to.

Then update, and let `init` add the settings that the new version brings to `.env`:

```bash
git checkout <tag>
./agoranomos init
```

`init` names the settings that it adds, with their defaults or new secrets, and changes no value that is set: an `.env` keeps the values it has. Look at them before you start the stack: an update can bring limits, such as the memory limits that every service got in October 2026, whose defaults fit the demo. A default that an update raises does not reach a setting that `.env` already sets to a value, which keeps that value until you raise it (an empty setting takes the default): compare the `*_MEMORY` settings of `.env` with those of `.env.example`. In October 2026 the default of `AIRFLOW_TASKS_MEMORY` went from `1152m` to `1280m`, that of `AIRFLOW_SCHEDULER_MEMORY`, which only the CeleryExecutor variant uses, from `512m` to `640m`, and that of `PLATFORM_DB_MEMORY` from `384m` to `512m`. On 19 October 2026 the default of `WAREHOUSE_DB_MEMORY` went from `256m` to `384m`, for the transform of the model of advertising statistics, which comes close to 256 MiB, and that of `AIRFLOW_TRIGGERER_MEMORY` came down from `640m` to `512m` to make room for it: set both in an `.env` that has the old values. On 21 October 2026 the default of `AIRFLOW_DAG_PROCESSOR_MEMORY` came down from `768m` to `512m`, from new measurements; an `.env` with `768m` can keep it, or take `512m` to give the memory back. On 5 November 2026 it went up to `640m`, as the DAG processor reached 512 MiB in the stack tests: raise it in an `.env` that has `512m`. On 13 November 2026 the worker of the CeleryExecutor variant got a limit of its own, `AIRFLOW_WORKER_MEMORY`, `1536m` by default, which `init` adds to `.env`, and `AIRFLOW_TASKS_MEMORY` now limits only the scheduler of the LocalExecutor ([System requirements](../reference/system-requirements.md#memory-limits)). With the CeleryExecutor, a value that `.env` gave `AIRFLOW_TASKS_MEMORY` for the worker then limits no service, and `init` does not move it: set it in `AIRFLOW_WORKER_MEMORY`, or the worker gets `1536m`. If your own DAGs, a higher `AIRFLOW_PARALLELISM` or the questions and dashboards of your Metabase need more, raise the limits now ([Give a service more memory](#give-a-service-more-memory)). With its limit, Metabase gives half of `METABASE_MEMORY` to its heap, 1,088 MiB by default; without one, Java gave the heap a quarter of the memory of Docker, about 1.9 GiB on 8 GB. Then start the stack:

```bash
./agoranomos up
```

`up` builds the Airflow image again and starts again the services that changed; the data stays. Its job `db-init` applies the migrations of the core that the release brings and the warehouse lacks, once each, and `./agoranomos status` shows them; a warehouse of v0.4.x gets the schema `core` this way, as a new one does ([The migrations of the core](../reference/configuration.md#the-migrations-of-the-core)). Then its job `marts-init` loads the seeds of the new release and builds the views of the marts from them, so that a mart shows the platforms that the release adds from this first start, and so that a card of Metabase reads them under the names that it knows, and `metabase-init` describes their new columns; a run of the transform does not build a view that exists again ([The views of the marts](../reference/configuration.md#the-views-of-the-marts)). It builds the views of `ops` of the new release too, which the Ops collection reads, so that its questions on a view or a column that the release adds answer from this first start. After its build, `marts-init` drops the relations of an earlier release that no model of the new one builds or reads, and names each in its log: on a warehouse of v0.4, two tables of `staging` that hold a copy of the statistics of the ads and of the ad groups, and a view ([Upgrading from v0.4](../reference/data-model.md#upgrading-from-v04)). Before `db-init` applies them to a warehouse that holds data, `up` takes a backup of the databases by itself, which no retention removes, and when it cannot, it stops before `db-init` and says why ([The backup before a migration of the core](back-up-and-restore.md#the-backup-before-a-migration-of-the-core)). The first `up` after the update that gave the network of the stack a label for IPv6 ([ADR-0023](../adr/0023-give-the-network-of-the-stack-ipv6.md)) also stops the stack once and makes the network and the containers again, because Compose would otherwise recreate the network and lose the names of the services in it; it says so in one line, builds and pulls the images first so that a failure leaves the stack running, and it takes a few minutes. Until that `up`, `./agoranomos metadata diff` says to run `up` first. A new key, such as the key that signs the sessions of Metabase, which came with the same update, signs everyone out of Metabase once: sign in again.

**What the upgrades of the last releases change.** From v0.6.0 to v0.7.0, `up` finds two migrations of the core waiting and takes its backup before `db-init` applies them, `init` adds no setting, the marts keep their data and gain the column `is_final`, the new DAGs of TikTok and GA4 start paused, and with the demo the dashboard **Web analytics (demo)** goes to the trash, as every deployment gets the dashboard **Web analytics** ([Upgrading from v0.6](../reference/data-model.md#upgrading-from-v06), [What later starts do](see-the-demo.md#what-later-starts-do)). The migrations change the warehouse one way, so the way back is the backup that you took before the update ([Go back from an upgrade](back-up-and-restore.md#go-back-from-an-upgrade)). What the update to v0.6.0 changes is in [Upgrading from v0.5](../reference/data-model.md#upgrading-from-v05), and what the update to v0.5.0 changes in [Before the update to v0.5](back-up-and-restore.md#before-the-update-to-v05) and [Upgrading from v0.4](../reference/data-model.md#upgrading-from-v04).

In earlier versions, the one-shot job `db-init` had an anonymous volume, and a stack that one of them started keeps it after an update, until the next `./agoranomos down` and `./agoranomos up`. The volume then stays behind, used by no container. It does no harm and does not grow, so removing it is up to you. To remove it, find its name first, in the root of the checkout, then start the stack again and remove the volume by that name:

```bash
docker inspect --format '{{range .Mounts}}{{if eq .Type "volume"}}{{.Name}}{{end}}{{end}}' "$(docker compose ps --all --quiet db-init)"
./agoranomos down
./agoranomos up
docker volume rm <the name that the first command printed>
```

When the first command prints nothing, `db-init` has no volume, and there is nothing to remove; when there is no container of `db-init` at all, it prints an error instead. The volumes that each `down` and `up` of those earlier versions left behind carry no name or label of the project, so they cannot be told apart from the unused volumes of other projects: leave them. For the same reason, do not use `docker volume prune`: it also removes the unused volumes of the other projects on this machine.

### Update from an older release

A stack of a release older than the previous minor one goes to the newest release of each minor version in turn, starting with its own when it does not run that one yet: from v0.5.0 to v0.6.0 and then to v0.7.0, from v0.4.2 to v0.4.5, v0.5.0, v0.6.0 and then v0.7.0. Each release does its part of the upgrade on the warehouse, which the next one relies on. v0.7.0 still holds the code of the upgrade from v0.4, untested, and v0.8.0 removes it.

1. List the releases, and note the newest of your minor version and of each one after it. `git tag --list 'v*' --sort=v:refname` lists them from the oldest to the newest.
1. Before each step, take a backup and note the commit that you run, as [Take a backup now](back-up-and-restore.md#take-a-backup-now) says. Before the step to v0.5.0, follow [Before the update to v0.5](back-up-and-restore.md#before-the-update-to-v05) instead, which takes them too.
1. Update to the next release of your list as the beginning of this section says, from `git diff HEAD <tag>` to `./agoranomos up`, with its tag in place of `<tag>`. The guides of that release, in `docs/` of the checkout, say what its update changes, under the sections "Upgrading from" of `docs/reference/data-model.md`.
1. Wait until the release has done its part. `./agoranomos status` says `none waiting` on its line `Migrations of the core:`. In Airflow, every DAG of a load that you have unpaused has run once after the update, and a run of `agoranomos_demo_transform` after them has succeeded; with the demo, `demo-init` runs the demo through in that `up` already. Trigger by hand a load that would wait for its schedule, as the step to v0.5.0 asks for `agoranomos_ecb_daily` and `agoranomos_google_ads_daily` ([Upgrading from v0.4](../reference/data-model.md#upgrading-from-v04)).
1. Take the next release of your list in the same way, from step 2, until the stack runs the newest one.

The backup of each step leads back to the release before that step, with the checkout at the commit that you noted ([Go back from an upgrade](back-up-and-restore.md#go-back-from-an-upgrade)). Keep the backup of the first step for as long as you may want to go back to the release that you came from.

## Run the tasks on a separate worker

By default the scheduler of Airflow runs the tasks itself, with the LocalExecutor ([ADR-0015](../adr/0015-run-localexecutor-by-default.md)). The CeleryExecutor variant runs them on a separate worker, as a deployment that grows to more than one machine needs: it adds the Valkey broker and the `airflow-worker` service, whose memory limit is `AIRFLOW_WORKER_MEMORY`, and 12 GB of memory is recommended for it ([System requirements](../reference/system-requirements.md)).

To switch a running stack to it:

1. In Airflow, wait until no DAG run is running or queued: the scheduler starts again, and stops the tasks that it runs.
2. In `.env`, set `COMPOSE_FILE=compose.yaml:compose.celery.yaml`.
3. Run `./agoranomos up`. It starts the broker and the worker, and starts the Airflow services again with the CeleryExecutor. The DAGs, their runs and the data stay.

`./agoranomos logs airflow-worker` shows what the worker does.

To go back, wait again until no DAG run is running or queued, set `COMPOSE_FILE=compose.yaml`, and run `./agoranomos up`. It removes the worker, with any task that it still runs, and the broker, which keeps nothing on disk, with any task in its queue, and starts the Airflow services again with the LocalExecutor.

## Give a service more memory

Every service has a memory limit, set from measurements on the demo. Pipelines heavier than the demo need more, above all in the service that runs the tasks. Raise its setting in `.env`, for example `AIRFLOW_TASKS_MEMORY=2048m` with the LocalExecutor or `AIRFLOW_WORKER_MEMORY=2048m` with the CeleryExecutor, and run `./agoranomos up`, which starts that service again with the new limit. With the LocalExecutor, up to `AIRFLOW_PARALLELISM` tasks run in the scheduler at the same time, so raise `AIRFLOW_TASKS_MEMORY` with it, and with the CeleryExecutor up to `AIRFLOW_WORKER_CONCURRENCY` in the worker, so raise `AIRFLOW_WORKER_MEMORY` with that. Each limit that you raise needs memory on the machine too ([System requirements](../reference/system-requirements.md#memory-limits)). [Configuration](../reference/configuration.md#memory-limits) lists the settings. The limit of the warehouse, `WAREHOUSE_DB_MEMORY`, also sets the settings of PostgreSQL that follow it, such as its cache and the memory of each query: raise it, and they follow, with no setting of PostgreSQL to know ([Configuration](../reference/configuration.md#the-settings-of-postgresql-of-the-warehouse)). The one-shot setup jobs, such as `airflow-init`, have their limits fixed in `compose.yaml`, with no setting in `.env`: [System requirements](../reference/system-requirements.md#when-a-service-reaches-its-limit) says what to do when one reaches its limit.

## If something goes wrong

- **A port is in use.** `preflight` names the port and its setting. Set another port in `.env`, for example `AIRFLOW_PORT=18080`, and run `./agoranomos up` again.
- **Docker has too little memory or too few CPUs.** In Docker Desktop on macOS, raise them in Settings → Resources → Advanced, Memory limit and CPU limit. On Windows with WSL2, Docker Desktop leaves them to WSL, which gives its virtual machine half of the memory of Windows by default: set `memory=` and `processors=` in the `[wsl2]` section of `.wslconfig` in your Windows user folder, such as `memory=8GB`, and create the file if there is none, which is the default; then restart WSL with `wsl --shutdown` in PowerShell, not in the WSL2 terminal. On Linux with Docker Engine, Docker has the memory and the CPUs of the machine. Then run `./agoranomos up` again. (As of October 2026, Docker's and Microsoft's documentation.)
- **A service failed.** `up` names it and the command that shows its logs, for example `./agoranomos logs airflow-init`. When the line says exit code 137, or Docker Compose says `exit 137`, or the log of a job says that a process of it exited with -9, the service was killed, most often at its memory limit: [System requirements](../reference/system-requirements.md#when-a-service-reaches-its-limit) says what to do.
- **A task failed with no error of its own, or a service restarted.** It may have reached its memory limit: [System requirements](../reference/system-requirements.md#when-a-service-reaches-its-limit) says how that shows, and [Give a service more memory](#give-a-service-more-memory) raises the limit. At the limit, the kernel stops a task before the service that runs it on a host whose kernel restricts ptrace with Yama, such as Ubuntu; on Docker Desktop for macOS, which has no Yama, it can stop the scheduler instead. When the scheduler of the LocalExecutor restarts, whatever the cause, it resets the tasks that were running in it as orphaned, about five minutes later on the stack that was measured, and runs them again, whatever their retries (as of Airflow 3.3.2, October 2026), so write tasks that are safe to run twice. With the CeleryExecutor, the tasks run in the container of the worker, apart from the scheduler ([above](#run-the-tasks-on-a-separate-worker)).
- **After an update, `up` waits ten minutes and fails because `metabase-init` did not complete, with `Metabase at http://metabase:3000 did not become healthy` in `./agoranomos logs metabase-init`.** Docker Compose recreated the network of the stack but kept the containers and joined them to it without the names of their services, which `up` and `restore` prevent by stopping the stack first; it happens after a plain `docker compose up` that follows a change of the network. Run `./agoranomos down` and then `./agoranomos up`: the data stays in the Docker volumes ([Deploy on a server](deploy-on-a-server.md#if-something-goes-wrong) has the cause).
- **`up` stops on `airflow-scheduler`, which is running and unhealthy.** After a task has reached its memory limit twice in the life of the scheduler, the kernel has stopped serve-logs and then the health check of the scheduler, as well as the tasks ([System requirements](../reference/system-requirements.md#when-a-service-reaches-its-limit)); the tasks go on, but `up` waits for a healthy scheduler, does not restart a container that runs, and the log of the scheduler does not say why. When no task runs, run `docker compose restart airflow-scheduler` in the checkout, then `./agoranomos up`: a restart runs the tasks that were running in the scheduler again.
- **`up` stops on `marts-init`.** dbt could not load the seeds or build the views of the marts: `./agoranomos logs marts-init` shows its lines and the seed or the model that failed. On a warehouse where no transform has built the versions of the core yet, the job builds what the views read, with its tests, so a test that fails there stops it as it stops the transform: the log names the test. When the log says that the lock did not come, a run of the transform wrote for more than 11 minutes: run `./agoranomos up` again when it has ended ([The views of the marts](../reference/configuration.md#the-views-of-the-marts)).
- **The demo is not ready.** [See the demo](see-the-demo.md) says what `demo-init` does and how to read its log.

[Command line](../reference/command-line.md) and [Configuration](../reference/configuration.md) describe every command and setting.
