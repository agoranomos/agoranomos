# Command line

`./agoranomos`, in the root of the checkout, sets up, checks and runs the stack ([ADR-0014](../adr/0014-start-the-stack-with-one-script.md)). It needs Docker and the standard tools of macOS, Linux or WSL2, and it works from any directory, because it finds the checkout from its own path.

!!! note "Status, as of October 2026"

    The stack is being built during Phase 1. Until it is complete, some of the commands and checks on this page belong to parts of the stack that do not exist yet.

```bash
./agoranomos <command> [options]
```

It exits with 0 when the command succeeds and with 1 when it fails, and it prints errors and warnings to standard error.

## `init`

Creates `.env` from `.env.example` if there is none, readable only by its owner (mode 600). It then adds every setting that `.env.example` has and `.env` lacks, which is how a new release brings new settings, and generates every secret that is empty. It never changes a value that is set, so running it again changes nothing. It also creates the extensions folder with its `dags` subfolder ([ADR-0013](../adr/0013-keep-extensions-apart-from-the-base-package.md)). [Configuration](configuration.md) describes the settings.

## `preflight`

Checks that this machine can run the stack. Each problem is reported with what to do about it:

| Check | Stops when | Warns when |
|---|---|---|
| Docker Engine | it is missing, not running, or older than 25.0 | — |
| Docker Compose | the plugin is missing or older than 2.24 | it is 2.37 or 2.38, or older than 5.4 with the containerd image store |
| CPU architecture | it is neither arm64 nor x86-64 with x86-64-v2 | — |
| Memory available to Docker | below 6 GB | below 8 GB |
| CPUs available to Docker | fewer than 2 | fewer than 4 |
| Free disk for Docker (Linux) | below 10 GB | below 20 GB |
| Ports `AIRFLOW_PORT` and `METABASE_PORT` | a port is in use and the stack is not running | — |

A Docker Compose that is 2.37 or 2.38, or older than 5.4 with the containerd image store, does not keep track of the image that `./agoranomos up` builds for Airflow ([docker/compose#13047](https://github.com/docker/compose/issues/13047), [docker/compose#13636](https://github.com/docker/compose/issues/13636)). The Airflow containers then restart at every start, or get an updated image only at the next one. The stack works, so preflight only warns: update Docker Compose to 5.4 or newer.

Ports are checked with `ss`, `netstat` and `lsof`, whichever of them are installed: a port counts as in use when any of them lists a listener on it. Without any of them, preflight warns that it cannot check the ports.

Docker reports a little less memory than its VM or machine has, so a machine or VM with 8 GB passes without a warning.

`--force` turns the stops on memory, CPUs and disk into warnings. Use it only if you accept that the stack may run slowly or fail.

## `up`

Prints [the banner](#the-banner), runs `init` and `preflight`, starts the stack with `docker compose up --detach`, and waits until every long-running service is healthy and every one-shot setup job has finished. It builds the Airflow image from `airflow/Dockerfile` first: the first start downloads the base image and the Python packages, from PyPI and GitHub, and takes a few minutes, and later starts take the image from the cache of Docker in seconds. With the `demo` profile, it also waits for the one-shot `demo-init` job, which runs the demo pipeline on the first start and creates the demo dashboard: 27 seconds more (measured on an arm64 machine). It waits at most 15 minutes and stops early when a service fails, naming it and the command that shows its logs. When the stack is up, it prints the addresses of Airflow and Metabase. A failed `demo-init` alone does not stop the wait, because the rest of the stack works without the demo: `up` names it and the command that shows its logs, waits until the rest of the stack is up, prints the addresses, says that the stack is up and the demo is not ready, and exits with 1. Running it again on a running stack keeps the data and the secrets; it runs the one-shot setup jobs again, which is how a changed password reaches PostgreSQL and Metabase. It waits only for the services of the profiles in `COMPOSE_PROFILES`: the container that a service of another profile left behind, such as `demo-init` after you set `COMPOSE_PROFILES=core`, does not count, and `status` still shows it; `down` with that profile on removes it. `--force` is passed to `preflight`.

## `down`

Stops the stack and removes the containers of the services of the profiles in `COMPOSE_PROFILES`, and those of services that the Compose files no longer define. A container of a service whose profile is off stays, such as the one that `demo-init` left after you set `COMPOSE_PROFILES=core`: a `down` with `COMPOSE_PROFILES=core,demo` in `.env` removes it. The data stays in the Docker volumes, and the next `up` starts with it. To delete the data as well, run `docker compose down --volumes`: this cannot be undone.

## `status`

Shows the state and health of every service, including the one-shot setup jobs.

## `logs`

Shows the last 200 lines of the logs of every service. With options or service names, passes them to `docker compose logs`, for example `./agoranomos logs -f warehouse-db` to follow one service, or `./agoranomos logs db-init` for a setup job.

## `help`

Prints the banner and lists the commands. `./agoranomos` without a command does the same.

## The banner

`up` and `help` start with the name of the project in large letters, the FIGlet font Slant, 65 columns wide and in plain ASCII, so that every terminal and every log shows it. On a terminal, the letters get a gradient from violet to teal: in 24-bit color when `COLORTERM` is `truecolor` or `24bit`, with the nearest colors of the 256-color palette when `tput colors` reports 256 or more, and in one color on a terminal with 8. The banner stays plain text when the output is not a terminal, such as a pipe or a log, when `NO_COLOR` is set to anything ([no-color.org](https://no-color.org/)), and when `TERM` is `dumb` or not set.
