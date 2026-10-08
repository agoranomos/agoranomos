---
status: accepted
date: 2026-10-05
decision-makers: Andrei Gromov
---

# Start the stack with one script and a generated .env file

## Context and Problem Statement

Agoranomos promises a stack that starts with one command on a machine with Docker. Before the first start, every internal secret has to exist: the passwords of the databases, of their roles and of the message broker; the keys with which Airflow and Metabase encrypt the connections they save, and with which Airflow signs its sessions and tokens; and the passwords of the admin accounts. No secret may have a default value. The machine also needs enough memory, CPUs and disk, and the ports of the stack must be free; when they are not, the stack fails late and in ways that are hard to read. People run the stack on macOS, on Linux servers and on Windows through WSL2.

How do people start the stack, and where do its settings and secrets live?

## Decision Drivers

* Docker is the only prerequisite.
* No secret has a default value, and nobody has to make one up.
* Problems with the machine show up before the start, with a message that says how to fix them.
* Plain `docker compose` keeps working for people who know it.
* The settings live in one file that is easy to read, to back up and to keep in a password manager.

## Considered Options

* A POSIX shell script, `./agoranomos`, and a `.env` file that it generates from `.env.example`
* `make` targets and a generated `.env` file
* A command-line tool in Python, installed on the host
* Docker secrets in files instead of a `.env` file
* Plain `docker compose` with a `.env` file written by hand

## Decision Outcome

Chosen option: "A POSIX shell script, `./agoranomos`, and a `.env` file that it generates from `.env.example`", because it needs nothing but Docker and the standard tools of macOS, Linux and WSL2, it generates every secret, and it checks the machine before the stack starts.

In practice this means:

* **Commands.** `./agoranomos init` creates `.env` from `.env.example`, generates every secret that is empty, and creates the extensions folder when it is missing ([ADR-0013](0013-keep-extensions-apart-from-the-base-package.md)). `preflight` checks the machine, and `up` runs both, starts the stack and waits until every service is healthy. Other commands stop the stack and show its state, logs and admin logins; `./agoranomos help` lists them.
* **Secrets.** `init` generates random secrets, never changes a value that is already set, and adds the settings that a new release brings in `.env.example`. Only the owner can read `.env` (mode 600), and it is never committed.
* **Fail fast.** `compose.yaml` requires every secret (`${VAR:?…}`), so `docker compose` stops with a hint to run `./agoranomos init` when one is missing.
* **Plain Compose.** `.env` also sets `COMPOSE_PROJECT_NAME` and `COMPOSE_PROFILES`, so `docker compose` commands work after `init` without extra options.
* **Preflight.** The start stops when Docker or Docker Compose is missing or older than the minimum versions (as of October 2026, Docker Engine 25.0 and Compose 2.24), when Docker has less than 6 GB of memory or fewer than 2 CPUs, when less than 10 GB of disk is free, when the CPU is neither arm64 nor x86-64 with x86-64-v2, or when a port of the stack is taken. Below the recommended 8 GB, 4 CPUs and 20 GB it warns. Preflight also warns about a Docker Compose that loses track of the image that `up` builds for Airflow (2.37 or 2.38, or older than 5.4 with the containerd image store), because the Airflow containers then restart at every start, or get an updated image only at the next one; the stack works, so this only warns. The resource figures follow the memory budget of [ADR-0010](0010-multi-arch-and-memory-budget.md), which is a target until the stack is measured, and they change with it. `--force` turns the stops on resources into warnings.
* **Windows** is supported through WSL2.

The other options were not chosen for these reasons:

* **make.** As of October 2026, it is not installed by default on Ubuntu Server or on Windows.
* **A Python tool on the host.** It needs Python on the host, and the Python of the host differs from machine to machine.
* **Docker secrets in files.** Each service reads them in its own way (`_FILE` variables for PostgreSQL and Metabase, `_CMD` variables for Airflow), not every secret can come from a file (as of October 2026, the `_FILE` variables of Metabase do not cover its encryption key), and every file has to be readable by the user inside its container. The gain is small, because whoever can run `docker inspect` already controls Docker, which amounts to root on the host.
* **A `.env` file written by hand.** People would pick weak secrets or copy them from examples.

### Consequences

* Good, because one command starts the stack on a clean machine, with no default passwords.
* Good, because problems with the machine are reported before anything starts.
* Good, because the stack stays a standard Compose project.
* Bad, because the secrets are environment variables of the containers: `docker inspect` shows them to whoever controls Docker, and the processes in a container can read them.
* Bad, because `.env` has to be kept safe: without its keys, the connections saved in a restored backup of Airflow or Metabase cannot be decrypted. `init` and the documentation are to ask people to keep a copy in a password manager.
* Bad, because a shell script is harder to test than Python code. Its tests are to run it against stand-ins for `docker`, `lsof` and `df`.
* Bad, because Windows without WSL2 is not supported.

## More Information

* **Implementation status, as of October 2026:** in place for local mode: `./agoranomos` with `init`, `preflight`, `up`, `down`, `status`, `logs`, `metadata`, `credentials` and `help`, `.env.example`, and the Compose files. `credentials` shows the admin logins and the settings of `.env` that hold their passwords, never a password. The measurements of the stack keep the resource figures of preflight ([System requirements](../reference/system-requirements.md)). Server mode is planned for v0.1.0 ([ADR-0011](0011-local-and-server-modes.md)).
* Docker Compose reads `.env` in the project directory both for variable substitution and for its own settings, such as `COMPOSE_PROFILES` ([pre-defined environment variables](https://docs.docker.com/compose/how-tos/environment-variables/envvars/)).
* Other self-hosted projects keep their secrets in a dotenv file too: Plausible Community Edition and Supabase in `.env`, and Mastodon in `.env.production`.
* The memory budget and the supported architectures are set in [ADR-0010](0010-multi-arch-and-memory-budget.md), and the two modes in [ADR-0011](0011-local-and-server-modes.md).
