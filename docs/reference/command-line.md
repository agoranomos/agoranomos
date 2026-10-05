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
| Docker Compose | the plugin is missing or older than 2.24 | — |
| CPU architecture | it is neither arm64 nor x86-64 with x86-64-v2 | — |
| Memory available to Docker | below 6 GB | below 8 GB |
| CPUs available to Docker | fewer than 2 | fewer than 4 |
| Free disk for Docker (Linux) | below 10 GB | below 20 GB |
| Ports `AIRFLOW_PORT` and `METABASE_PORT` | a port is in use and the stack is not running | — |

Ports are checked with `ss`, `netstat` and `lsof`, whichever of them are installed: a port counts as in use when any of them lists a listener on it. Without any of them, preflight warns that it cannot check the ports.

Docker reports a little less memory than its VM or machine has, so a machine or VM with 8 GB passes without a warning.

`--force` turns the stops on memory, CPUs and disk into warnings. Use it only if you accept that the stack may run slowly or fail.

## `help`

Lists the commands.
