# Command line

`./agoranomos`, in the root of the checkout, sets up, checks and runs the stack ([ADR-0014](../adr/0014-start-the-stack-with-one-script.md)). It needs Docker and the standard tools of macOS, Linux or WSL2, and it works from any directory, because it finds the checkout from its own path.

```bash
./agoranomos <command> [options]
```

It exits with 0 when the command succeeds and with 1 when it fails, and it prints errors and warnings to standard error. `metadata diff` has exit codes of its own ([`metadata`](#metadata)).

## `init`

Creates `.env` from `.env.example` if there is none, readable only by its owner (mode 600). It then adds every setting that `.env.example` has and `.env` lacks, which is how a new release brings new settings, and generates every secret that is empty. It never changes a value that is set, so running it again changes nothing. It puts the user ID and the group ID of the person who runs it into `AGORANOMOS_UID` and `AGORANOMOS_GID`, where they are `auto`, and creates the backups folder, open to its owner only ([ADR-0020](../adr/0020-back-up-the-databases-every-day.md)). It also creates the extensions folder with its `dags` and `metadata` subfolders ([ADR-0013](../adr/0013-keep-extensions-apart-from-the-base-package.md)). [Configuration](configuration.md) describes the settings.

`./agoranomos init --server <domain>` also turns server mode on ([ADR-0019](../adr/0019-open-only-metabase-in-server-mode.md)): it sets `AGORANOMOS_SITE_URL=https://<domain>`, adds the `server` and `ops` profiles to `COMPOSE_PROFILES`, and, where they are not set, sets `AGORANOMOS_IPV6=true` (IPv6 on the network of the stack, [ADR-0023](../adr/0023-give-the-network-of-the-stack-ipv6.md)), `METABASE_PASSWORD_COMPLEXITY=strong-enough` and `METABASE_SESSION_TIMEOUT` to 24 hours ([Configuration](configuration.md#server-mode)). The domain is the name alone, such as `bi.example.com`, and its A record, and its AAAA record if it has one, must point at the server before `up` can get a certificate. For a name that Let's Encrypt issues no certificate for, such as one under `.localhost`, it adds a hint to set `AGORANOMOS_TLS=internal` for a test. It also says that a Metabase set up before this release keeps its public links, which are on by default, and to check Admin > Settings > Public sharing ([Configuration](configuration.md#public-links)). Running it again changes nothing, also in a `.env` that you edited: it writes a line only when its value changes. A plain `init`, which `up` runs, adds `AGORANOMOS_IPV6=true` to a `.env` that lists the `server` profile and lacks the setting, and says so.

## `preflight`

Checks that this machine can run the stack. Each problem is reported with what to do about it:

| Check | Stops when | Warns when |
|---|---|---|
| Docker Engine | it is missing, not running, or older than 25.0 | — |
| Docker Engine, in server mode | it is older than 28.0.0 | — |
| `AGORANOMOS_IPV6` in `.env` | it is neither `true` nor `false`, or it is `true` and Docker Engine is older than 27.0.1 | — |
| Docker Compose | the plugin is missing or older than 2.24 | it is 2.37 or 2.38, or older than 5.4 with the containerd image store |
| CPU architecture | it is neither arm64 nor x86-64 with x86-64-v2 | — |
| Memory available to Docker | below 6 GB | below 8 GB, and below 12 GB with the CeleryExecutor variant |
| CPUs available to Docker | fewer than 2 | fewer than 4 |
| Free disk for Docker (Linux) | below 10 GB | below 20 GB |
| Ports `AIRFLOW_PORT` and `METABASE_PORT` | a port is in use and the stack is not running | — |
| Ports 80 and 443, in server mode | a port is in use and the stack is not running | — |
| The site, in server mode | `AGORANOMOS_SITE_URL` is not `https://` and a domain name, `AGORANOMOS_TLS` is neither `acme` nor `internal`, or with `acme` the name cannot get a certificate of Let's Encrypt, such as `agoranomos.localhost` | — |
| The folders of `AGORANOMOS_BACKUP_DIR` and `AGORANOMOS_EXTENSIONS_DIR` | a value is not an absolute path and does not start with `./`, `../` or `~/` (`~/` is your home folder): Compose would read it as the name of a volume | — |
| The backups, with the `ops` profile | `AGORANOMOS_UID` is 0 (or all zeros), `AGORANOMOS_UID` or `AGORANOMOS_GID` is not a number, `BACKUP_TIME` is not `HH:MM`, a `BACKUP_KEEP_*` setting is not a whole number, `BACKUP_KEEP_DAILY` is 0 (or all zeros), `BACKUP_TZ` is not a time zone of the machine, or, on Linux, the backups folder exists and belongs to another user than `AGORANOMOS_UID`: the service runs as that user and cannot write into a folder of mode 700 that another user owns | On macOS, the backups folder belongs to another user than `AGORANOMOS_UID` (Docker Desktop maps the owner of a shared folder, so the service can write there) |
| Server mode without the `ops` profile | — | `COMPOSE_PROFILES` lists `server` and not `ops`: the stack runs without backups, and `./agoranomos init --server <domain>` turns them on |
| `METABASE_ADMIN_PASSWORD` in `.env`, with `METABASE_PASSWORD_COMPLEXITY=strong-enough` | it is set and has fewer than 15 characters: a new Metabase would refuse it at the first start, and for one that is set up already, `metabase-init` never changes the password of its admin, so first change it in Metabase (Account settings) to at least 15 characters (with the stack stopped, `docker compose up -d metabase` in this folder starts Metabase alone, with the databases it needs and without the proxy, at `127.0.0.1` on `METABASE_PORT`, which on a server you reach through an SSH tunnel), then put the same value in `.env` | — |
| `METABASE_SESSION_KEY` in `.env` | it has fewer than 16 characters, with which Metabase does not start | — |
| Each `*_MEMORY` setting in `.env` | it is not empty and not a number with the unit `k`, `m` or `g`, in either case, such as `2048m` or `2g`: Compose would read a number alone as bytes; or it is smaller than 6 MiB, the smallest memory limit that Docker accepts | — |
| A comment after an empty value in `.env` | a setting has a space or a tab and then `#` after its `=`, as in `KEY= # note`: Compose trims the blanks and reads `# note` as the value, a secret or the session key of Metabase among them; a `#` right after the `=`, as in `KEY=#abc`, is a value and no problem | — |

A Docker Compose that is 2.37 or 2.38, or older than 5.4 with the containerd image store, does not keep track of the image that `./agoranomos up` builds for Airflow ([docker/compose#13047](https://github.com/docker/compose/issues/13047), [docker/compose#13636](https://github.com/docker/compose/issues/13636)). The Airflow containers then restart at every start, or get an updated image only at the next one. The stack works, so preflight only warns: update Docker Compose to 5.4 or newer.

Ports are checked with `ss`, `netstat` and `lsof`, whichever of them are installed: a port counts as in use when any of them lists a listener on it. Without any of them, preflight warns that it cannot check the ports.

Docker reports a little less memory than its VM or machine has, so a machine or VM with 8 GB passes without a warning for the default variant, and one with 12 GB for the CeleryExecutor variant. Preflight takes the CeleryExecutor variant to be the one whose `COMPOSE_FILE` lists `compose.celery.yaml`.

`--force` turns the stops on memory, CPUs and disk into warnings. Use it only if you accept that the stack may run slowly or fail.

## `up`

Prints [the banner](#the-banner), runs `init` and `preflight`, stops the stack first, once, when the network that exists lacks the label of `AGORANOMOS_IPV6` or has another value or the other IPv6 setting, which every existing stack meets at the first `up` after the release that added the label; before the stop it builds and pulls the images and, with `AGORANOMOS_IPV6=true`, makes a test network with IPv6 and removes it, so that a failure of one of them leaves the stack running ([Configuration](configuration.md#server-mode)), starts the stack with `docker compose up --detach`, and waits until every long-running service is healthy and every one-shot setup job has finished. Among the jobs, `metadata-export` exports the metadata of the dbt project, and `metabase-init` applies it to the marts in Metabase ([`metadata`](#metadata)); when dbt cannot read the project, `metadata-export` fails with the error of dbt in its log, and `up` stops. It builds the Airflow image from `airflow/Dockerfile` first: the first start downloads the base image and the Python packages, from PyPI and GitHub, and takes a few minutes, and later starts take the image from the cache of Docker in seconds. With the `demo` profile, it also waits for the one-shot `demo-init` job, which runs the demo pipeline on the first start and creates the demo dashboard: 27 seconds more (measured on an arm64 machine). It waits at most 15 minutes and stops early when a service fails, naming it and the command that shows its logs; when the service exited with the code 137, the line adds that it was killed, most often because it reached its memory limit ([System requirements](system-requirements.md#when-a-service-reaches-its-limit)). A one-shot job that other services wait for, such as `airflow-init`, stops `docker compose up` itself when it fails, and the line of Docker Compose names it, such as `service "airflow-init" didn't complete successfully: exit 137`. When the stack is up, it prints the addresses of Airflow and Metabase, and that [`credentials`](#credentials) shows how to sign in. In server mode it prints the site, and Airflow on `127.0.0.1` of the server with the SSH command of the tunnel; with `AGORANOMOS_TLS=acme` it then checks, for about a minute, that the site answers with a certificate that the machine trusts, and warns when it does not: the A record of the domain does not point at the server yet, the ports 80 and 443 are closed, or `AGORANOMOS_ACME_CA` is the staging directory. Caddy waits longer and longer between its tries for a certificate; after a fix of the A record or of the firewall, `docker compose restart proxy` makes it try again at once. A `backup` service that is unhealthy, because it has no good backup in the last 26 hours, or that is restarting or has exited, does not stop the wait either: `up` warns, names `./agoranomos logs backup`, and goes on. A failed `demo-init` alone does not stop the wait, because the rest of the stack works without the demo: `up` names it and the command that shows its logs, waits until the rest of the stack is up, prints the addresses, says that the stack is up and the demo is not ready, and exits with 1. Running it again on a running stack keeps the data and the secrets; it runs the one-shot setup jobs again, which is how a changed password reaches PostgreSQL and Metabase. It waits only for the services of the profiles in `COMPOSE_PROFILES`: the container that a service of another profile left behind, such as `demo-init` after you set `COMPOSE_PROFILES=core`, does not count, and `status` still shows it; `down` with that profile on removes it. The exceptions are the proxy of server mode and the `backup` service: after `preflight`, `up` stops and removes a `proxy` container that server mode left behind when `server` is out of `COMPOSE_PROFILES`, and a `backup` container when `ops` is out, and says so, because Compose leaves them running, the old Caddy would go on serving Metabase on ports 80 and 443 with the rules of local mode, and the old service would go on taking backups. The volume `caddy-data`, with the certificates, and the backups stay. `--force` is passed to `preflight`.

## `metadata`

`./agoranomos metadata diff` shows what the next `up` would change in the metadata of the marts in Metabase: the display names, descriptions and semantic types that the dbt project and the overrides of the deployment give the tables and fields of the marts ([Configuration](configuration.md#the-metadata-of-the-marts)). It runs `metabase-init` in a container of its own, which prints one line for each change, and changes nothing. It needs Metabase to be running, and it reads the export that the last `up` made: after a change to the dbt project, run `up` first, and run it first after an update that changes the network of the stack too: until then `metadata diff` exits with 2 and says so, since Compose cannot run it on the old network.

It exits with 0 when Metabase shows what the code says, with 1 when there is something to change, and with 2 when the check could not run, also when Docker Compose could not start its container, with one line that says why.

## `credentials`

Shows where to sign in to Airflow and Metabase and as whom: the address of each, with the ports of `.env`, the user name `admin` of Airflow, the email of the admin of Metabase (`AGORANOMOS_ADMIN_EMAIL`), and the settings of `.env` that hold the two passwords, `AIRFLOW_ADMIN_PASSWORD` and `METABASE_ADMIN_PASSWORD`. It never prints a password, so that none lands in the scrollback of a terminal, in a log or on a shared screen: open `.env`, which only you can read, or the password manager that keeps its copy. It needs `.env`, and it does not need the stack to run. In server mode it names the site of Metabase, `AGORANOMOS_SITE_URL`, and Airflow on `127.0.0.1` of the server, which you reach through an SSH tunnel, such as `ssh -L 8080:127.0.0.1:8080 <user>@<server>`.

## `down`

Stops the stack and removes the containers of the services of the profiles in `COMPOSE_PROFILES`, and those of services that the Compose files no longer define. A container of a service whose profile is off stays, such as the one that `demo-init` left after you set `COMPOSE_PROFILES=core`: a `down` with `COMPOSE_PROFILES=core,demo` in `.env` removes it. The exceptions are the proxy of server mode and the `backup` service: `down` first stops and removes a `proxy` container that server mode left behind when `server` is out of `COMPOSE_PROFILES`, and a `backup` container when `ops` is out, and says so, because the old Caddy would go on serving Metabase on ports 80 and 443; the volume `caddy-data` and the backups stay. The data stays in the Docker volumes, and the next `up` starts with it. To delete the data as well, run `docker compose down --volumes`: this cannot be undone.

## `backup`

Takes a backup of the databases now, into the backups folder ([Configuration](configuration.md#backups)): in the `backup` service when it runs, and in a container of its own otherwise, such as in local mode. A service that was made before you changed `AGORANOMOS_BACKUP_DIR`, `AGORANOMOS_UID` or `AGORANOMOS_GID` and did not run `up` since still has the old folder or user, so `backup` says so and takes the backup in a container of its own, with the current ones: it goes into the folder that `.env` names now. The databases must run. It prints the name of the backup, such as `20261011T030012Z`, and its size, and it removes the scheduled backups that the retention no longer keeps. The backup that it takes stays until you delete it: the retention removes scheduled backups only. While another backup runs, such as the scheduled one, it waits for it, up to 10 minutes. It makes the backups folder first, open to its owner only, as `init` does, and stops on `AGORANOMOS_UID=0`, because the backups would belong to root, and on Linux when the folder belongs to another user than `AGORANOMOS_UID`, because the container could not write into it. Take the backup before you update the checkout with `git pull`, such as before an upgrade: a backup in a container of its own records the images of the checkout as they are when it runs, and the manifest of a backup that follows the update would name the new versions for the data of the old ones.

## `restore`

`./agoranomos restore <backup>` puts the databases back as a backup has them; `<backup>` is the name of its folder, such as `20261011T030012Z`, and `latest` names the last good backup, which `./agoranomos status` shows. `--only metabase`, or a list such as `--only warehouse,airflow`, restores some of them: give `--only` once, and each database once.

Before it asks or changes anything, it runs the checks of `preflight`, without `--force`: on a machine where `up` needs `--force`, because Docker has less memory or disk than the minimum, `restore` stops too, and its own `--force` is about the images only. On Linux it also stops when the backups folder belongs to another user than `AGORANOMOS_UID`, as `backup` does, and on `AGORANOMOS_UID=0`. It then checks the backup, in the running `backup` service when the `ops` profile is on and the service runs, and in a container of its own otherwise, as `AGORANOMOS_UID` either way (a service that was made before you changed `AGORANOMOS_BACKUP_DIR` or `AGORANOMOS_UID` and did not run `up` since still has the old folder or user, so `restore` says so and checks in a container of its own, with the current ones): every file against its SHA-256, and each dump that it restores read to its end. While a backup runs, the check waits for it, up to 10 minutes; and while the check runs, it holds a shared lock on the backups folder, so that a scheduled backup that comes due then fails and is tried again an hour later. It compares the images that the backup records with those of the checkout, for the databases that it restores: the major version of PostgreSQL, such as 18, and the tags of Airflow and Metabase. It stops when they differ, unless `--force`. On a terminal it then asks you to type `yes`, on standard error, so that you see it when the output goes to a file; elsewhere it needs `--yes`.

Only after the answer does it change anything: it stops and removes a `proxy` or `backup` container that a mode left behind, as `up` does, stops the whole stack first if the network that exists is not the one that `AGORANOMOS_IPV6` asks for, as `up` does, stops every service but the databases, runs `db-init`, restores each database inside its container with `pg_restore --clean --if-exists --create --exit-on-error`, runs `ANALYZE`, and starts the stack. The site is down while it runs. A refused restore, whether the check, the images or your answer refuses it, changes nothing.

It needs the `.env` of the deployment that took the backup, and free disk for Docker of the size of the biggest dump, which it copies into the container of its database for the time of the restore. A restore runs inside the memory limit of the database: before you restore a warehouse of several GB, raise `WAREHOUSE_DB_MEMORY` ([Configuration](configuration.md#memory-limits)). A scheduled backup near the end of its retention can be removed between the check and the copy, so restore such a backup away from `BACKUP_TIME`, or copy the backup out of the backups folder by hand first. Do not take a backup by hand while a restore runs: it could dump a database that is half restored.

When a restore fails, or you interrupt it with Ctrl-C, after it stopped the services, it says which databases it restored and which not, that the services are stopped, and what to do: when no database was restored, `./agoranomos up` starts the stack again as it was; when a database may be incomplete, restore again before `up`. It removes the copy of the dump that it left in the container. When `pg_restore` says that the database is being accessed by other users, that database was not changed: stop what is connected and run `restore` again. It does not unpack the archive of the extensions folder.

To go back from an upgrade of Metabase, restore `--only metabase` from the backup that you took before it ([Configuration](configuration.md#back-up-metabase-before-an-upgrade)). When the same update also changed the Airflow image, which migrates the database of Airflow one way too, restore `--only metabase,airflow`. See whether it did before you return the checkout, while it is still at the new release: the command prints the old and the new `FROM` line when Airflow changed, and nothing otherwise.

```bash
git diff <the commit that you came from> HEAD -- airflow/Dockerfile | grep '^[-+]FROM'
./agoranomos restore <backup> --only metabase,airflow
```

## `status`

Shows the state and health of every service, including the one-shot setup jobs. It then says when the last good backup was, lists the backups by hand with their size and age, and warns when the last run failed, when the `backup` service is unhealthy, which means that it has no good backup in the last 26 hours, or, with the `ops` profile, when the last good backup is older than that. Without the `ops` profile it says how old the last good backup is and does not warn, as backups by hand come when you take them. It also prints the free space of the disk of the backups folder, which by default is the disk that holds the data of Docker.

## `logs`

Shows the last 200 lines of the logs of every service. With options or service names, passes them to `docker compose logs`, for example `./agoranomos logs -f warehouse-db` to follow one service, or `./agoranomos logs db-init` for a setup job.

## `help`

Prints the banner and lists the commands. `./agoranomos` without a command does the same.

## The banner

`up` and `help` start with the mark of the project and its name:

```text
  ██
██  ██  agoranomos
        marketing bi, out of the box.
```

The mark is three cells that together read as the letter A, and the name is in lowercase. The cells are the Unicode full block (U+2588) in the plain text too, so a terminal or a log that does not use UTF-8 shows them wrongly.

### Colors

The text reads on a dark and on a light terminal. On a terminal, the top cell of the mark is bright amber, the only bright color, and it is a graphic, not text: on a white background it is a pale block. The two bottom cells and the name use the own text color of the terminal (the name in bold), so they show on any background, and the tagline is gray.

`./agoranomos` colors a few messages too, and changes no word of them. Their colors are darker shades of the colors of the project, chosen so that each keeps a contrast of at least 4.5 to 1 against both a black and a white background (in 24-bit color, by the WCAG formula):

| Color | Where |
|---|---|
| Green | In `up`, `restore` and `preflight`: `Preflight passed:`. In `up` and `restore`: `All services are healthy.` (`All services but the backup are healthy.` when the `backup` service is unhealthy) and `The stack is up.` |
| Gray | In `up`, the names `Airflow:` and `Metabase:` before their addresses |
| Amber | In `up`, the addresses of Airflow and Metabase. On standard error, the prefix `warning:` |
| Red-orange | On standard error, the prefix `error:` |

The colors are 24-bit when `COLORTERM` is `truecolor` or `24bit`. When `tput colors` reports 256 or more, they are entries of the 256-color palette that keep at least 4.5 to 1 against both backgrounds, so their hues drift: `error:` is a muted brick red. No entry of the palette has the hue of amber and 4.5 to 1 on both, so amber is the entry that comes closest, with 4.46 to 1 against black. On a terminal with 8 colors, they are its basic green, yellow and red, and dim for gray, which its theme sets for its own background.

Each stream gets colors only when it is itself a terminal. With `./agoranomos up > up.log`, the file has none, while an `error:` on the terminal still has its color, and with `./agoranomos up 2> up.err`, the file has none and the rest of the output still does. Neither stream gets any escape codes when `NO_COLOR` is set to a value that is not empty ([no-color.org](https://no-color.org/)) or when `TERM` is `dumb` or not set.

The plain text, when there are no colors, is the same text without escape codes, with the same full blocks and no ASCII look-alike for them: the colored output with its escape codes removed is the plain output.
