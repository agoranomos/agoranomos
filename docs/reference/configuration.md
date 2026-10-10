# Configuration

Agoranomos reads its settings from the `.env` file in the root of the checkout. `./agoranomos init` creates it from `.env.example` and fills in every secret that is empty ([Command line](command-line.md)). Only its owner can read `.env`, and it is never committed.

!!! warning "Keep a copy of `.env`"

    `.env` holds the keys that decrypt the connections Airflow and Metabase save in their databases. Without them, the connections in a restored backup of those databases cannot be decrypted. Keep a copy in a password manager.

    Before an upgrade of Metabase, back up its database as well: [Back up Metabase before an upgrade](#back-up-metabase-before-an-upgrade).

`./agoranomos` reads each setting as Docker Compose does (compose-go 2.16.1, as of October 2026) for lines of the form `KEY=value`: the last line that sets a setting counts, a value in single or double quotes is what is between the quotes, and a value without quotes ends where a space and `#` start a comment, without the blanks and the carriage return at its end, which a `.env` saved with the line endings of Windows has. After an empty value, a space and `#` do not start a comment: Compose reads `KEY= # note` as the value `# note`, and `./agoranomos preflight` stops on such a line, so put a comment on a line of its own. Write each setting at the start of its line, without `export` and without blanks around the `=`, keep each value on one line, and put a value with spaces, a `#` or a `$` in single quotes: Compose expands `$` and reads escapes such as `\"` outside single quotes, and `./agoranomos` does neither.

## Docker Compose

| Setting | Default | What it does |
|---|---|---|
| `COMPOSE_PROJECT_NAME` | `agoranomos` | The name of the Compose project. It starts the names of the containers, networks and volumes. |
| `COMPOSE_PROFILES` | `core,demo` | The profiles that run. `core` is the whole stack in local mode, and `demo` adds the demo: on the first start, the one-shot `demo-init` job runs the demo pipeline and creates a demo dashboard in Metabase ([See the demo](../how-to/see-the-demo.md)). Set `core` to start without it. `server` adds the proxy of server mode, which `./agoranomos init --server` adds ([Server mode](#server-mode), [ADR-0019](../adr/0019-open-only-metabase-in-server-mode.md)). |
| `COMPOSE_FILE` | `compose.yaml` | The Compose files of the stack. `compose.yaml` runs Airflow with LocalExecutor, which needs no broker. `compose.yaml:compose.celery.yaml` adds the Celery broker and a separate worker (CeleryExecutor), for which 12 GB of memory is recommended ([System requirements](system-requirements.md)). |

## Stack

| Setting | Default | What it does |
|---|---|---|
| `AGORANOMOS_ADMIN_EMAIL` | `admin@example.com` | The email of the admin account of Metabase and Airflow (user name `admin` in Airflow), created on the first start. Change it before the first start. |
| `AGORANOMOS_EXTENSIONS_DIR` | `./extensions` | The folder for the deployment's own extensions ([ADR-0013](../adr/0013-keep-extensions-apart-from-the-base-package.md)): its DAGs, and its overrides of the metadata of the marts ([below](#the-metadata-of-the-marts)). Releases never overwrite it. A relative path starts at the checkout. |
| `AIRFLOW_PORT` | `8080` | The port of the Airflow UI and API on `127.0.0.1`. |
| `METABASE_PORT` | `3000` | The port of Metabase on `127.0.0.1`. |
| `AIRFLOW_PARALLELISM` | `4` | How many Airflow tasks run at the same time at most, in the whole stack. With LocalExecutor, each one is a process in the scheduler container, so more tasks need more memory; when the container reaches its limit, the kernel stops a task before the scheduler on a host whose kernel restricts ptrace with Yama, such as Ubuntu, and can stop the scheduler on one without it, such as Docker Desktop for macOS ([System requirements](system-requirements.md#when-a-service-reaches-its-limit)). With CeleryExecutor it caps all workers together: raise it together with `AIRFLOW_WORKER_CONCURRENCY` or the number of workers. |
| `AIRFLOW_WORKER_CONCURRENCY` | `4` | Only with CeleryExecutor: how many tasks one Airflow worker runs at a time. More tasks need more memory: raise `AIRFLOW_TASKS_MEMORY`, the limit of the worker, with it. |

## Server mode

Server mode puts Metabase on the internet behind Caddy, with HTTPS, and opens the ports 80 and 443 of the host and no other ([ADR-0019](../adr/0019-open-only-metabase-in-server-mode.md)). Airflow and Metabase stay on `127.0.0.1` too, for SSH tunnels.

`./agoranomos init --server <domain>` turns it on: it sets `AGORANOMOS_SITE_URL=https://<domain>`, adds the `server` profile to `COMPOSE_PROFILES`, and sets the two rules of Metabase below where they are not set. To go back to local mode, run `./agoranomos down` first, while `server` is still in `COMPOSE_PROFILES`: Compose stops only the services of the active profiles, so after you take `server` out, the `proxy` container would keep running on ports 80 and 443. Then take `server` out of `COMPOSE_PROFILES`, delete the lines `AGORANOMOS_SITE_URL`, `METABASE_PASSWORD_COMPLEXITY` and `METABASE_SESSION_TIMEOUT` of `.env`, and run `./agoranomos up`: Metabase then has its address on `localhost` again.

| Setting | Default | What it does |
|---|---|---|
| `AGORANOMOS_SITE_URL` | `http://localhost:` and `METABASE_PORT` | The address of Metabase, which it uses in its links. In server mode it is `https://` and the domain name of the server, which Caddy serves; `./agoranomos init --server <domain>` sets it. It is not in `.env.example`, so that local mode keeps the default. |
| `AGORANOMOS_TLS` | `acme` | Where Caddy gets the certificate of the site: `acme`, from the ACME directory below, or `internal`, from its own authority, which no browser trusts, for tests. |
| `AGORANOMOS_ACME_CA` | `https://acme-v02.api.letsencrypt.org/directory` | The ACME directory of Let's Encrypt. For a trial, its staging directory, `https://acme-staging-v02.api.letsencrypt.org/directory`, has higher limits and gives certificates that browsers do not trust. Let's Encrypt issues at most five certificates for the same names in seven days (as of October 2026). |
| `METABASE_PASSWORD_COMPLEXITY` | `normal` | The rule of Metabase for new and changed passwords: `normal`, 6 characters with a digit, or `strong-enough`, at least 15 characters, which `./agoranomos init --server` sets. With `strong-enough`, in local mode as well, a `METABASE_ADMIN_PASSWORD` that you write by hand must have at least 15 characters too, and `./agoranomos preflight` stops on a shorter one; the one that `init` generates has 32. Like `AGORANOMOS_SITE_URL`, it is not in `.env.example`. |
| `METABASE_SESSION_TIMEOUT` | none | How long a person stays signed in to Metabase without activity, as JSON: `./agoranomos init --server` sets `'{"amount":24,"unit":"hours"}'`; the unit is `seconds`, `minutes` or `hours`. Empty, Metabase keeps a session until it expires, after two weeks. Not in `.env.example`. |

Caddy keeps its certificates in the Docker volume `caddy-data`, which backups leave out: a lost volume means a new certificate. It sends Let's Encrypt no contact email, as Let's Encrypt has sent no expiration emails since June 2025, and Caddy renews the certificate by itself.

## Memory limits

Every service has a memory limit, also for swap, so that the stack stays within the memory of [System requirements](system-requirements.md) and, on the recommended memory (8 GB, or 12 GB for the CeleryExecutor variant), a service that runs short of memory cannot take it from the others ([System requirements](system-requirements.md#memory-limits) says what holds on the minimum of 6 GB). The limits of the services that run all the time are these settings, with defaults from measurements on the demo; those of the one-shot setup jobs are fixed in `compose.yaml`. A value is a number of MiB with `m`, or of GiB with `g`: `preflight` stops on a number without a unit, which Compose would read as bytes, and on a size below 6 MiB, the smallest limit that Docker accepts.

| Setting | Default | Service |
|---|---|---|
| `WAREHOUSE_DB_MEMORY` | `256m` | `warehouse-db`, the warehouse |
| `PLATFORM_DB_MEMORY` | `384m` | `platform-db`, the databases of Airflow and Metabase |
| `AIRFLOW_APISERVER_MEMORY` | `512m` | `airflow-apiserver` |
| `AIRFLOW_DAG_PROCESSOR_MEMORY` | `768m` | `airflow-dag-processor`, which runs `dbt ls` each time that it parses the transform DAG (below) |
| `AIRFLOW_TRIGGERER_MEMORY` | `640m` | `airflow-triggerer` |
| `AIRFLOW_TASKS_MEMORY` | `1280m` | The service that runs the Airflow tasks: `airflow-scheduler` with LocalExecutor, `airflow-worker` with CeleryExecutor |
| `METABASE_MEMORY` | `2176m` | `metabase`; half of it is the heap of Java |
| `PROXY_MEMORY` | `256m` | Server mode only: `proxy`, the Caddy in front of Metabase |
| `AIRFLOW_SCHEDULER_MEMORY` | `640m` | Only with CeleryExecutor: `airflow-scheduler`, which then runs no task |
| `BROKER_MEMORY` | `256m` | Only with CeleryExecutor: `broker` |

When a service needs more, raise its limit in `.env` and run `./agoranomos up`, which starts it again with the new limit. A one-shot job has no setting here: [System requirements](system-requirements.md#when-a-service-reaches-its-limit) says how one that reaches its limit shows, and what to do. The tasks of heavier pipelines need more than the demo, and so do more tasks at the same time: with LocalExecutor, up to `AIRFLOW_PARALLELISM` tasks run in the scheduler at once, so raise `AIRFLOW_TASKS_MEMORY` with it. Each limit that you raise needs memory on the machine as well: [System requirements](system-requirements.md#memory-limits) says how the defaults add up. When a service reaches its limit, the kernel stops one of its processes ([System requirements](system-requirements.md#when-a-service-reaches-its-limit)).

## Settings fixed in `compose.yaml`

Some settings are not in `.env`, because changing them is a decision and not a preference. They are environment variables in `compose.yaml`, and a value from an environment variable wins over any value saved in the application.

The AI switches of Metabase are fixed off in `compose.yaml` until the AI layer ships, which is to set them from the level of AI access in `.env` ([ADR-0017](../adr/0017-set-ai-access-in-metabase-by-levels.md)): `MB_AI_FEATURES_ENABLED`, `MB_MCP_ENABLED` and `MB_AGENT_API_ENABLED` are all `false`, which keeps the built-in MCP server, the registration of its OAuth clients and the Agent API off. The switch in Admin > AI has no effect while they are. Changing it saves a value that would apply if a variable were removed, so leave it alone.

The switches do not cover the keys of LLM providers, which Metabase uses for its own AI features, such as Metabot and the generation of SQL. Metabase 0.64 keeps them in a list of provider connections, to which an admin can add, in Admin > AI and whatever the switches say, a connection with the key of one of eleven providers, such as Anthropic, OpenAI or Google. An Anthropic key also opens `/api/llm/generate-sql`, which does not read the master switch, as in Metabase 0.63. So `compose.yaml` also sets `MB_LLM_PROVIDERS` to an empty list, `[]`: the list then comes from the environment, and Metabase refuses to change it, in Admin > AI and through its provider API, with a message that names the variable (checked on Metabase 0.64.1, as of October 2026). The lock covers the provider API only. The older setting of each provider, such as `llm-anthropic-api-key` or `llm-openai-api-key` (`PUT /api/setting/…`), still saves a value into the stored list, which has no effect while the variable is set. While the variable is set, Metabase neither shows that stored list nor lets anyone clear it through its API, and the list applies as soon as the variable is removed: a change that removes `MB_LLM_PROVIDERS` should check Admin > AI at the first start without it, which is when that list shows. A variable of one provider, such as `MB_LLM_ANTHROPIC_API_KEY`, would still add a connection; `compose.yaml` sets none, and the stack tests check that the list is empty.

Metabase keeps the addresses of its users at home too. On a sign-in from a new device, Metabase 0.63 and 0.64 send the IP address of the sign-in to `get.geojs.io`, a service on the internet, to name its place in an email to the user. `MB_SEND_EMAIL_ON_FIRST_LOGIN_FROM_NEW_DEVICE=false` turns that email off, and the question with it. The stack sets up no email server, so no email is lost; if you set up email in Metabase, it still sends no email about a sign-in from a new device, because this setting can be changed only from the environment. The login history in Account settings still asks where each address is, so `compose.yaml` also gives the container of Metabase the name `get.geojs.io` for its own address, where nothing listens: the question reaches no one, the login history shows no place, and the log of Metabase has an `ERROR` line, `Error geocoding IP addresses: Connection refused`, each time that someone opens the login history (checked on Metabase 0.64.1, as of October 2026). `MB_AI_EVAL_CAPTURE=false` keeps off a feature of Metabase 0.64 that would write the traces of its AI features, prompts and answers with the data that they carry, into files for an admin to read; it is off by default, and it can be turned on only from the environment.

dlt, dbt and Cosmos send anonymous usage statistics by default, and four variables in `compose.yaml`, which the Airflow image sets too, turn that off: `RUNTIME__DLTHUB_TELEMETRY=false` for dlt, `DBT_ENGINE_SEND_ANONYMOUS_USAGE_STATS=False` for dbt, `AIRFLOW__COSMOS__ENABLE_TELEMETRY=False` for Cosmos, and `DO_NOT_TRACK=1`, which dbt and Cosmos also honor. The dbt project says the same with `send_anonymous_usage_stats: false` in `dbt/dbt_project.yml`. Metabase reports nothing to its makers either: `MB_ANON_TRACKING_ENABLED` and `MB_CHECK_FOR_UPDATES` are `false`.

A connection of Metabase to a database may go to a private network, where the warehouse is, or to a public address on the internet, but not to a loopback or link-local address, such as the metadata service of a cloud at `169.254.169.254`, or to another address that is neither private nor public, such as the documentation range `192.0.2.0/24` (checked on Metabase 0.64.1, as of October 2026): `MB_WAREHOUSE_ALLOWED_NETWORKS=allow-private`, which also covers the SSH tunnels of connections and the URLs of their authentication providers. Metabase reaches the internet only for what an admin sets up, such as email or Slack, which Agoranomos does not set up ([ADR-0021](../adr/0021-leave-the-outgoing-traffic-of-metabase-open.md)). In server mode the proxy closes the two endpoints of Metabase that forward requests to the servers of Metabase without a sign-in, `/api/analytics-proxy` and `/api/product-feedback`.

dlt reads no settings or secrets from Airflow Variables: `PROVIDERS__ENABLE_AIRFLOW_SECRETS=false` is set in `compose.yaml`, and in the Airflow image too. Inside an Airflow task, dlt would otherwise also take its settings from Variables, and a person who may edit them, such as a user with the Op role of Airflow, could redirect a pipeline: for example, send the connection of a load, with its password, to another host. So a DAG in the extensions folder ([ADR-0013](../adr/0013-keep-extensions-apart-from-the-base-package.md)) that follows dlt's guide for Airflow cannot keep its settings in a `dlt_secrets_toml` Variable, which dlt ignores here: the pipeline takes them from environment variables instead.

The two caches from which Cosmos renders the tasks of dbt stay out of Airflow Variables: `AIRFLOW__COSMOS__ENABLE_CACHE_DBT_LS=False` and `AIRFLOW__COSMOS__ENABLE_CACHE_DBT_YAML_SELECTORS=False` are set in `compose.yaml`, and in the Airflow image too. By default Cosmos saves the output of `dbt ls`, from which it renders the tasks of dbt and their arguments, in a Variable named `cosmos_cache__<DAG>__<task group>`, and the YAML selectors of a dbt project the same way: a person who may edit Variables could then change what a dbt task runs. A `cosmos_cache__` Variable that was saved before is no longer read, and you can delete it.

Without the caches, `dbt ls` runs each time that the DAG processor parses a DAG of Cosmos, and again when each task of such a DAG starts, since a task parses the file of its DAG first. For the demo project that takes 1.2 to 2 seconds in the DAG processor and 1.3 to 1.7 seconds at the start of each task of `agoranomos_demo_transform` (measured on an arm64 machine). That DAG has a file of its own, so the task of `agoranomos_demo_load` does not wait for `dbt ls`.

A DAG in the extensions folder should not use two features of Cosmos that still keep state in Variables, and that the stack does not use: `SeedRenderingBehavior.WHEN_SEED_CHANGES`, which keeps `cosmos_seed_checksum__…` Variables, and the watcher execution mode, which keeps a backup of its XComs there.

## Public links

Metabase can give a question or a dashboard a public link, which anyone who has it opens without signing in. Metabase 0.64 allows them by default; `metabase-init` turns them off once, when it sets up a new Metabase, and an admin may turn them on again in Admin > Settings > Public sharing. A public link shows its question or dashboard with the values of the filters in its address, and whoever has the link can take them out of the address and see what the filters hid. A Metabase set up before this release keeps the setting that it had, so before you turn server mode on for one, check Admin > Settings > Public sharing. `metabase-init` tries three times, two seconds apart, at the first start: if all three fail, its error, which `./agoranomos logs metabase-init` shows (`./agoranomos up` does not print it), says that the links are still on and where to turn them off, and no later start tries again, so turn them off in Admin > Settings > Public sharing.

## The metadata of the marts

The display names, descriptions and semantic types of the tables and fields of the marts in Metabase come from the dbt project ([Describe the marts](describe-the-marts.md)). On every start, the one-shot job `metadata-export` exports them from dbt, and `metabase-init` applies them, with the overrides of this deployment; `demo-init` applies them too, after its sync ([ADR-0016](../adr/0016-keep-the-metadata-of-the-marts-as-code.md)).

The overrides are in `metadata/overrides.toml` in the extensions folder, which releases never change, and the file is optional. Each entry names a table of the marts, `[marts.<table>]`, or a column of one, `[marts.<table>.columns.<column>]`, with any of three properties ([Rename a field or add your own definitions](../how-to/rename-a-field.md)):

| Property | What it does | At most |
|---|---|---|
| `display_name` | Replaces the display name of the release. One line. | 100 characters |
| `description` | Replaces the description of the release. | 2,000 characters |
| `note` | Follows the description of the release after an empty line, so that a better description of a later release still reaches Metabase. | 2,000 characters |

```toml
[marts.ad_spend_daily.columns.conversions]
display_name = "Purchases"
note = "In this business, a conversion is a paid order in the online shop."
```

Each value is a text in quotes, and the file has at most 256 KiB. A table, a column or a property that dbt does not have, a value that is not a text or is empty, two columns of a table with one display name, and a mistake in the TOML are errors: the job then writes nothing in Metabase and stops with one line that names the entry, and `./agoranomos up` fails. The file is data: the jobs parse it, and nothing in it runs. Metabase OSS shows one display name for each field to everyone, so names in another language are for the whole deployment.

The code always wins. A display name, a description or a semantic type that someone changes in the Metabase UI is set back at the next start, and each change is one line of `./agoranomos logs metabase-init`, and on the first start with the demo also of `./agoranomos logs demo-init`, which applies the metadata of the mart of the demo once the demo pipeline has built it: a first fill, an update from the code, or the revert of an edit, with the value that it replaced. A line is written as soon as its change is made, so the log keeps it also when a later step of the job fails. What the code does not declare, such as whether a field is hidden, stays as it is. `./agoranomos metadata diff` prints what the next start would change, and changes nothing ([Command line](command-line.md#metadata)).

A mart, or a column that a release adds or renames, reaches the warehouse with the run of dbt that builds it, which can come after a start. Until the next start, Metabase shows it with its own name and type, and with the description of its comment from its next sync; the next start syncs it and applies its metadata. Until dbt has built it, the log of `metabase-init` has a note that says so.

## Secrets

`./agoranomos init` generates each of these when it is empty: random letters and digits, and a Fernet key for Airflow.

| Setting | What it is | Changing it later |
|---|---|---|
| `WAREHOUSE_POSTGRES_PASSWORD` | The superuser of the warehouse PostgreSQL | It is set only when the database is first created. To change it, change it in PostgreSQL too. |
| `PLATFORM_POSTGRES_PASSWORD` | The superuser of the platform PostgreSQL | As for `WAREHOUSE_POSTGRES_PASSWORD`. |
| `AIRFLOW_DB_PASSWORD` | The role that owns the metadata database of Airflow | Change it and run `./agoranomos up`: the role gets the new password. |
| `METABASE_DB_PASSWORD` | The role that owns the application database of Metabase | As for `AIRFLOW_DB_PASSWORD`. |
| `METABASE_READER_PASSWORD` | The read-only role that Metabase reads the warehouse as | As for `AIRFLOW_DB_PASSWORD`; Metabase's connection is updated too. |
| `WAREHOUSE_LOADER_PASSWORD` | The role for loading data into the raw schemas of the warehouse (dlt) | As for `AIRFLOW_DB_PASSWORD`. |
| `WAREHOUSE_TRANSFORMER_PASSWORD` | The role for building the staging and marts schemas of the warehouse (dbt) | As for `AIRFLOW_DB_PASSWORD`. |
| `VALKEY_PASSWORD` | The password of the Celery broker, used only with CeleryExecutor | Change it and run `./agoranomos up`. |
| `AIRFLOW_FERNET_KEY` | The key that encrypts the connections and variables saved in Airflow | Do not change it: what Airflow saved could no longer be decrypted. |
| `AIRFLOW_JWT_SECRET` | The key that signs the API tokens of Airflow | Change it and run `./agoranomos up`; people have to sign in again. |
| `AIRFLOW_API_SECRET_KEY` | The secret key of the Airflow API server | As for `AIRFLOW_JWT_SECRET`. |
| `AIRFLOW_ADMIN_PASSWORD` | The password of the Airflow user `admin`, set on the first start | With the `demo` profile, `demo-init` signs in with it on every start. If you change the password in Airflow, put the same value here. |
| `METABASE_ENCRYPTION_KEY` | The key that encrypts the connection details saved in Metabase | Do not change it: Metabase could no longer read its saved connections. |
| `METABASE_SESSION_KEY` | The key that signs the sessions that Metabase keeps in its application database, so that whoever can read or write that database cannot make a session of their own. At least 16 characters: Metabase does not start with a shorter one | Change it and run `./agoranomos up`: everyone has to sign in to Metabase again. The first start of a release that adds it signs everyone out once, the same way. |
| `METABASE_ADMIN_PASSWORD` | The password of the Metabase admin account | The first-start setup signs in with it on every start, and so does `demo-init`. If you change the password in Metabase, put the same value here. |

On every start, the setup job `db-init` sets each role of the warehouse (`loader`, `transformer` and `metabase_reader`) and of the platform instance (`airflow` and `metabase`) to `LOGIN`, with no `SUPERUSER`, `CREATEDB`, `CREATEROLE`, `REPLICATION` or `BYPASSRLS` attribute, and gives it the password from `.env`. So if you give one of these roles such a power by hand, the next `./agoranomos up` takes it back. On both instances, no role but the superuser can connect to the `postgres` and `template1` databases or read the statistics of the statements (`pg_stat_statements`): `airflow` and `metabase` connect only to the database that each owns, and the roles of the warehouse only to `warehouse`. `db-init` also gives `loader` and `transformer` a lock timeout of one minute: a statement that waits longer for a lock fails with `canceling statement due to lock timeout`. A load then fails and is tried again instead of waiting without end. A dbt run fails too, and the mart keeps its data: dbt renames a table that it rebuilds, which waits for the queries that read the table, and the queries that come later, such as those of Metabase, wait behind it.

`db-init` also gives `metabase_reader` a statement timeout of five minutes and a limit of 20 connections. A question or a dashboard card whose query runs longer fails with `canceling statement due to statement timeout`, before the query timeout of Metabase itself, 20 minutes; and Metabase, whose pool holds up to 15 connections to the warehouse, cannot take the connections that the pipelines need (both defaults of Metabase 0.64.1, as of October 2026).

`AIRFLOW_DB_PASSWORD` and `VALKEY_PASSWORD` are written into connection URLs. If you choose them yourself instead of letting `init` generate them, use letters and digits only: a character such as `@`, `/` or `:` breaks the URL.

## Back up Metabase before an upgrade

A release that moves Metabase to a new major version, such as from 0.63 to 0.64, migrates the application database of Metabase, the `metabase` database of `platform-db`, at the first `./agoranomos up`. Do not count on undoing the migration: as of October 2026, Metabase 0.63.19 refuses to start on a database that 0.64.1 has migrated, and after the `migrate down` command of 0.64.1 it fails on a migration of its own. The way back is a backup made before the upgrade. A large application database can take a while to migrate, because 0.64 changes the primary key of the `query_execution` table, which logs the queries that Metabase runs, and splits that table into partitions.

Before the first `./agoranomos up` of the new release, back up the database of Metabase. Run the commands in the root of the checkout, where `docker compose` finds `.env`. They need `platform-db` running, which the first command checks: it fails with `service "platform-db" is not running` after `./agoranomos down`, and `docker compose up --detach --wait platform-db` then starts it alone. The second command stops Metabase, so that nothing changes during the backup, the third makes the backup, and the last one checks that the backup reads:

```bash
docker compose exec -T platform-db pg_isready --quiet
docker compose stop metabase
(umask 077 && docker compose exec -T platform-db pg_dump --username=postgres --format=custom --dbname=metabase > ../metabase-before-upgrade.dump)
docker compose exec -T platform-db pg_restore --list < ../metabase-before-upgrade.dump > /dev/null && echo "the backup reads"
```

Go on only when the last command prints `the backup reads`. The shell creates the file even when `pg_dump` fails, and the check fails on an empty or broken file, for example with `input file is too short`.

`pg_dump` runs in the `platform-db` container and connects through the socket of the container, where the PostgreSQL image accepts a connection without a password, so no password is typed or printed. The backup lands next to the checkout, outside it, so that git never sees it, and `umask 077` makes it readable only by you. Keep it as safe as `.env`: it holds what Metabase keeps, such as the hashes of the passwords of its users and the connection details, which only the `METABASE_ENCRYPTION_KEY` of this `.env` decrypts. Then run `./agoranomos up`, which starts the new version; `./agoranomos logs metabase` shows the migration.

To go back, stop Metabase first, so that nothing is connected to its database, and restore the backup, which drops the `metabase` database and creates it again, with its owner and access rights, from the backup. Then return the checkout to the release that you came from, which pins the earlier version of Metabase, and start it:

```bash
docker compose stop metabase
docker compose exec -T platform-db pg_restore --username=postgres --clean --if-exists --create --exit-on-error --dbname=postgres < ../metabase-before-upgrade.dump
# Only when pg_restore ended without an error: return the checkout to the release that you came from, then:
./agoranomos up
```

If Metabase, or anything else, is still connected to the database, the restore stops at once with `database "metabase" is being accessed by other users` and changes nothing, because PostgreSQL does not drop a database that has connections: stop what is connected, such as Metabase, and run the restore again. `--exit-on-error` makes it stop at the first error; without it, `pg_restore` goes on after an error, and would restore what it can into the database that is still there. If the restore stops on any other error, after it dropped the database, do not start the stack: Metabase would start on an empty database. Fix the cause and run the restore again. Everything that Metabase saved after the backup is lost.
