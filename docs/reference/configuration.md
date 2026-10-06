# Configuration

Agoranomos reads its settings from the `.env` file in the root of the checkout. `./agoranomos init` creates it from `.env.example` and fills in every secret that is empty ([Command line](command-line.md)). Only its owner can read `.env`, and it is never committed.

!!! note "Status, as of October 2026"

    The stack is being built during Phase 1. Until it is complete, some of the settings and commands on this page belong to parts of the stack that do not exist yet.

!!! warning "Keep a copy of `.env`"

    `.env` holds the keys that decrypt the connections Airflow and Metabase save in their databases. Without them, the connections in a restored backup of those databases cannot be decrypted. Keep a copy in a password manager.

Each value is used as written: keep it on one line, do not put a comment after it, and put a value with spaces or a `$` in single quotes.

## Docker Compose

| Setting | Default | What it does |
|---|---|---|
| `COMPOSE_PROJECT_NAME` | `agoranomos` | The name of the Compose project. It starts the names of the containers, networks and volumes. |
| `COMPOSE_PROFILES` | `core` | The profiles that run. `core` is the whole stack in local mode. |
| `COMPOSE_FILE` | `compose.yaml` | The Compose files of the stack. `compose.yaml` runs Airflow with LocalExecutor, which needs no broker. `compose.yaml:compose.celery.yaml` adds the Celery broker and a separate worker (CeleryExecutor). |

## Stack

| Setting | Default | What it does |
|---|---|---|
| `AGORANOMOS_ADMIN_EMAIL` | `admin@example.com` | The email of the admin account of Metabase and Airflow (user name `admin` in Airflow), created on the first start. Change it before the first start. |
| `AGORANOMOS_EXTENSIONS_DIR` | `./extensions` | The folder for the deployment's own extensions ([ADR-0013](../adr/0013-keep-extensions-apart-from-the-base-package.md)). Releases never overwrite it. A relative path starts at the checkout. |
| `AIRFLOW_PORT` | `8080` | The port of the Airflow UI and API on `127.0.0.1`. |
| `METABASE_PORT` | `3000` | The port of Metabase on `127.0.0.1`. |
| `AIRFLOW_PARALLELISM` | `4` | How many Airflow tasks run at the same time, in the whole stack. With LocalExecutor, each one is a process in the scheduler container, so more tasks need more memory, and a task that runs out of memory can make the scheduler restart (Airflow's documentation warns about this). With CeleryExecutor it caps all workers together: raise it together with `AIRFLOW_WORKER_CONCURRENCY` or the number of workers. |
| `AIRFLOW_WORKER_CONCURRENCY` | `4` | Only with CeleryExecutor: how many tasks one Airflow worker runs at a time. More tasks need more memory. |

## Settings fixed in `compose.yaml`

Some settings are not in `.env`, because changing them is a decision and not a preference. They are environment variables in `compose.yaml`, and a value from an environment variable wins over any value saved in the application.

The AI switches of Metabase are fixed off in `compose.yaml` until the permissions of AI access are decided ([ADR-0007](../adr/0007-ai-access-via-mcp-read-only.md)): `MB_AI_FEATURES_ENABLED`, `MB_MCP_ENABLED` and `MB_AGENT_API_ENABLED` are all `false`, which keeps the built-in MCP server, the registration of its OAuth clients and the Agent API off. The switch in Admin > AI has no effect while they are. Changing it saves a value that would apply if a variable were removed, so leave it alone.

dlt reads no settings or secrets from Airflow Variables: `PROVIDERS__ENABLE_AIRFLOW_SECRETS=false` is set in `compose.yaml`, and in the Airflow image too. Inside an Airflow task, dlt would otherwise also take its settings from Variables, and a person who may edit them, such as a user with the Op role of Airflow, could redirect a pipeline: for example, send the connection of a load, with its password, to another host. So a DAG in the extensions folder ([ADR-0013](../adr/0013-keep-extensions-apart-from-the-base-package.md)) that follows dlt's guide for Airflow cannot keep its settings in a `dlt_secrets_toml` Variable, which dlt ignores here: the pipeline takes them from environment variables instead.

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
| `AIRFLOW_ADMIN_PASSWORD` | The password of the Airflow user `admin`, set on the first start | Change the password in Airflow; this value is not applied again. |
| `METABASE_ENCRYPTION_KEY` | The key that encrypts the connection details saved in Metabase | Do not change it: Metabase could no longer read its saved connections. |
| `METABASE_ADMIN_PASSWORD` | The password of the Metabase admin account | The first-start setup signs in with it on every start. If you change the password in Metabase, put the same value here. |

On every start, the setup job `db-init` sets each role of the warehouse (`loader`, `transformer` and `metabase_reader`) to `LOGIN`, with no `SUPERUSER`, `CREATEDB`, `CREATEROLE`, `REPLICATION` or `BYPASSRLS` attribute, and gives it the password from `.env`. So if you give one of these roles such a power by hand, the next `./agoranomos up` takes it back. It also gives `loader` a lock timeout of one minute: a statement of a load that waits longer for a lock fails with `canceling statement due to lock timeout`, so the load fails and is tried again instead of waiting without end.

`AIRFLOW_DB_PASSWORD` and `VALKEY_PASSWORD` are written into connection URLs. If you choose them yourself instead of letting `init` generate them, use letters and digits only: a character such as `@`, `/` or `:` breaks the URL.
