# Extensions

This folder holds what this deployment adds to Agoranomos: its own DAGs and its own words for the metadata of the marts today, and its own dbt models and data sources once the stack supports them ([ADR-0013](../docs/adr/0013-keep-extensions-apart-from-the-base-package.md)). Releases of Agoranomos never overwrite or delete what you put here, and git ignores everything in this folder except this file, so updates leave your work alone.

- `dags/`: Airflow DAGs. Airflow loads them as the `extensions` DAG bundle, next to the `agoranomos` bundle of the base package. A new file appears in Airflow within about a minute.
- `metadata/overrides.toml`: this deployment's own names, descriptions and notes for the tables and fields of the marts in Metabase ([Rename a field or add your own definitions](../docs/how-to/rename-a-field.md), [ADR-0016](../docs/adr/0016-keep-the-metadata-of-the-marts-as-code.md)). It is data: `metabase-init` and `demo-init` read it, check it before they write anything, and apply it on every start, and nothing in it runs.

Keep this folder in a git repository of your own, so that changes, including those made by AI agents, have a history and can be reviewed. Code here runs on the same Airflow workers as the DAGs of Agoranomos, with the same permissions: a task can read the environment of the process that runs it, which holds the keys of Airflow, the password of its metadata database, and the passwords of the warehouse roles `loader` and `transformer` (`WAREHOUSE_LOADER_PASSWORD`, and `WAREHOUSE_TRANSFORMER_PASSWORD` as `DBT_ENV_SECRET_WAREHOUSE_PASSWORD`). With the CeleryExecutor, a task on the worker can also read the password of the Valkey broker, which is part of `AIRFLOW__CELERY__BROKER_URL`. Review it as you would review any code that touches your data.

To keep extensions somewhere else, set `AGORANOMOS_EXTENSIONS_DIR` in `.env`.
