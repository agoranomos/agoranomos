# Extensions

This folder holds what this deployment adds to Agoranomos: its own DAGs today, and its own dbt models and data sources once the stack supports them ([ADR-0013](../docs/adr/0013-keep-extensions-apart-from-the-base-package.md)). Releases of Agoranomos never overwrite or delete what you put here, and git ignores everything in this folder except this file, so updates leave your work alone.

> **Status, as of October 2026:** the stack is being built during Phase 1. Until it is complete, some of what this file describes belongs to parts of the stack that do not exist yet.

- `dags/`: Airflow DAGs. Airflow loads them as the `extensions` DAG bundle, next to the `agoranomos` bundle of the base package. A new file appears in Airflow within about a minute.

Keep this folder in a git repository of your own, so that changes, including those made by AI agents, have a history and can be reviewed. Code here runs on the same Airflow workers as the DAGs of Agoranomos, with the same permissions: a task can read the environment of the process that runs it, which holds the keys of Airflow, the password of its metadata database, and the passwords of the warehouse roles `loader` and `transformer` (`WAREHOUSE_LOADER_PASSWORD`, and `WAREHOUSE_TRANSFORMER_PASSWORD` as `DBT_ENV_SECRET_WAREHOUSE_PASSWORD`). Review it as you would review any code that touches your data.

To keep extensions somewhere else, set `AGORANOMOS_EXTENSIONS_DIR` in `.env`.
