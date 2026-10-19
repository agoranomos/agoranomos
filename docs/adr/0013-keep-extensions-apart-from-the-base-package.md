---
status: accepted
date: 2026-10-05
decision-makers: Andrei Gromov
---

# Keep extensions apart from the base package

The overrides of the metadata of the marts in `extensions/metadata/`, an extension point that is data and not code, are decided in [ADR-0016](0016-keep-the-metadata-of-the-marts-as-code.md).

## Context and Problem Statement

Each release of Agoranomos is to ship a base package: the Compose stack, the pipelines, the dbt models and the dashboards. The people who run it, and the AI agents they work with, add their own work on top: DAGs for their own jobs, dbt models for their own metrics and, later, data sources that the base package does not have. A release must be able to replace the base package without overwriting that work, and an AI agent must know where its changes belong. This record covers code only: the questions and dashboards that people make in Metabase are saved in Metabase's own database.

Where does the code that a deployment adds live, and how does the stack load it?

## Decision Drivers

* An update of the base package never overwrites or deletes what a deployment added.
* There is one obvious place for additions, for people and for AI agents.
* The owners of the additions can keep them under version control, apart from the Agoranomos repository.
* The stack loads additions in a defined way, so that a broken addition shows up as an error and not as a silent change of the base package.
* Code that runs in the stack can read the folders it is loaded from, but not rewrite them.

## Considered Options

* A separate `extensions/` folder in each deployment, outside the version control of the base package, with defined extension points
* Changing the files of the base package in place, in a fork or by local edits
* Separate packages that a deployment installs, such as Python or dbt packages
* Changes made only through the user interfaces of Airflow and Metabase

## Decision Outcome

Chosen option: "A separate `extensions/` folder in each deployment, outside the version control of the base package, with defined extension points", because updates of the base package never overwrite what a deployment puts there, it is one place for people and AI agents alike, and each tool loads it through its own mechanism.

In practice this means:

* **The folder.** `extensions/` in the root of a deployment holds the code that the deployment adds: today its DAGs, and later its dbt models and data sources. The `AGORANOMOS_EXTENSIONS_DIR` setting in `.env` can point it elsewhere. The repository is to track only `extensions/README.md` and to ignore everything else in the folder, so that `git pull` and new releases leave the deployment's files alone. A team can keep the folder in a git repository of its own.
* **Releases never overwrite it.** Releases change only the files of the base package, and never overwrite or delete what a deployment added. `./agoranomos init` creates the folder when it is missing ([ADR-0014](0014-start-the-stack-with-one-script.md)).
* **Airflow DAGs are the first extension point.** Airflow loads DAGs from two DAG bundles: `agoranomos`, the DAGs of the base package in `airflow/dags`, and `extensions`, the DAGs in `extensions/dags`. Both folders are mounted read-only into the Airflow containers, and the Airflow UI shows which bundle each DAG comes from.
* **Later extension points follow the same rule.** dbt models and data sources are to get their own subfolders when the stack gains dbt and more sources, each described in a record of its own.

The other options were not chosen for these reasons:

* **Changing the base package in place.** Every release would conflict with the local changes, and it would be unclear which code belongs to Agoranomos and which to the deployment.
* **Separate packages.** They suit developers who publish reusable code, but every change needs a build and an installation step, which is too heavy for the small additions that a business makes with an AI agent.
* **Only the user interfaces.** Airflow DAGs and dbt models are code and cannot be written there, and changes made only in a user interface have no review and no history.

### Consequences

* Good, because releases never overwrite a deployment's own work.
* Good, because people and AI agents find additions in one place, and the base package stays recognizable.
* Good, because the folder can be versioned, reviewed and backed up on its own.
* Bad, because the DAGs in `extensions` run on the same Airflow workers as the DAGs of the base package, with the same permissions: the workers hold the password of the Airflow metadata database, and later also the credentials of the warehouse and of the ad platforms. Whoever can write to the folder, a person or an AI agent, can run code with these permissions, outside the limits that [ADR-0007](0007-ai-access-via-mcp-read-only.md) sets for AI access through MCP. Whether the workers can do without the password of the metadata database is to be checked during Phase 1.
* Bad, because an extension can break when a release changes something it depends on. Keeping extensions compatible is up to their owners, and release notes have to name such changes.
* Bad, because the CI of Agoranomos does not test the extensions of a deployment. It is to test only that Airflow loads and runs a DAG from the folder.

## More Information

* **Implementation status, as of October 2026:** in place for v0.1.0: the extensions folder, which `./agoranomos init` creates and git ignores apart from its README, with its `dags` subfolder and the `metadata` subfolder of [ADR-0016](0016-keep-the-metadata-of-the-marts-as-code.md); the two DAG bundles, `agoranomos` and `extensions`; and a stack test that runs a DAG from the folder. For v0.2.0 the folder also has the `model` subfolder, with the settings of the model of a deployment, `clients.toml` and `conversion_actions.toml` ([ADR-0024](0024-one-deployment-is-one-business.md), [ADR-0025](0025-model-advertising-statistics-once.md)), which the Airflow services mount read-only and the first task of the transform loads. dbt models and data sources of a deployment are planned.
* Checked in October 2026, with both executors: task code can read the password of the Airflow metadata database from the environment of the process that runs it, so the question above is answered "not as built" ([ADR-0015](0015-run-localexecutor-by-default.md)). From v0.1.5 the Celery worker holds neither that password nor the key that signs the tokens of Airflow, but a task there can still run code in the scheduler, which holds both, through the results of Celery, which the scheduler unpickles ([ADR-0015](0015-run-localexecutor-by-default.md)); with the LocalExecutor, the default, the scheduler runs the tasks and holds both.
* Since the demo pipeline (October 2026), the credentials of the warehouse that the consequences above expect "later" are there: the process that runs the tasks, the scheduler with LocalExecutor and the worker with CeleryExecutor, also holds the passwords of the warehouse roles `loader` and `transformer`, as `WAREHOUSE_LOADER_PASSWORD` and `DBT_ENV_SECRET_WAREHOUSE_PASSWORD`, so the code of a DAG in `extensions` can read them. The DAG processor, which parses the DAGs, holds neither.
* Airflow 3 loads DAGs from [DAG bundles](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-bundles.html). A local bundle always runs the latest code in its folder. A Git bundle records the commit that each DAG run used, which may later suit an extensions folder kept in git.
* [Home Assistant](https://developers.home-assistant.io/docs/creating_integration_file_structure/) takes a similar approach: the integrations a user adds live in the `custom_components` folder of the configuration directory, apart from the code of Home Assistant itself.
* The [architecture overview](../explanation/architecture.md) describes the stack, and [ADR-0007](0007-ai-access-via-mcp-read-only.md) covers AI access to it.
