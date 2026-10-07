---
status: accepted
date: 2026-10-07
decision-makers: Andrei Gromov
---

# Keep the metadata of the marts as code, with a deployment's overrides in extensions

## Context and Problem Statement

People understand the marts through their metadata, and the planned AI access through MCP is to take its context from it: the display name and the description of each table and field in Metabase, the semantic type of each field, and the comments in the warehouse. So every table and column needs a description, written to one rule, and saying the same in every channel. Metabase OSS keeps one value of each, which its admins edit in the UI, with no lock, no history and no record of who wrote it. Its sync copies a comment of PostgreSQL into a description only while the description is empty, so a changed comment never reaches it, and display names and semantic types never come from the warehouse at all. A deployment also needs words of its own: what a conversion is differs from one business to the next.

Where does the metadata of the marts live, how does it reach Metabase, and how does a deployment change it?

## Decision Drivers

* Every table and column of the marts has a description, every mart has a display name, and each of its columns a display name and a semantic type, and tests check all of them.
* One source of truth, reviewed and versioned, that says the same in Metabase, in SQL clients and to the planned AI access through MCP.
* A deployment can use its own words for good, without a change to the base package ([ADR-0013](0013-keep-extensions-apart-from-the-base-package.md)).
* A release that improves the base reaches each deployment wherever the deployment did not use its own words.
* An edit outside the code is never lost without a word.
* No new privilege and no new holder of the password of the admin of Metabase ([ADR-0007](0007-ai-access-via-mcp-read-only.md)).

## Considered Options

* The dbt YAML as the base, the overrides of a deployment in `extensions/metadata/overrides.toml`, and the setup jobs apply both to Metabase on every start
* dbt-metabase
* Comments of PostgreSQL only
* The `_metabase_metadata` table of Metabase
* The Metabase UI wins, or the jobs fill in only what is empty
* A three-way merge with a store of state
* Models of Metabase as the way to rename
* The paid features of Metabase: serialization, Remote Sync and the Library

## Decision Outcome

Chosen option: "The dbt YAML as the base, the overrides of a deployment in `extensions/metadata/overrides.toml`, and the setup jobs apply both to Metabase on every start", because each property then has one reviewed source, a deployment keeps its own words in a file that releases never change, and nothing in it needs a feature that Metabase OSS lacks.

In practice this means:

* **Two layers, both code.** The base is the YAML of the dbt project: the descriptions of the models, their columns and the tables of the sources, and, under `config: meta: metabase:`, the display name and the semantic type of every mart and of each of its columns, with the key names of dbt-metabase. The overrides of a deployment are `extensions/metadata/overrides.toml`, an extension point of ADR-0013 that is data and not code: `display_name` and `description` replace the base, and `note` follows the description of the base. The effective metadata is the override where there is one, and the base everywhere else.
* **The comments.** dbt writes the descriptions of the base into the comments of the tables, views and columns of the warehouse (`persist_docs`), for SQL clients and the planned AI access.
* **The export.** The one-shot `metadata-export` service runs `dbt ls` on the project, which is mounted read-only, and writes the metadata as JSON into a volume that only it can write. dbt resolves its doc blocks and configs itself, so no code of Agoranomos repeats the rules by which dbt merges them. The service holds no secret and has no network, and no Airflow service mounts the volume, so the code of a DAG cannot change what the jobs that hold the admin password apply.
* **The jobs.** `metabase-init`, on every start, and `demo-init`, after its sync, apply the effective metadata through the API of Metabase, which needs an admin: they are the jobs that hold the admin password already. A mart that the warehouse has and Metabase does not show yet, with the fields of its columns, gets one sync.
* **Code always wins, out loud.** Each declared property that Metabase shows otherwise is set, and one line of the log of the job says so: a first fill, an update from the code, or the revert of an edit, with the value that it replaced. What the code does not declare, such as whether a field is hidden, stays as it is. `./agoranomos metadata diff` prints the plan and applies nothing. A mistake in the file of overrides stops the job before it writes anything.
* **The questions of the demo** take the titles of their columns and axes from the effective metadata when `demo-init` creates them, and keep them after that; a question that lacks a title gets it on a later start.

### Consequences

* Good, because every channel shows one reviewed definition, and tests check it in dbt, in the comments of the warehouse and in Metabase.
* Good, because a deployment keeps its own words across releases, in a file that it can review and version, and an AI agent that changes the words of a deployment edits that file and needs no credential of Metabase.
* Good, because an improvement of the base reaches every property that a deployment did not override.
* Bad, because an edit in the Metabase UI does not last, which surprises a person who did not read the docs. The log and the dry run make it visible; they cannot make it impossible, as Metabase OSS has no lock.
* Bad, because an override hides later improvements of the base for its property. A `note` does not, so the docs advise it for descriptions.
* Bad, because the overrides reach Metabase only: the comments of the warehouse and dbt keep the words of the base.
* Bad, because a mart that a DAG run creates after a start, or a column that a release adds or renames once dbt rebuilds the mart, gets its descriptions at the next sync of Metabase, from the comments, and its display names and types only at the next start.
* Bad, because Metabase OSS has one display name for each field, so a deployment that uses its own language uses it for everyone.

## More Information

* **Implementation status, as of October 2026:** planned for v0.1.0, and in place: the descriptions, the names and types in the dbt YAML, the contract of the mart, the comments of the warehouse, the export, the jobs that apply the metadata, the overrides and the dry run.
* The other options were not chosen for these reasons:
  * **dbt-metabase** (1.7.5, as of October 2026) covers tables and fields only, shows hidden fields again and resets values by design, states no support for Metabase 0.63, has one maintainer, and would need the admin credential in one more place.
  * **Comments only.** Metabase takes a comment only into an empty description, so a changed comment never reaches it, and comments carry no display names and no types.
  * **`_metabase_metadata`** is not documented, takes strings only, and has no schema in its keys.
  * **The UI wins, or fill in only what is empty.** Improvements of the base would stop reaching the deployments, and the test that every field is described could not hold.
  * **A three-way merge** needs a store of state, which Metabase has no room for, and lets edits live outside the code. The file of overrides is the reviewed way to keep the words of a deployment.
  * **Models of Metabase** make a copy for each renaming, which drifts, and which has no home in the extensions folder. They stay the way to build new business objects.
  * **Serialization, Remote Sync and the Library** are paid, and Remote Sync leaves the metadata of tables out.
* Metabase 0.64 keeps what users and the API write in a layer of its own, and records no author for either. The jobs send explicit, non-empty values only, which both versions treat alike, with one exception: a null semantic type, which clears the type that Metabase shows for a column that the code declares as having none. Their stack tests pass on Metabase 0.64.1 (as of October 2026). Two differences that the jobs do not meet were checked on 0.63.19 and 0.64.1: a null display name of a field now clears the name that someone gave it and shows the one of the sync, where 0.63.19 ignored it, and the description of a field that someone cleared stays empty at the next sync, where 0.63.19 filled it from the comment of the column.
* Not decided here: a glossary and metrics, translation of the base, a currency of the deployment's own (with the rates of exchange), a command that turns edits in the UI into overrides, and a hash of the base in an override, which would show when an override hides a newer base.
