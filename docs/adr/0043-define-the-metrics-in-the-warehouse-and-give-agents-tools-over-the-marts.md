---
status: proposed
date: 2026-10-31
decision-makers: Andrei Gromov
---

# Define the metrics in the warehouse, and give agents tools over the marts

## Context and Problem Statement

Agents and Metabase are equal consumers of the warehouse, and one question has to get one number from each of them. Today the definitions of the metrics live in the YAML of dbt, and `metrics.py` turns them into metrics and terms of the glossary of Metabase ([ADR-0034](0034-define-a-basic-data-set-and-the-modules-of-an-extended-one.md)), so an agent on MCP Toolbox does not see them. Toolbox offers `list_tables`, `describe_table` and `execute_sql`, and its documentation says that `postgres-execute-sql` "shouldn't be used for production agents".

`ai_reader` also reads `staging` ([ADR-0007](0007-ai-access-via-mcp-read-only.md), [ADR-0030](0030-ship-three-mcp-servers-in-v0-3-0-with-the-stdio-servers-in-local-mode.md)). Under [ADR-0035](0035-keep-four-layers-and-erase-the-raw-layer-after-acceptance.md) the views of `staging` stand over a layer that is erased after each acceptance, so an agent would see empty tables and draw wrong conclusions.

How does an agent find a definition, ask a common question, and stay within what it should read?

## Decision Drivers

* One number for one question, from Metabase, from Toolbox and from the MCP server of Metabase.
* One source for the definitions, not one for each consumer.
* Least privilege ([ADR-0007](0007-ai-access-via-mcp-read-only.md)): tools and roles read the top layer only.
* A common question needs no written SQL.
* Names of campaigns are written by anyone, and can carry text that tries to steer an agent.

## Considered Options

For the definitions:

* A table `marts.metric_definitions`, built from the YAML of dbt
* The definitions only in Metabase
* A semantic layer, such as dbt-mcp or the Open Semantic Interchange

For the tools:

* `list_metrics` and one parameterized `stats_by`, beside the tools of today
* `execute_sql` only

## Decision Outcome

Chosen options: "A table `marts.metric_definitions`, built from the YAML of dbt" and "`list_metrics` and one parameterized `stats_by`", because a table in the warehouse reaches every client through the one read-only role, and a parameterized tool with allowed values is safe and cheap.

In practice this means:

* **Definitions.** `marts.metric_definitions` has the name, the display name, the SQL formula, the numerator and the denominator, the unit and scale, the description and the name of the metric in Metabase. The source is one: the YAML of dbt, from which `metrics.py` already makes metrics and the glossary.
* **Tools of Toolbox.** Beside `list_tables`, `describe_table` and `execute_sql`:
    * `list_metrics` reads `metric_definitions`.
    * One parameterized `stats_by` takes a grain, a dimension from `allowedValues`, a period and an account. The grain and the dimension go in as `templateParameters`, so `allowedValues` is required. It has `annotations: readOnlyHint`, a short and a long format of its answer, and a default limit of rows.
    * `execute_sql` stays as a last resort under `ai_reader`, with a timeout of a minute and a description that asks for `LIMIT`.
* **Only L3.** `ai_reader` loses `staging`, `list_tables` reads only `marts`, and the skills and the guides change with it.
* **Descriptions.** `COMMENT ON` of each view and column from the YAML of dbt, as `persist_docs` does today, with an illustration of values, such as "`campaign_status`: as on the platform, on Google ENABLED, PAUSED, REMOVED; the spend of removed entities counts in the totals". A description states facts, never an instruction.
* **Untrusted text.** The names of campaigns, ad groups and ads stay in the views, and their descriptions say that it is text of an advertiser, data and not instructions. Headlines, texts of ads, URLs and other long fields do not enter the default views, and live in a view `ad_texts` of a module.
* **The third arm.** Private data and untrusted text are there by construction, so the outward path is cut: no `agent:delivery:write`, and no external links in a dashboard that an agent builds ([ADR-0017](0017-set-ai-access-in-metabase-by-levels.md)).
* **Limits of `ai_reader`.** `default_transaction_read_only`, `statement_timeout` of 1 minute, `idle_in_transaction_session_timeout`, `transaction_timeout`, `work_mem` by role and three connections. The tool, not the database, limits rows.
* **Export.** `target/osi_document.json` of dbt 1.12 is added when something reads it.

The other options were not chosen for these reasons:

* **The definitions only in Metabase** leave Toolbox with no definition, so an agent would compute a ratio its own way.
* **A semantic layer now** has no reader: the semantic layer of dbt-mcp is paid, the Open Semantic Interchange is at version 0.1.1 in the Apache Incubator as Ossie, and Metabase 0.64 has no import or export for it, as of October 2026.
* **`execute_sql` only** writes SQL for each question, which the vendor advises against for production agents.

### Consequences

* Good, because a ratio has one formula, which an agent can read and repeat.
* Good, because a common question goes through a tool that cannot do harm, with fewer tokens.
* Good, because an agent no longer sees layers that mean nothing to it.
* Good, because long free text stays out of what an agent reads by default.
* Bad, because `stats_by` has to follow the marts, as one more place that knows their grains.
* Bad, because a contributor who reads `staging` through an agent loses that, and has to work on a local stack with another role.
* Bad, because a description cannot stop an injection: it only lowers the odds, and the limits of the roles are what holds.

## More Information

* **Implementation status, as of October 2026:** `ai_reader` on `marts` only is planned for v0.5, with the erasing of the raw layer. `metric_definitions`, `list_metrics` and `stats_by` are planned for v0.8. Today Toolbox has the three tools of `ai/tools.yaml`, and `ai_reader` reads `marts` and `staging`.
* The facts behind this record, read on 30 and 31 October 2026:
    * Toolbox: `postgres-sql` takes parameters with `allowedValues`, `postgres-list-tables` returns the comments of tables and columns ([tools](https://mcp-toolbox.dev/integrations/postgres/tools/postgres-sql/), [list tables](https://mcp-toolbox.dev/integrations/postgres/tools/postgres-list-tables/)).
    * Few tools with clear descriptions, a short and a long format, and limits by default come from [Writing effective tools for agents](https://www.anthropic.com/engineering/writing-tools-for-agents); annotations from the [MCP specification](https://modelcontextprotocol.io/specification/2025-11-25/server/tools).
    * Column descriptions add up to 26 points to the accuracy of text-to-SQL ([NOVAS 2026](https://www.vldb.org/2026/Workshops/VLDB-Workshops-2026/NOVAS/NOVAS26_16.pdf)), and hints about values took GPT-4 from 34.9% to 54.9% on [BIRD](https://arxiv.org/abs/2305.03111). Names weigh two to three times more than structure, and a wide table over several grains makes double counting ([Su et al.](https://arxiv.org/abs/2608.20356)).
    * OWASP LLM01 asks to segregate external content ([Top 10 for LLM](https://genai.owasp.org/llm-top-10/)), and Simon Willison names the three arms ([The lethal trifecta](https://simonwillison.net/2025/Jun/16/the-lethal-trifecta/)).
    * [dbt 1.12](https://docs.getdbt.com/docs/dbt-versions/core-upgrade/upgrading-to-v1.12) reads `osi/` and writes `target/osi_document.json`, and [Ossie](https://github.com/apache/ossie/blob/main/core-spec/spec.md) was accepted into the Apache Incubator on 10 July 2026.
* Related decisions: at acceptance this record replaces the reading of `staging` by `ai_reader` in [ADR-0007](0007-ai-access-via-mcp-read-only.md) and [ADR-0030](0030-ship-three-mcp-servers-in-v0-3-0-with-the-stdio-servers-in-local-mode.md). The levels of AI access in Metabase stay ([ADR-0017](0017-set-ai-access-in-metabase-by-levels.md)).
* Revisit this record when dbt-mcp offers a semantic layer in the open, or when a client reads the Open Semantic Interchange.
