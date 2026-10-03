# Architecture decision records

An architecture decision record (ADR) captures one significant decision: the context, the options that were considered, the choice that was made and its consequences. Together the records explain why Agoranomos is built the way it is. [ADR-0001](0001-record-architecture-decisions.md) describes which decisions are recorded and how.

## Format

Records follow the [MADR 4.0](https://adr.github.io/madr/) format (Markdown Architectural Decision Records). Each record is one Markdown file named `NNNN-title.md`, with a short metadata block (status, date, decision-makers) and these sections:

- **Context and Problem Statement**: what needs deciding and why.
- **Decision Drivers**: the forces and concerns that matter.
- **Considered Options**: the alternatives that were weighed.
- **Decision Outcome**: the chosen option and its consequences, good and bad.
- **More Information**: evidence, links and when to revisit the decision.

## Statuses

The `status` in the metadata block of a record is one of these words, written in lowercase.

| Status | Meaning |
|---|---|
| proposed | The decision is under discussion. |
| accepted | The decision is in force. |
| rejected | The proposal was considered and declined. The record stays, so that the question is not reopened without its context. |
| deprecated | The decision no longer applies, and no other record replaces it. |
| superseded | A newer record replaced the decision. The status is `superseded`, and a line `Superseded by [ADR-0123](0123-title.md).` under the title names the new record. |

## Accepted records are not edited

An accepted record is never rewritten to fit new circumstances. To change a decision, write a new ADR that supersedes the old one. The old record keeps its text. Its status becomes `superseded`, and one line under its title points to the new record; nothing else changes. That way the history of why things changed stays readable.

A superseded record starts like this:

```markdown
---
status: superseded
date: 2026-10-02
decision-makers: Andrei Gromov
---

# Title of the old decision

Superseded by [ADR-0123](0123-title-of-the-new-decision.md).
```

## All records

Agoranomos is pre-alpha. A record states a decision, and the components it describes arrive release by release.

| ADR | Title | Status | Date |
|---|---|---|---|
| [0001](0001-record-architecture-decisions.md) | Record architecture decisions | accepted | 2026-10-02 |
| [0002](0002-orchestrate-with-airflow-3.md) | Orchestrate with Apache Airflow 3 | accepted | 2026-10-02 |
| [0003](0003-two-postgres-instances.md) | Run two PostgreSQL instances | accepted | 2026-10-02 |
| [0004](0004-ingest-with-dlt.md) | Ingest data with dlt | accepted | 2026-10-02 |
| [0005](0005-transform-with-dbt-core.md) | Transform data with dbt Core | accepted | 2026-10-02 |
| [0006](0006-metabase-as-bi.md) | Use Metabase as the BI layer | accepted | 2026-10-02 |
| [0007](0007-ai-access-via-mcp-read-only.md) | Give AI clients read-only access through MCP | accepted | 2026-10-02 |
| [0008](0008-apache-2-and-dco.md) | License under Apache-2.0 with DCO sign-off | accepted | 2026-10-02 |
| [0009](0009-documentation.md) | Write documentation with Zensical and Diátaxis | accepted | 2026-10-02 |
| [0010](0010-multi-arch-and-memory-budget.md) | Support amd64 and arm64 within a memory budget | accepted | 2026-10-02 |
| [0011](0011-local-and-server-modes.md) | Support local and server modes | accepted | 2026-10-02 |
| [0012](0012-python-and-tooling.md) | Use Python 3.13 and a pinned toolchain | accepted | 2026-10-02 |
