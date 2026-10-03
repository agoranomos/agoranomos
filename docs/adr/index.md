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

| Status | Meaning |
|---|---|
| proposed | The decision is under discussion. |
| accepted | The decision is in force. |
| deprecated | The decision no longer applies, and no other record replaces it. |
| superseded | A newer record replaced the decision. The status names it, for example `superseded by ADR-0123`. |

## Accepted records are not edited

An accepted record is never rewritten to fit new circumstances. To change a decision, write a new ADR that supersedes the old one. The old record keeps its text, and only its status changes to point to the new one. That way the history of why things changed stays readable.

## All records

| ADR | Title | Status | Date |
|---|---|---|---|
| [0001](0001-record-architecture-decisions.md) | Record architecture decisions | accepted | 2026-10-02 |
