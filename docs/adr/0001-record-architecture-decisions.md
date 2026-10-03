---
status: accepted
date: 2026-10-02
decision-makers: Andrei Gromov
---

# Record architecture decisions

## Context and Problem Statement

Agoranomos combines many components, and some choices about them are hard to reverse or shape what users and contributors can rely on: which components make up the stack, the contracts of the data models, what is allowed by default (above all for AI access), the license, and how releases are made. The reasons behind such choices are easily lost. Newcomers cannot tell a deliberate choice from an accident, settled questions get reopened without their original context, and changes of direction happen silently.

How do we record significant decisions so that they stay easy to find, open to review and honest about their trade-offs?

## Decision Drivers

* Users and contributors should be able to find out why things are the way they are.
* Decisions should be reviewed in the open, the way code is.
* The record should live with the code and change with it, so that it cannot drift away from the project it describes.
* Writing a record should cost less than losing the reasoning behind a decision.
* The format should be plain Markdown and need no special tooling.
* A later reader should be able to see what was decided at the time and what replaced it.

## Considered Options

* Architecture decision records in the repository, in the MADR 4.0 format
* Free-form design documents or wiki pages
* Issue and pull request discussions only
* No formal record

## Decision Outcome

Chosen option: "Architecture decision records in the repository, in the MADR 4.0 format", because it keeps the reasoning next to the code, where it is versioned, reviewed through pull requests and published with the documentation, and it needs nothing beyond Markdown. MADR is a small, widely used format that makes the considered options and the consequences explicit.

The rules are:

* **What is recorded.** A decision gets an ADR when it is hard to reverse or affects users and contributors. Typical subjects are the components of the stack, the contracts of the data models, security defaults, the license and releases.
* **Where and how.** Each record is a Markdown file `docs/adr/NNNN-title.md` with a sequential four-digit number. It follows the [MADR 4.0](https://adr.github.io/madr/) template, with a metadata block for status, date and decision-makers.
* **Lifecycle.** A record has one of the statuses proposed, accepted, rejected, deprecated or superseded. An accepted record is not edited. To change a decision, a new ADR supersedes the old one. The old record keeps its text, its status becomes `superseded`, and one line under its title, `Superseded by [ADR-NNNN](NNNN-title.md)`, points to the new record.

### Consequences

* Good, because the reasoning behind significant choices is public, searchable and versioned together with the code.
* Good, because a change of direction is explicit: the superseded record points to the one that replaced it.
* Good, because proposing or questioning a decision follows the same pull request review as any other change.
* Bad, because writing records takes effort, so they are limited to decisions that meet the criteria above.
* Bad, because superseded records stay in place, so readers have to follow the "Superseded by" line to the current decision.

## More Information

* The format is [MADR 4.0.0](https://github.com/adr/madr/blob/4.0.0/template/adr-template.md). The idea of keeping decisions as short documents next to the code comes from Michael Nygard's article [Documenting Architecture Decisions](https://www.cognitect.com/blog/2011/11/15/documenting-architecture-decisions).
* All records are listed in the [index](index.md).
* This process can itself be changed by a new ADR that supersedes this one, for example if records turn out to be too heavy for the project.
