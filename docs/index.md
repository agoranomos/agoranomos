# Agoranomos

Agoranomos is open-source marketing BI out of the box: ad platforms → PostgreSQL warehouse → Metabase dashboards → ask anything in Claude, ChatGPT or Gemini via MCP.

!!! warning "Status: pre-alpha"

    Agoranomos is under active development and is not ready for use yet. These pages describe the intended design. Pages about features that are not released yet describe what is planned and, where a release is known, which one. Local mode and server mode run today, from the latest release: [Run Agoranomos locally](how-to/run-locally.md).

## What it does

- **Loads** data from ad platforms, web analytics, CRM systems and files into a PostgreSQL warehouse, on a schedule.
- **Models** that data with dbt into one unified view of advertising performance across all platforms.
- **Shows** it in Metabase, with dashboards, metrics and a glossary that come ready-made.
- **Lets you ask** questions about the data in AI clients, through MCP servers with least-privilege defaults. Claude works today, in its apps and in Claude Code, and ChatGPT, Gemini and Kimi are planned ([ADR-0033](adr/0033-support-claude-chatgpt-and-gemini-first.md)).

To see how the pieces fit together, read the [architecture overview](explanation/architecture.md).

## How this documentation is organized

The documentation follows [Diátaxis](https://diataxis.fr/): content is split by what you need at the moment, so every part has one job.

- [Tutorials](tutorials/index.md): learn by doing. The first tutorial, [Run Agoranomos locally with demo data](tutorials/run-locally-with-demo-data.md), starts the stack with the demo and follows its dashboards back to the pipeline.
- [How-to guides](how-to/index.md): recipes for specific tasks, such as adding a data source. Published together with the features they describe: the first guides, [Run Agoranomos locally](how-to/run-locally.md), [Deploy on a server](how-to/deploy-on-a-server.md), [Back up and restore](how-to/back-up-and-restore.md), [See the demo](how-to/see-the-demo.md) and [Watch the pipeline](how-to/watch-the-pipeline.md), start the stack in local mode and in server mode, back it up and restore it, cover the demo, from the demo data through the model to a dashboard, and show the health of the pipeline.
- [Reference](reference/index.md): exact facts to look up, such as configuration and the data model. Published together with the features they describe.
- [Explanation](explanation/index.md): how Agoranomos works and why it is built that way, starting with the [architecture](explanation/architecture.md).
- [Decisions](adr/index.md): the architecture decision records, one per significant choice.
