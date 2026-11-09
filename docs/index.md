---
template: home.html
hide:
  - navigation
  - toc
---

!!! warning "Status: pre-alpha"

    agoranomos is under active development and is not ready for use yet. These pages describe the intended design. Pages about features that are not released yet describe what is planned and, where a release is known, which one. Local mode and server mode run today, from the latest release: [Run agoranomos locally](how-to/run-locally.md).

<span class="ag-kicker">Open-source marketing BI out of the box</span>

## What it does

agoranomos is open-source marketing BI out of the box: ad platforms → PostgreSQL warehouse → Metabase dashboards → ask anything in Claude, ChatGPT or Gemini via MCP.

<div class="grid cards ag-four" markdown>

-   <span class="ag-n">01 · load</span>

    **Loads** data from ad platforms, web analytics, CRM systems and files into a PostgreSQL warehouse, on a schedule.

-   <span class="ag-n">02 · model</span>

    **Models** that data with dbt into one unified view of advertising performance across all platforms.

-   <span class="ag-n">03 · show</span>

    **Shows** it in Metabase, with dashboards, metrics and a glossary that come ready-made.

-   <span class="ag-n">04 · ask</span>

    **Lets you ask** questions about the data in AI clients, through MCP servers with least-privilege defaults. Claude works today, in its apps and in Claude Code, and ChatGPT, Gemini and Kimi are planned ([ADR-0033](adr/0033-support-claude-chatgpt-and-gemini-first.md)).

</div>

<figure class="ag-shot" markdown="span">
    ![The dashboard Advertising in Metabase on the demo data: its tabs and filters, the spend, the conversions and the other KPIs of the last 12 weeks, and the spend and the conversions over time](assets/screenshots/advertising-overview-light.webp#only-light){ width="1440" height="900" loading="lazy" }
    ![The dashboard Advertising in Metabase on the demo data, in the dark theme of Metabase: its tabs and filters, the spend, the conversions and the other KPIs of the last 12 weeks, and the spend and the conversions over time](assets/screenshots/advertising-overview-dark.webp#only-dark){ width="1440" height="900" loading="lazy" }
    <figcaption>Metabase · the dashboard Advertising · demo data</figcaption>
</figure>

To see how the pieces fit together, read the [architecture overview](explanation/architecture.md).

<span class="ag-kicker">Diátaxis</span>

## How this documentation is organized

The documentation follows [Diátaxis](https://diataxis.fr/): content is split by what you need at the moment, so every part has one job.

<div class="grid cards" markdown>

-   <span class="ag-n">learn by doing</span>

    **[Tutorials](tutorials/index.md)**

    The first tutorial, [Run agoranomos locally with demo data](tutorials/run-locally-with-demo-data.md), starts the stack with the demo and follows its dashboards back to the pipeline.

-   <span class="ag-n">recipes for tasks</span>

    **[How-to guides](how-to/index.md)**

    Recipes for specific tasks, such as adding a data source. Published together with the features they describe: the first guides, [Run agoranomos locally](how-to/run-locally.md), [Deploy on a server](how-to/deploy-on-a-server.md), [Back up and restore](how-to/back-up-and-restore.md), [See the demo](how-to/see-the-demo.md) and [Watch the pipeline](how-to/watch-the-pipeline.md), start the stack in local mode and in server mode, back it up and restore it, cover the demo, from the demo data through the model to a dashboard, and show the health of the pipeline.

-   <span class="ag-n">facts to look up</span>

    **[Reference](reference/index.md)**

    Exact facts to look up, such as configuration and the data model. Published together with the features they describe.

-   <span class="ag-n">background</span>

    **[Explanation](explanation/index.md)**

    How agoranomos works and why it is built that way, starting with the [architecture](explanation/architecture.md).

-   <span class="ag-n">one per choice</span>

    **[Decisions](adr/index.md)**

    The architecture decision records, one per significant choice.

</div>
