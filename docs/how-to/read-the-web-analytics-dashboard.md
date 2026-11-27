# Read the web analytics dashboard

The collection **agoranomos / Web analytics** in Metabase holds the dashboard **Web analytics**: the sessions, engaged sessions, key events and revenue of the properties of web analytics of the deployment, by source, medium, default channel group and campaign, for the period, the client and the property of its filters ([ADR-0054](../adr/0054-connect-ga4-with-a-service-account-of-the-deployment.md)). Every deployment has it, so it is empty until a source of web analytics loads: GA4 ([Connect GA4](connect-ga4.md)), or the fictional platform `demo_web` of the demo ([See the demo](see-the-demo.md)). The long-running test stand of the project loads real GA4 properties since 26 November 2026.

<figure class="ag-shot" markdown="span">
    ![The dashboard Web analytics in Metabase on the demo data: the filters Period, Time grouping, Client and Property, the sessions, the engaged sessions, the key events and the revenue of the last week compared with the week before, the sessions and key events over time, the top of the text about these numbers, and the sessions of each channel group over time](../assets/screenshots/web-analytics-light.webp#only-light){ width="1440" height="900" loading="lazy" }
    ![The dashboard Web analytics in Metabase on the demo data: the filters Period, Time grouping, Client and Property, the sessions, the engaged sessions, the key events and the revenue of the last week compared with the week before, the sessions and key events over time, the top of the text about these numbers, and the sessions of each channel group over time, in the dark theme](../assets/screenshots/web-analytics-dark.webp#only-dark){ width="1440" height="900" loading="lazy" }
    <figcaption>Metabase · agoranomos / Web analytics · Web analytics · demo data</figcaption>
</figure>

## Open it

Sign in to Metabase as [Run agoranomos locally](run-locally.md#sign-in) says and open the collection **agoranomos / Web analytics**. `metabase-init` makes the dashboard once Metabase shows the marts, after the first run of the transform, with or without the demo, and keeps it as the code says on every start, view-only for every group but Administrators ([ADR-0018](../adr/0018-keep-the-content-of-the-package-in-metabase-as-code.md)). With the demo, `demo-init` makes it at the first start, right after the transform. Until then, the log of the start says that the dashboard waits for the marts.

## The filters

- **Period**: the days of the sessions, in the time zone of each property. It starts as the 12 weeks before the current one, so it ends before today.
- **Time grouping**: how the charts over time and the four numbers at the top group the days, by day, week, month, quarter or year. It starts as the week, which starts on Monday. It groups and filters nothing.
- **Client** and **Property**. The property offers only the properties of the client that the filter names, as a linked filter of Metabase. Their values come from the sessions, `web_sessions_daily`: the clients and properties that have sessions on any day, in the period or not.

## What it shows

- The sessions, the engaged sessions, the key events and the revenue, each for its last period of the time grouping, compared with the one before.
- The sessions and their key events over time, and a text about how to read the numbers.
- The sessions of each default channel group over time.
- **Results by channel group**: the sessions, engaged sessions, engagement rate, key events and revenue of each default channel group.
- **Top sources and mediums**: the 20 pairs of a source and a medium with the most sessions, with the same numbers.
- **Campaigns**: every campaign that web analytics names, with its source and medium and the same numbers, the most sessions first. Besides the campaigns of the ads, it lists the names that web analytics gives the sessions without a campaign, such as `(organic)` or `(direct)`.

## How the numbers are computed

- **Revenue** is in the reporting currency, `AGORANOMOS_REPORTING_CURRENCY` of `.env`, at the rate of the day of each row ([The data model](../reference/data-model.md#the-marts)).
- **The engagement rate** is the sum of the engaged sessions divided by the sum of the sessions, over the rows that the filters select, never an average of the rates of the rows.
- **A total of sessions** adds up the rows of each day, source, medium, campaign and default channel group: GA4 can split the sessions of one campaign of a day between two groups, such as Cross-network and Display ([ADR-0057](../adr/0057-keep-the-default-channel-group-in-the-key-of-the-sessions.md)). GA4 estimates its sessions, so the total that its interface shows for a period can differ ([ADR-0054](../adr/0054-connect-ga4-with-a-service-account-of-the-deployment.md)).
- **Recent days can still change.** Each daily load takes the last 14 days of each property again, the window of restatements of GA4 and of `demo_web` ([ADR-0038](../adr/0038-load-by-copy-and-write-the-core-by-whole-days.md)), and the view marks a day that no daily load takes again with `is_final`. Web analytics takes no load of the current day.
- **The key events** are dated by their event and credited to one source, medium and campaign of the session, so they differ from the conversions of the ad platforms, which each platform credits to its own ads and dates by the click. The dashboard Advertising shows the key events beside those conversions ([Read the advertising dashboard](read-the-advertising-dashboard.md)).

[The data model](../reference/data-model.md) describes the mart that the dashboard reads.

## Keep a variant of your own

Duplicate the dashboard, with copies of its questions, into a collection of your own, and change the copy: the next start puts back what differs from the code in the collection **Web analytics**, and logs it.
