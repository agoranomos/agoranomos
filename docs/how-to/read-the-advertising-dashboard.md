# Read the advertising dashboard

The collection **agoranomos / Advertising** in Metabase holds the dashboard **Advertising**: the spend, the results and the KPIs of every ad platform of the deployment, on five tabs, for the period, the client, the platform, the account and the campaign of its filters ([ADR-0034](../adr/0034-define-a-basic-data-set-and-the-modules-of-an-extended-one.md)).

<figure class="ag-shot" markdown="span">
    ![The tab Overview of the dashboard Advertising in Metabase: the filters, the KPIs of the last week compared with the week before, and the spend and the conversions over time](../assets/screenshots/advertising-overview-light.webp#only-light){ width="1440" height="900" loading="lazy" }
    ![The tab Overview of the dashboard Advertising in Metabase: the filters, the KPIs of the last week compared with the week before, and the spend and the conversions over time, in the dark theme](../assets/screenshots/advertising-overview-dark.webp#only-dark){ width="1440" height="900" loading="lazy" }
    <figcaption>Metabase · agoranomos / Advertising · the tab Overview · demo data</figcaption>
</figure>

## Open it

Sign in to Metabase as [Run agoranomos locally](run-locally.md#sign-in) says and open the collection **agoranomos / Advertising**. `metabase-init` makes the dashboard once Metabase shows the marts, after the first run of the transform, with or without the demo, and keeps it as the code says on every start, view-only for every group but Administrators ([ADR-0018](../adr/0018-keep-the-content-of-the-package-in-metabase-as-code.md)). With the demo, `demo-init` makes it at the first start, right after the transform. Until then, the log of the start says that the dashboard waits for the marts.

## The filters

The filters at the top apply to every tab:

- **Period**: the days of the statistics, in the time zone of each account. It starts as the 12 weeks before the current one.
- **Time grouping**: how the charts over time and the KPIs group the days, by day, week, month, quarter or year. It starts as the week, which starts on Monday. It groups and filters nothing.
- **Client**, **Platform**, **Account**, **Campaign state** and **Campaign**. The account offers only the accounts of the client and the platform that the filters name, and the campaign only the campaigns of those, of the account and of the state, as linked filters of Metabase. Their values come from the statistics of the campaigns, `ad_stats_campaign_daily`: the clients, platforms, accounts and campaigns that have statistics on any day, in the period or not.
- **Campaign state**: `active`, `paused`, `removed` or `other`, the same words on every platform for the statuses that each writes in its own way, as of the last load of the campaign ([The data model](../reference/data-model.md#dimensions-and-versions)). It has no value at first, so the spend of a removed campaign counts in every total, and it applies to the cards of the statistics of the campaigns: Metabase shows it on the tabs Overview, Platforms and Campaigns.

The cards of web analytics take the period, the time grouping and the client only: a session has no account or campaign of an ad platform of its own. The cards of the ad groups, the ads and the conversions by action take every filter but the state of a campaign, which their marts do not have.

## What it shows

- **Overview.**
    - The KPIs: spend, conversions and cost per conversion of the primary conversion actions, ROAS, clicks, CTR and CPC, and the key events of web analytics beside them. Each shows its last period of the time grouping, compared with the one before.
    - The spend and the conversions over time, a text about how to read the numbers, the results of each platform, the spend of each client, the top ten campaigns by spend, and the last load of each account.
- **Platforms.** Every statistic and KPI of each platform, with the conversions of the primary conversion actions and those of the own metric of the platform side by side, and its spend and conversions over time.
- **Campaigns.** Every campaign with its state, its statistics and KPIs, by platform and account.
- **Ad groups and ads.** Every ad group and every ad with its state, its statistics and KPIs, with the conversions of the own metric of the platform. Performance Max campaigns of Google have no ad groups or ads, so the totals of this tab can be lower than those of the campaigns.
- **Conversions.** The conversions of each conversion action, primary or not, all of them and those that the own metric of its platform counts, with their value, its category and the attribution setting of its platform, every conversion by category over time, and the key events of web analytics over time.

A click on a campaign, in the table of the campaigns or of the top campaigns, opens the tab **Ad groups and ads** with the filter **Campaign** set to it.

<figure class="ag-shot" markdown="span">
    ![The tab Campaigns of the dashboard Advertising in Metabase: every campaign with its platform, account, statistics and KPIs](../assets/screenshots/advertising-campaigns-light.webp#only-light){ width="1440" height="900" loading="lazy" }
    ![The tab Campaigns of the dashboard Advertising in Metabase: every campaign with its platform, account, statistics and KPIs, in the dark theme](../assets/screenshots/advertising-campaigns-dark.webp#only-dark){ width="1440" height="900" loading="lazy" }
    <figcaption>Metabase · agoranomos / Advertising · the tab Campaigns · demo data</figcaption>
</figure>

## How the numbers are computed

- **Money** is in the reporting currency, `AGORANOMOS_REPORTING_CURRENCY` of `.env`.
- **A KPI is a ratio of sums**, over the rows that the filters select: CTR is the sum of the clicks divided by the sum of the impressions, never an average of the rates of the rows. Each card computes it from the same declaration in dbt as the metric of Metabase of the same name, such as `CTR (Campaigns)` in the collection **agoranomos / Metrics** ([Describe the marts](../reference/describe-the-marts.md#metrics)).
- **The conversions** of the cards of the campaigns are those of the primary conversion actions of the deployment, `conversions_primary`, which every platform has, Meta included; with them go the KPIs Cost per Primary Conversion and Primary Conversion Rate. The tab Platforms also shows the own metric of conversions of each platform, `conversions`, as its account shows it, and none on Meta, which counts each action apart, with the KPIs Cost per Conversion and Conversion Rate; the tab of the ad groups and ads has only that metric, as the conversions of the primary actions at those levels are a module. The value of the conversions, and so ROAS, is that of the own metric of each platform ([The data model](../reference/data-model.md#conversions)). Each platform credits its own ads and dates a conversion by the click, so a sum across platforms can count one conversion more than once, and the last days still rise for a while. **The key events** of web analytics date each by its event and credit it to one source, so the two counts differ.

[The data model](../reference/data-model.md) describes the marts that the dashboard reads.

## Keep a variant of your own

Duplicate the dashboard, with copies of its questions, into a collection of your own, and change the copy: the next start puts back what differs from the code in the collection **Advertising**, and logs it.
