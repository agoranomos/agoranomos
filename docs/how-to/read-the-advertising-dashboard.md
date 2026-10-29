# Read the advertising dashboard

The collection **Agoranomos / Advertising** in Metabase holds the dashboard **Advertising**: the spend, the results and the KPIs of every ad platform of the deployment, on five tabs, for the period, the client, the platform, the account and the campaign of its filters ([ADR-0034](../adr/0034-define-a-basic-data-set-and-the-modules-of-an-extended-one.md)).

## Open it

Sign in to Metabase as [Run Agoranomos locally](run-locally.md#sign-in) says and open the collection **Agoranomos / Advertising**. `metabase-init` makes the dashboard once Metabase shows the marts, after the first run of the transform, with or without the demo, and keeps it as the code says on every start, view-only for every group but Administrators ([ADR-0018](../adr/0018-keep-the-content-of-the-package-in-metabase-as-code.md)). With the demo, `demo-init` makes it at the first start, right after the transform. Until then, the log of the start says that the dashboard waits for the marts.

## The filters

The filters at the top apply to every tab:

- **Period**: the days of the statistics, in the time zone of each account. It starts as the 12 weeks before the current one.
- **Time grouping**: how the charts over time and the KPIs group the days, by day, week, month, quarter or year. It starts as the week, which starts on Monday. It groups and filters nothing.
- **Client**, **Platform**, **Account** and **Campaign**. The account offers only the accounts of the client and the platform that the filters name, and the campaign only the campaigns of those and of the account, as linked filters of Metabase.

The cards of web analytics take the period, the time grouping and the client only: a session has no account or campaign of an ad platform of its own.

## What it shows

- **Overview.**
    - The KPIs: spend, conversions, cost per conversion, ROAS, clicks, CTR and CPC, and the key events of web analytics beside them. Each shows its last period of the time grouping, compared with the one before.
    - The spend and the conversions over time, a text about how to read the numbers, the results of each platform, the spend of each client, the top ten campaigns by spend, and the last load of each account.
- **Platforms.** Every statistic and KPI of each platform, and its spend and conversions over time.
- **Campaigns.** Every campaign with its statistics and KPIs, by platform and account.
- **Ad groups and ads.** Every ad group and every ad with its statistics and KPIs. Performance Max campaigns of Google have no ad groups or ads, so the totals of this tab can be lower than those of the campaigns.
- **Conversions.** The conversions and the conversion value of each conversion action, primary or not, with its category and the attribution setting of its platform, the conversions by category over time, and the key events of web analytics over time.

A click on a campaign, in the table of the campaigns or of the top campaigns, opens the tab **Ad groups and ads** with the filter **Campaign** set to it.

## How the numbers are computed

- **Money** is in the reporting currency, `AGORANOMOS_REPORTING_CURRENCY` of `.env`.
- **A KPI is a ratio of sums**, over the rows that the filters select: CTR is the sum of the clicks divided by the sum of the impressions, never an average of the rates of the rows. Each card computes it from the same declaration in dbt as the metric of Metabase of the same name, such as `CTR (Campaigns)` in the collection **Agoranomos / Metrics** ([Describe the marts](../reference/describe-the-marts.md#metrics)).
- **The conversions** are those of the primary conversion actions, as each platform counts them. Each platform credits its own ads and dates a conversion by the click, so a sum across platforms can count one conversion more than once, and the last days still rise for a while. **The key events** of web analytics date each by its event and credit it to one source, so the two counts differ.

[The data model](../reference/data-model.md) describes the marts that the dashboard reads.

## Keep a variant of your own

Duplicate the dashboard, with copies of its questions, into a collection of your own, and change the copy: the next start puts back what differs from the code in the collection **Advertising**, and logs it.
