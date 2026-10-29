---
status: accepted
date: 2026-10-29
decision-makers: Andrei Gromov
---

# Define a basic data set of the marts and the dashboards, and the modules of an extended one

## Context and Problem Statement

[ADR-0025](0025-model-advertising-statistics-once.md) modeled advertising statistics at the grain of a campaign and a day, and left ad groups and ads to the first real connector. [ADR-0032](0032-connect-google-ads-first-with-the-reference-rates-of-the-ecb.md) brings that connector, Google Ads, with the rates of the ECB, and Phase 4 adds one platform in each minor release. The marts, the dashboards and the MCP servers now need a stated scope: which data every deployment gets, which it may turn on, at which grains of time, and how the dashboards of Metabase present them.

The research behind this record, read on 29 October 2026, covered four things (see More Information):

* the models of established marketing data products, above all Fivetran's dbt_ad_reporting;
* the reporting APIs of Google Ads, Meta, TikTok, Microsoft Advertising, LinkedIn and GA4, and their limits;
* what the open models of marketing mix need (Google Meridian, Meta Robyn, PyMC-Marketing);
* the guidance on dashboards of Metabase and of the literature, and what Metabase OSS 0.64 offers.

Which data sets do the marts and the dashboards serve, at which grains, how does a deployment turn on more, and how does the model measure the overlap of reach?

## Decision Drivers

* A deployment gets, without settings, what every established product standardizes: the date, the platform, the account, the campaign, the cost, the impressions, the clicks and the conversions, at the levels of the platforms.
* Data that a deployment cannot get back later is loaded while it can be: the platforms keep daily data for 37 months.
* A number on a dashboard means one thing everywhere: a ratio is a ratio of sums, a non-additive metric is never summed, and a conversion that two platforms both claim is shown for what it is.
* A deployment turns on what it needs, and pays for it in quota, time and storage only then.
* Every part works in Metabase OSS, and keeps to the content of the package as code ([ADR-0016](0016-keep-the-metadata-of-the-marts-as-code.md), [ADR-0018](0018-keep-the-content-of-the-package-in-metabase-as-code.md)).

## Considered Options

For the grains of time:

* Facts by day, with coarser periods grouped in Metabase, and non-additive metrics as facts of their own periods
* Stored aggregates by week and by month beside the days

For the layout of the marts:

* One mart for each grain and breakdown
* One wide table with nullable columns of breakdowns
* Models or transforms in Metabase over the marts

For the extended data:

* Modules that a deployment turns on in `extensions/model/sources.toml`
* Switches in `.env`
* Everything for every deployment

For the conversions of the overview:

* The conversions of the platforms and the key events of GA4, side by side
* The conversions of the platforms only
* The key events of GA4 only

For the dashboards:

* One dashboard Advertising with tabs
* A dashboard for each level

For the overlap of reach:

* The reach of unions from the platforms, and overlaps by inclusion and exclusion
* An estimate only, by random duplication
* No overlap

## Decision Outcome

Chosen options: "Facts by day, with coarser periods grouped in Metabase, and non-additive metrics as facts of their own periods", "One mart for each grain and breakdown", "Modules that a deployment turns on in `extensions/model/sources.toml`", "The conversions of the platforms and the key events of GA4, side by side", "One dashboard Advertising with tabs" and "The reach of unions from the platforms, and overlaps by inclusion and exclusion", because a sum of days is exact for every additive metric, a mart of one grain cannot be summed wrong, a module costs only the deployments that turn it on, and only a platform knows the people behind its reach.

In practice this means:

* **The basic set.** Every deployment gets the marts of today and, from v0.4.0, `ad_stats_ad_group_daily`, `ad_stats_ad_daily` and the dimensions `ad_groups` and `ads`, each from a report of its own (ADR-0025).
    * The facts of ad groups and ads carry the conversions of the primary actions only; the conversions by action stay at the grain of campaigns, in `ad_conversions_campaign_daily`.
    * Performance Max stays at the grain of campaigns (ADR-0032).
    * The demo fills the whole basic set, so the stack tests build it.
* **History.** The first load of every real source of the basic set takes the last 36 months, and a later load the days since the last one and at least the reload window of the platform. Google Ads keeps data by day for 37 months, and Meta returns at most 37 months, so a shorter first load loses the rest for good. The rates of the ECB cover the same months and one more. The demo keeps its 18 months: its history is made up, and nothing of it is lost. The 36 months are what a national model of Meridian needs, and more than a geo model needs.
* **Grains of time.**
    * Additive facts (impressions, clicks, cost, conversions, sessions, key events, revenue) are stored by day, in the time zone of the account or the property (ADR-0025). Week, month, quarter and year are groupings in Metabase, through the filter of time grouping of the dashboards: a sum of days is exact.
    * The week starts on Monday (ISO 8601), which `metabase-init` sets as the first day of the week of Metabase.
    * Reach, frequency and unique users are not additive across days, levels or rows. They are stored as facts of their own periods: the week, the calendar month, and the lifetime of a campaign where a platform gives it. No row of them is summed. A quarter is not stored, as Google Ads and LinkedIn give reach for 92 days at most.
    * The panel of marketing mix is by week, and budgets compare daily facts with a monthly plan.
    * Hours are not stored; a later module may add them for the analysis of the time of day.
* **Conversions on the overview.** The overview shows the conversions as the platforms report them, and, where the deployment has GA4, the key events of GA4 beside them, as a count that no two platforms both claim. A platform credits only its own touchpoints, and dates a conversion by the click, while GA4 dates it by the event, so the two differ, above all in recent days. A text card on the overview says so.
* **KPIs as metrics of Metabase.** CTR, CPC, CPM, cost per conversion, conversion rate and ROAS are metrics of Metabase, created from code, each a ratio of sums over a mart, so that every card, every question of a person and every MCP client computes them alike.
    * Their definitions also go into the glossary of Metabase, from the descriptions of dbt.
    * The metadata of the marts gives the keys between them the semantic type of a foreign key, so that the filters of the dashboards can be linked.
    * The API of Metabase is not versioned, so a stack test checks that the metrics exist and compute after an upgrade.
* **Dashboards.**
    * **Advertising**, one dashboard with the tabs Overview, Platforms, Campaigns, Ad groups and ads, and Conversions, and a tab for each module that adds one.
        * The overview leads with KPIs compared with the previous period, then the trend, the platforms, the clients and the top campaigns.
        * Filters at the level of the dashboard: the period, the time grouping, the client, the platform, the account and the campaign.
        * A click on a campaign sets the filter of the tab of ad groups and ads.
        * Text cards say when the data was loaded (from `ops_loads`), that recent days still change, and how the conversions are attributed.
    * **Web analytics** and the Ops collection stay as they are.
    * No dashboard depends on a feature that Metabase sells only in its paid plans, such as verified content, official collections or caching by dashboard.
* **Modules.** The extended set is a list of modules that a deployment turns on in `extensions/model/sources.toml`, in this order of work:
    1. Budgets and pacing: the budgets of the campaigns and a monthly plan of the deployment, against the cost of the month to date.
    2. Reach and frequency, with their overlaps (below).
    3. Video and engagement, each metric under the name and definition of its platform, as the platforms count a view differently.
    4. Search: keywords and search terms.
    5. Geography and devices: campaigns by country and by device, by day, where a platform reports them.
    6. The panel of marketing mix (week × country × channel) and the conversions of the business from a CRM.
    7. Experiments: the results of lift studies.

    Later candidates are demographics, the landing pages, devices and countries of GA4, the asset groups of Performance Max, and creatives. A module adds its own reports to the loads of its platforms, its own marts and its own tabs. The models of a module are built in every deployment and stay empty until it is turned on, as `relation_or_empty` keeps those of a paused source (ADR-0032), so the project and its tests keep one shape. `metabase-init` creates the cards and tabs of a module only when it is on.
* **The overlap of reach.** The overlap of A and B is reach(A) + reach(B) − reach(A ∪ B). No sum of reaches gives the reach of a union: only the platform can, so the module asks each platform for the unions that it can answer, and computes the overlaps in dbt:
    1. **A parent and its children** (a campaign and its ad groups, an ad group and its ads, a month and its weeks, a week and its days): the sum of the reach of the children minus the reach of the parent, from the facts of periods, where a platform gives each level.
    2. **Successive periods of one object** (a day and the next, a week and the next, a month and the next), and the curve of cumulative reach from the start of a campaign, from which the increase of reach in each period follows. A union of periods that are not next to each other cannot be asked for in one range, and is not computed.
    3. **Pairs of objects in one period** (campaigns, ad groups or ads), from a query of the reach of each pair. As pairs grow with the square of the objects, the module takes the top objects by cost within an account, as many as `sources.toml` says, or a list that it names. This comes in a second version of the module, once a real account has shown that a platform deduplicates the reach of a filtered set.
    4. **Across platforms** nothing is exact, as no platform shares people. An estimate by random duplication may come later, in a column marked as an estimate.

    Reach is estimated and sampled by the platforms, and the subtraction makes its error larger. An overlap is therefore kept between 0 and the smaller reach, a row below a threshold of reach is marked, and every reach of a union is checked to lie between the larger reach and the sum of both: a platform that always returns the sum does not deduplicate, and that kind of overlap is turned off for it.

The other options were not chosen for these reasons:

* **Stored aggregates by week and month** would repeat what a sum of days gives exactly, and would still be wrong for reach, which no stored week or month of days gives.
* **One wide table** would mix grains in one table, and a sum over it would count a row of a campaign and a row of its country both.
* **Models or transforms in Metabase** would duplicate dbt, where the model and its tests live; Metabase also phases out its models in favor of transforms.
* **Switches in `.env`** would put a setting of the model among the secrets, while the clients and the conversion actions of the model already live in `extensions/model/` ([ADR-0024](0024-one-deployment-is-one-business.md)).
* **Everything for every deployment** would spend the quota of every account on reports that most deployments never read: each report of Google Ads costs one operation a day for each account.
* **The conversions of the platforms only** would hide that a sum across platforms may count one conversion more than once, and **the key events of GA4 only** would hide what each platform optimizes for.
* **A dashboard for each level** would repeat its filters in each dashboard, and lose the click from a campaign to its ad groups.
* **An estimate only** would show a guess where a platform can give the answer, and **no overlap** would drop what the owner asked for.

### Consequences

* Good, because the basic set matches what established products standardize, so a person who knows them finds the same levels and fields.
* Good, because 36 months keep what the platforms would drop, and are enough for a model of marketing mix.
* Good, because one definition of each KPI serves the dashboards, the questions of people and the AI clients.
* Good, because a deployment pays in quota and storage only for the modules that it turns on.
* Bad, because the first load of an account takes longer: about 230 operations of Google Ads for the six reports of facts of the basic set, the statistics and the conversions by action of campaigns, ad groups and ads, a month in each request, against about 40 for 18 months of campaigns.
* Bad, because the marts of a module exist, empty, in every deployment, and a person who browses the warehouse sees them.
* Bad, because Google Ads gives reach only for campaigns of Display, Video, Demand Gen and App, for 92 days at most, and at the level of campaigns only, so its overlaps are those of periods within one campaign; Microsoft Advertising gives no reach at all.
* Bad, because the overlaps of pairs depend on behavior that the documentation of Meta and LinkedIn does not state, and wait for a test on a real account.
* Bad, because the metrics of Metabase are created through an API that Metabase does not version, which an upgrade of Metabase may change.

## More Information

* **Implementation status, as of October 2026:** the ad groups and the ads of the basic set are in place for v0.4.0: the marts `ad_stats_ad_group_daily`, `ad_stats_ad_daily`, `ad_groups` and `ads`, with the conversions of the primary actions from the reports by action of each level, which the demo fills on its three ad platforms and the connector of Google Ads loads (ADR-0032); the history of 36 months of Google Ads and of 37 of the rates of the ECB. The KPIs are in place as metrics of Metabase on `ad_stats_campaign_daily`, `ad_stats_ad_group_daily` and `ad_stats_ad_daily`, declared in dbt under `config: meta: metabase: metrics:` and named `<KPI> (<level>)` on every level, such as `CTR (Campaigns)`, with one term for each KPI in the glossary, and `metabase-init` starts the weeks of Metabase on Monday while Metabase keeps its default, Sunday; Metabase 0.64.1 keeps no value of Sunday, so a choice of Sunday becomes Monday again at the next start. The metadata of the marts makes `client_id` and `platform` foreign keys of `clients` and `ad_platforms` in Metabase; the IDs of accounts, campaigns, ad groups and ads have none, as each is a key only with its platform and its account, and Metabase joins a foreign key on one column. Not built yet: the dashboard Advertising and the modules.
* The facts behind this record, read on 29 October 2026:
    * Fivetran's [dbt_ad_reporting](https://github.com/fivetran/dbt_ad_reporting) v2.8.1 (15 October 2026) has reports of accounts, campaigns, ad groups and ads by day for 11 platforms, of keywords, search queries and URLs for some, and of countries and regions by month; it added conversions in v1.10.0 and has no reach ([DECISIONLOG](https://github.com/fivetran/dbt_ad_reporting/blob/main/DECISIONLOG.md)). Funnel standardizes cost, clicks and impressions across sources ([standard metric rules](https://help.funnel.io/en/articles/2308315-funnel-s-standard-metric-rules)).
    * Google Ads keeps data by date, week and hour for 37 months ([Segmentation](https://developers.google.com/google-ads/api/docs/reporting/segmentation)); `metrics.unique_users` "cannot be aggregated", needs a range of 92 days or less and is selectable with `campaign` only ([Metrics v26](https://developers.google.com/google-ads/api/fields/v26/metrics)); reach covers Display, Demand Gen, Video and App ([Measuring reach and frequency](https://support.google.com/google-ads/answer/2472714)).
    * Meta returns at most 37 months, deduplicates unique metrics across child objects, and returns reach for breakdowns older than 13 months only through asynchronous jobs ([Ad Account Insights](https://developers.facebook.com/docs/marketing-api/reference/ad-account/insights/), [Limits and Best Practices](https://developers.facebook.com/docs/marketing-api/insights/best-practices)); its reach is estimated from sampled data ([About Reach Metrics](https://www.facebook.com/business/help/710746785663278)).
    * TikTok takes a range of 365 days without a day dimension, and its filter of campaign IDs is not valid at the level of the advertiser ([Supported filters](https://business-api.tiktok.com/portal/docs/basic-reports-supported-filters/v1.3)). LinkedIn gives `approximateMemberReach` for 92 days at most ([Ads reporting schema](https://learn.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting-schema)). The reporting API of Microsoft Advertising v13 has no column of reach ([CampaignPerformanceReportColumn](https://learn.microsoft.com/en-us/advertising/reporting-service/campaignperformancereportcolumn?view=bingads-13)).
    * Google Meridian 2.1.0 recommends weekly data by geo, and at least two years for a geo model and three for a national one ([Collect data](https://developers.google.com/meridian/docs/user-guide/collect-data)); Robyn asks for at least two years of weekly data ([Analyst's guide](https://facebookexperimental.github.io/Robyn/docs/analysts-guide-to-MMM/)).
    * GA4 offers data-driven and last-click attribution only ([Attribution models](https://support.google.com/analytics/answer/10596866)), and Google explains why Google Ads and GA4 count conversions differently ([answer 2375435](https://support.google.com/google-ads/answer/2375435)). Metabase advises counting conversions once, from the business's own data ([Marketing analytics](https://www.metabase.com/analytics/marketing-analytics)).
    * Metabase OSS 0.64 (LTS) has tabs, linked filters, click behavior, metrics, segments and a glossary; verified content, official collections and caching by dashboard are paid ([Metrics](https://www.metabase.com/docs/v0.64/data-modeling/semantic-layer/metrics), [Filters](https://www.metabase.com/docs/latest/dashboards/filters), [Paid features](https://www.metabase.com/docs/latest/paid-features)); "Transforms are the successor to models" ([Models](https://www.metabase.com/docs/v0.64/data-modeling/models/models)). Its guidance puts the important cards first and prefers time series and tables ([BI dashboard best practices](https://www.metabase.com/learn/metabase-basics/querying-and-dashboards/dashboards/bi-dashboard-best-practices)), as do Stephen Few ([Common Pitfalls in Dashboard Design](https://www.perceptualedge.com/articles/Whitepapers/Common_Pitfalls.pdf)) and Shneiderman ("Overview first, zoom and filter, then details-on-demand").
    * Google defines CTR as clicks divided by impressions and CPC as cost divided by clicks ([CTR](https://support.google.com/google-ads/answer/2615875), [CPC](https://support.google.com/google-ads/answer/14074)), and Looker Studio shows that a sum of row ratios is wrong ([Re-aggregate data](https://docs.cloud.google.com/looker/docs/studio/use-blending-to-reaggregate-data)). Kimball stores the additive parts and computes ratios afterwards ([Additive facts](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/additive-semi-additive-non-additive-fact/)).
    * The IAB finds no widely adopted cross-channel deduplication of reach ([Anatomy of Reach in Video Advertising](https://www.iab.com/wp-content/uploads/2025/10/IAB_ANATOMY_REACH_D6.pdf), October 2025); random duplication, the method of Sainsbury, is the usual estimate where the union is unknown ([Anesbury et al., 2020](https://doi.org/10.1080/0267257X.2020.1794933)).
* Related decisions: the model and its grains ([ADR-0025](0025-model-advertising-statistics-once.md)), the first connector ([ADR-0032](0032-connect-google-ads-first-with-the-reference-rates-of-the-ecb.md)), the metadata and the content of the package as code (ADR-0016, ADR-0018), and the levels of AI access in Metabase ([ADR-0017](0017-set-ai-access-in-metabase-by-levels.md)).
* Revisit this record when a platform adds the reach of unions to its API, when Metabase replaces its metrics or versions its API, or when a deployment needs a module that the list lacks.
