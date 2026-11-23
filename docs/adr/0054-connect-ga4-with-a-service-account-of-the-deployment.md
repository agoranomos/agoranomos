---
status: accepted
date: 2026-11-20
decision-makers: Andrei Gromov
---

# Connect GA4 with a service account of the deployment

## Context and Problem Statement

[ADR-0032](0032-connect-google-ads-first-with-the-reference-rates-of-the-ecb.md) put GA4 after Google Ads, because it brings sessions and no spend, and noted that it has "the easiest sign-in of all, a service account with the Viewer role on a property". It left each later source to a record of its own. Later records already decided the parts of GA4 that the model needs: [ADR-0034](0034-define-a-basic-data-set-and-the-modules-of-an-extended-one.md) shows the key events of GA4 beside the conversions of the platforms, where the deployment has GA4; [ADR-0036](0036-model-the-core-as-narrow-facts-and-versioned-dimensions.md) gives the facts of GA4 a table of their own on the text keys of the source; [ADR-0038](0038-load-by-copy-and-write-the-core-by-whole-days.md) sets the window of restatements of GA4 to 14 days and the history to portions by quarters; [ADR-0051](0051-take-the-current-day-as-a-load-of-the-kind-intraday.md) takes the current day of the ad platforms.

What exists today, since v0.5, is the model and the demo. Migration `0005` creates `core.web_sessions_daily`, with `session_source`, `session_medium`, `session_campaign_name` and `session_campaign_id` as `text COLLATE "C"` in its key, monthly partitions and the freeze; `dbt/models/core/core_web_sessions_daily.sql` writes it by whole days from the fictional platform `demo_web`, and `marts.web_sessions_daily` is a view over it that names the ad campaign whose ID a session carries, when exactly one campaign of the core has it ([The data model](../reference/data-model.md)). `demo_web` mimics the GA4 Data API v1beta (`airflow/dags/agoranomos_pipelines/demo/web.py`), with the tables `properties` and `report_sessions_daily` in `raw_demo_web`, and the shape macros `dbt/macros/shapes/ga4_v1beta.sql` read them: `ga4_properties` takes `name` (`properties/<id>`), `display_name`, `time_zone` and `currency_code` of the Admin API, and `ga4_sessions_daily` takes `date` (YYYYMMDD), the `property` that the connector adds, five dimensions of the session and the metrics `sessions`, `engaged_sessions`, `key_events` and `total_revenue`, every value as text, and drops a row whose measures are all zero. The seed of the platforms gives `demo_web` the kind `web_analytics` and the window of 14 days. G1 and G1a of [ADR-0035](0035-keep-four-layers-and-erase-the-raw-layer-after-acceptance.md) already check a property: G1 the form of its sessions and of the list of properties, G1a a load of web analytics with no row where the core holds sessions. `core.platform_totals_daily`, against which G0a checks an ad platform, holds impressions, clicks and spend, each `NOT NULL`. The dashboard Advertising of every deployment has a card of the key events of `web_sessions_daily`, "none where the deployment has no source of web analytics" (`metabase/advertising.py`), and the dashboard of web analytics is demo content, which `demo-init` creates with the profile `demo` only. No connector of GA4 exists, and no task of the stand reads a GA4 property: S6 ([#220](https://github.com/agoranomos/agoranomos/issues/220)) connects Meta and TikTok.

How does a deployment connect GA4: under which code and with which key, which properties and requests, which windows and checks, within which quota and on which version of the API; does it get a dashboard of web analytics; and where are the points that no page settles measured?

## Decision Drivers

* A deployment connects the source with steps that one person can follow, with no review by Google (ADR-0032).
* The source serves one business: only the properties that `extensions/model/clients.toml` names ([ADR-0024](0024-one-deployment-is-one-business.md)).
* The connector fills the model that the demo fills, through the same shape macros, so that the dashboards, the tests and the skills of the demo hold for real data ([ADR-0025](0025-model-advertising-statistics-once.md)).
* The loads read only: the least role and scope that read the reports ([ADR-0007](0007-ai-access-via-mcp-read-only.md)).
* As little secret material in `.env` as the sign-in needs, a leak that opens as little as it can, and no secret in a log or in the output of `./agoranomos` (AGENTS.md).
* The contract of a source: a window in the time zone of the property, safe repeats, explicit retries within the limits of the API, loud failures, and a property that fails leaves the others to load ([ADR-0004](0004-ingest-with-dlt.md), ADR-0035).
* A number of the core that cannot match the interface of the platform is explained and marked (ADR-0036).
* No new package next to Airflow, and every package pinned (AGENTS.md).
* v0.7 already plans more sessions than #177 gave it, so what a task adds is weighed against the release.

## Considered Options

For the platform code (open question 1 of #226):

* `ga4`, with the schemas `raw_ga4` and `raw_ga4_staging`
* `google_analytics`

For the key of the service account (open question 1):

* A variable of its own, `GA4_SERVICE_ACCOUNT_KEY`, which may hold the same key as `GOOGLE_ADS_SERVICE_ACCOUNT_KEY` or another
* The key of Google Ads, `GOOGLE_ADS_SERVICE_ACCOUNT_KEY`
* One variable for every service of Google, such as `GOOGLE_SERVICE_ACCOUNT_KEY`

For the dashboard of web analytics (open question 2):

* A dashboard Web analytics in the content of the package for every deployment, kept by `metabase-init` as Advertising is, in a task of its own after C6c
* The same dashboard, built in C6c
* The same dashboard, only in a deployment whose marts hold a property of a real source
* Only the card of key events of the dashboard Advertising, as today

For G0a (open question 3):

* No G0a for GA4 in v0.7: the four flags of each answer in the journal of the load, and a check of the rows of each answer against its `rowCount`
* A request of the totals of each property by day, with `date` alone, into a table of its own made by a migration, and G0a against it
* The same totals in `core.platform_totals_daily`, by a migration that adds the sessions and lets impressions, clicks and spend be null

For a live property (open question 4):

* A GA4 property of the maintainer or of the agency in S6 (#220), read with the role Viewer for a month of daily loads and the history
* A task of the stand of its own for GA4, after S6
* The points left unverified until a deployment reports them

## Decision Outcome

Chosen options: "`ga4`", "A variable of its own, `GA4_SERVICE_ACCOUNT_KEY`", "A dashboard Web analytics in the content of the package for every deployment, in a task of its own after C6c", "No G0a for GA4 in v0.7" and "A GA4 property of the maintainer or of the agency in S6", because a service account with the role Viewer reads a property with no review by Google, the source keeps the shape that `demo_web` already has and writes a fact that exists, a key of its own lets a deployment keep a leak of one key from opening both sources, and sessions are estimates that a sum of rows cannot check, while each answer says itself what it may have lost.

These choices answer the four open questions of #226, and are the maintainer's to accept or change in the review of this record. The rest follows the brief of C6a and the records before this one.

In practice this means:

* **The platform code and the schemas.** The platform is `ga4`, after the shape macros `ga4_v1beta` and the name that Google gives the product, with the schemas `raw_ga4` and `raw_ga4_staging`, owned by `loader` and read by `transformer` as those of Google Ads and Meta are. Its row of the seed of the platforms has the kind `web_analytics`, the window of 14 days and no `mimics`. The code is in every key of the core and of `extensions/model/clients.toml`, so it never changes once data exists.
* **The sign-in.** The deployment uses a service account of its Google Cloud project, which may be the project of Google Ads, enables the Google Analytics Admin API and the Google Analytics Data API in that project, and adds the email of the service account as a user of each property with the role Viewer, the least role that sees the data of a property, and with no data restriction, as "No Revenue Metrics" would hide `totalRevenue`. The load asks for the scope `https://www.googleapis.com/auth/analytics.readonly` alone, makes its access token with `google-auth` at the start of each run, as `google_ads.py` does, and sends it as `Authorization: Bearer`, never in a URL.
    * *Open to the maintainer's acceptance:* the key goes into `.env` as `GA4_SERVICE_ACCOUNT_KEY`, in the forms that `GOOGLE_ADS_SERVICE_ACCOUNT_KEY` takes, JSON or base64 of the JSON. A deployment may put the same key in both variables, or make a second service account for GA4, so that a leak of one key reads one source.
    * The load checks the form of the key, that it is the JSON of a key of a service account, and fails with a line that names the variable and never the key, as `google_ads.py` does. `./agoranomos` checks neither key today: ADR-0032 plans that it write the key of Google Ads into `.env` from its file, which would serve this key too.
    * In an organization of Google Cloud created on or after 3 May 2024, the policy `constraints/iam.managed.disableServiceAccountKeyCreation` forbids keys of service accounts by default, and an administrator of the policies of the organization has to allow them for the project, as for Google Ads.
* **The properties.** The source loads the properties of the platform `ga4` that `extensions/model/clients.toml` names, by their numeric ID without `properties/`, and no other (ADR-0024). One service account can be a user of the properties of several Analytics accounts, and nothing read limits how many; the file keeps the deployment to the properties of its business. The default client `unassigned` never applies to a property, as to an account of Google Ads.
* **The client of the API.** The source calls the REST endpoints of the two APIs with the HTTP client of dlt (`dlt.sources.helpers.requests`), with explicit retries, and not the libraries `google-analytics-data` and `google-analytics-admin`, which require `google-api-core[grpc]`, `grpcio`, `proto-plus` and `protobuf`.
* **The requests.** For each property, one after another, and one request at a time, as a standard property allows ten at the same time:
    * `properties.get` of the Admin API, `GET https://analyticsadmin.googleapis.com/v1beta/properties/<id>`, for `name`, `displayName`, `timeZone` and `currencyCode`, into `raw_ga4.properties`;
    * one `runReport` of the Data API, `POST https://analyticsdata.googleapis.com/v1beta/properties/<id>:runReport`, for its window in one date range, with the dimensions `date`, `sessionSource`, `sessionMedium`, `sessionCampaignName`, `sessionCampaignId` and `sessionDefaultChannelGroup`, six of the nine that a request may have, the metrics `sessions`, `engagedSessions`, `keyEvents` and `totalRevenue`, `limit` 250,000, the most rows that a request returns, with `offset` for each next page, `orderBys` on the six dimensions so that the pages of one report follow one order, `keepEmptyRows` false and `returnPropertyQuota` true, into `raw_ga4.report_sessions_daily`, with `property` added to each row as `properties/<id>`.

    The names and the fields are those that the macros of `ga4_v1beta` read from `raw_demo_web`, and every value stays text, as a metric value of the API is a string. The load reads each page as it comes into a file on disk, as `google_ads.py` does, and compares the rows that it read with `rowCount` of the answer: a property whose pages give fewer or more rows fails. `sessionCampaignId` already includes the campaigns of Google Ads, which auto-tagging names, and the manual campaigns of `utm_id`, which the links of Meta and TikTok carry where they name their campaign, so the join of the mart works for both. `sessionGoogleAdsCampaignId` and `sessionManualCampaignId` are not requested now: each would be a new column of the key of the fact, and so a migration of the core (ADR-0044), for what `sessionCampaignId` already gives.
* **The flags of each answer.** Four fields of `ResponseMetaData` say that the numbers of an answer may not be the whole truth: `subjectToThresholding`, `dataLossFromOtherRow`, `samplingMetadatas` and `dataTruncationReasons`. The load writes, for each property and load, four `boolean` columns of the journal `raw_ga4._agoranomos_loads`: `is_subject_to_thresholding`, `has_data_loss_from_other_row`, `is_sampled` (`samplingMetadatas` set) and `is_truncated` (`dataTruncationReasons` set), each true when any page of the property in the load had it. C8b ([#228](https://github.com/agoranomos/agoranomos/issues/228)) carries them into `core.loads` under the same names, null for a source whose answers carry none, and into the view of sessions. A flag says "may": a report subject to thresholding may have lost nothing.
* **The quota.** A standard property has 200,000 core tokens a day, 40,000 an hour, 14,000 for one project an hour, and 10 server errors for one project an hour; most requests cost 10 tokens or fewer, and the cost grows with the rows, the dimensions, the length of the date range and the cardinality. A daily load costs two requests a property, `properties.get` and one report with its pages, far below the tokens of a day. A portion of history of a quarter is one report and its pages; a request of more than six months can cost 1,000 tokens or more, so a quarter likely costs some hundreds, within the 14,000 of an hour. The tokens of a property are shared with every other project that reads it, so the load logs `propertyQuota` of each answer, consumed and remaining, to show what it took and what was left.
* **The retries.** The 10 server errors of a project and property an hour bound the retries: the load asks again after a 500 or a 503 at most twice for one request, with a backoff, and fails the property at its fourth server error in one load, so that a daily load and a portion of history in the same hour stay below 10. Other errors, such as 400, 403 or 404, fail the property at once; they count toward the limit of 10,000 errors a project and property in 15 minutes, which a load is far from. A `429 RESOURCE_EXHAUSTED`, which a property returns once any bucket is at zero, fails the property at once when the daily tokens are spent, as they are refreshed at midnight Pacific time, and otherwise waits for the hourly buckets, which are refreshed "within an hour but not necessarily on the whole hour boundaries", within the time of its task, then fails the property. A property that fails is recorded as `failed` in the journal with the error, the others load, and the task fails at the end with one line (ADR-0035).
* **The window and the history.** A daily load of a property takes `[min(yesterday + 1 − 14, last accepted day + 1), yesterday]` in the time zone of the property, from `properties.get` (ADR-0038). 14 days cover the 12 days in which the attribution credit of key events can change and the 7 in which data can arrive late, with two to spare; that is a reading of Google's pages, not a measurement. The DAG runs after the 12 hours in which GA4 processes the daily data of a standard property. The first load takes the window, so that a dashboard fills in minutes, and the history follows in the background by quarters, from new to old, through `history.py`, back to the first day of the month 36 months before yesterday ([ADR-0034](0034-define-a-basic-data-set-and-the-modules-of-an-extended-one.md)). No page read gives the oldest day that `runReport` returns, so no oldest day bounds the plan: a quarter before the property had data costs one request, writes its window into the journal and no row. A history portion brings the key events as GA4 counts them today: marking an event as a key event does not change past data, and a new lookback window of key events, which also applies to the attribution of sessions, applies going forward.
* **No current day.** GA4 takes no load of the kind `intraday`: the intraday data of a standard property comes 2 to 6 hours late and changes after the daily data, and ADR-0051 is for the cost of the ad platforms.
* **The checks.** G1 and G1a stay as they are: G1 reads the form of the sessions and of the list of properties, a currency outside ISO 4217 and a time zone outside IANA among them, and G1a puts a property whose load brings no row where the core holds sessions in quarantine with `empty_window`. *Open to the maintainer's acceptance:* no G0a for GA4 in v0.7. The four flags say what an answer may have lost, and the check of `rowCount` catches a page that the load lost.
* **A change of the currency or the time zone of a property.** After a change of the currency, GA4 converts the previous data to the new currency, so the revenue of every past day changes in GA4. The core keeps what it accepted, each row with its currency (ADR-0036): the days of the next windows come again in the new currency and replace their days whole, and the days before them stay in the old one. A portion of history taken after the change, of days that the core never held, brings them in the new currency. `dim_account` takes the current currency of the property, and the marts convert each row from its own currency by the rates of its day ([ADR-0042](0042-take-the-rates-from-the-bank-of-italy-and-the-national-bank-of-kazakhstan.md)), so a sum in the reporting currency holds, while the revenue of a past day may differ from what GA4 now shows. A change of the time zone "only affects data going forward", so the past days stay as they are, and the window follows the new time zone from the next load.
* **The version.** The source calls `v1beta` of both APIs, from one place in its code. The Data API and the Admin API have `v1alpha` and `v1beta` and no `v1`; of Beta, Google says "No breaking changes are expected in this channel". Google gives no date of deprecation for either `v1beta`, so no test of a sunset comes until it does: a release that finds one adds the date and the test that fails from 90 days before it, as ADR-0032 does for Google Ads.
* *Open to the maintainer's acceptance:* **the dashboard of web analytics.** A dashboard Web analytics joins the content of the package, kept by `metabase-init` on every start as Advertising is ([ADR-0018](0018-keep-the-content-of-the-package-in-metabase-as-code.md)), in every deployment, with the sessions, engaged sessions, key events and revenue of `web_sessions_daily` by source, medium, channel group and campaign, and its description says that it is empty until a source of web analytics loads. A task of its own builds it after C6c, so that C6c stays one session and the maintainer places the task in v0.7 or after. Until then, the card of key events of Advertising shows the key events of GA4, and the demo keeps its own dashboard of web analytics.
* *Open to the maintainer's acceptance:* **a live property.** S6 (#220) adds a GA4 property of the maintainer or of the agency, read by a service account with the role Viewer, for a month of daily loads and its history, and measures the points that More Information lists as unverified. A property with real traffic brings the data of a business onto the stand, which keeps it as it is to keep the accounts of Meta and TikTok.

### Consequences

* Good, because a deployment connects GA4 with no review by Google, a service account, the role Viewer and the read-only scope.
* Good, because the connector and `demo_web` share the shape macros and the fact of the core, so the marts, the card of key events and the tests of the demo hold for real data, and no migration is needed.
* Good, because a key of its own lets a deployment keep GA4 and Google Ads apart, and one that does not care puts the same key in both.
* Good, because each load says, for each property, whether an answer was thresholded, folded into "(other)", sampled or cut, which a day of sessions that cannot match the interface of GA4 needs.
* Good, because a daily load costs two requests a property and a few tokens, far below the quotas of a standard property, and the retries stay within its server errors.
* Bad, because the key of a service account is a long-lived secret: whoever reads `.env` or a copy of it reads every property that the service account is a user of.
* Bad, because an organization created on or after 3 May 2024 forbids keys of service accounts by default, and its administrator has to allow them.
* Bad, because no G0a checks GA4 in v0.7: a loss that no flag reports and no count of rows catches reaches the core.
* Bad, because after a change of the currency of a property, the core holds its past revenue in the old currency and GA4 in the new one.
* Bad, because a dashboard of web analytics in every deployment is empty in a deployment without a source of web analytics, and adds content of the package to keep.
* Bad, because the history depth, the window of 14 days and the share of "(other)" stay readings of Google's pages until S6 measures them.

### Confirmation

C6b checks the requests, the paging, the count of rows, the flags, the errors and the retries, the window, a failed property and a key that never reaches a log in unit tests with answers faked by hand from the reference. C6c checks the faked answers in `core.web_sessions_daily` and `marts.web_sessions_daily` in a stack test, as `tests/stack/test_meta_ads.py` does for Meta. S6 checks the rest on a live property.

## Pros and Cons of the Options

### `ga4`

* Good, because it is the name of the shape macros, `ga4_v1beta`, and the one that Google uses for the product.
* Good, because it is short, in every key of the core and in `clients.toml`.
* Bad, because a later version of Google Analytics would need a code of its own.

### `google_analytics`

* Good, because it follows `google_ads` and names the product without its version.
* Bad, because it differs from the shape macros and from the name that Google, the guides and the users give the product today.

### A variable of its own, `GA4_SERVICE_ACCOUNT_KEY`

* Good, because a deployment can keep the two sources on two service accounts, so that a leak of one key reads one source.
* Good, because one key in both variables still works for a deployment that wants one service account.
* Bad, because a deployment with one service account writes the same secret twice into `.env`.

### The key of Google Ads, `GOOGLE_ADS_SERVICE_ACCOUNT_KEY`

* Good, because a deployment with both sources writes one secret once.
* Bad, because a deployment that connects GA4 alone sets a variable named after Google Ads.
* Bad, because one key always opens both sources, and a deployment cannot keep them apart.

### One variable for every service of Google, such as `GOOGLE_SERVICE_ACCOUNT_KEY`

* Good, because one name serves Google Ads, GA4 and any later service of Google.
* Bad, because it renames the variable of Google Ads, which every deployment with Google Ads has to change, or keeps two names for one key.
* Bad, because one key opens every source of Google.

### A dashboard Web analytics for every deployment, in a task of its own after C6c

* Good, because a deployment that connects GA4 sees its sessions by source, channel and campaign, as the demo shows them, with no question of its own.
* Good, because `metabase-init` keeps it as it keeps Advertising, with no rule on which deployment gets it.
* Good, because C6c stays one session, and the maintainer places the task.
* Bad, because a deployment without a source of web analytics has an empty dashboard.
* Bad, because the release that connects GA4 may come before the dashboard.

### The same dashboard, built in C6c

* Good, because GA4 and its dashboard come in one pull request.
* Bad, because C6c grows to two sessions in a release that already has more than #177 planned.

### The same dashboard, only in a deployment whose marts hold a property of a real source

* Good, because no deployment sees an empty dashboard.
* Bad, because `metabase-init` runs at `./agoranomos up`, so the dashboard comes at the first start after the first load, not with it.
* Bad, because the content of the package then depends on the data, which no other content does.

### Only the card of key events of the dashboard Advertising

* Good, because it needs no work, and ADR-0034 asks for no more.
* Bad, because a deployment with GA4 sees the key events and none of its sessions, sources or channels without building a dashboard itself.

### No G0a for GA4 in v0.7, with the flags and the count of rows

* Good, because GA4 estimates sessions with HyperLogLog++, so the sessions of a day need not equal the sum of its rows, and any tolerance would be a guess until S6.
* Good, because the rows that GA4 folds into "(other)" keep their sessions, so a sum would not see them, while `dataLossFromOtherRow` does.
* Good, because thresholding falls on demographic and audience data, none of it in this shape, and `subjectToThresholding` says where it applies.
* Good, because it needs no migration, and the count of `rowCount` catches the page that a load loses.
* Bad, because a loss that GA4 does not report and that keeps the count of rows reaches the core unchecked.

### The totals of each property by day in a table of their own

* Good, because it checks the rows of a day against the property as GA4 sums it.
* Bad, because it needs a migration and a request more a property, and a tolerance for the estimates that S6 has not measured.
* Bad, because it does not see what "(other)" holds, which the sum keeps.

### The totals in `core.platform_totals_daily`

* Good, because G0a keeps one table.
* Bad, because a migration loosens the `NOT NULL` of impressions, clicks and spend, which every ad platform has, for a source that has none of them.
* Bad, because it has the faults of the table of its own too.

### A GA4 property in S6

* Good, because the history depth, the window of 14 days, the share of "(other)", the flags and the quota are measured on a live property before a deployment relies on them.
* Good, because S6 already plans a stand with real platforms, with its setup and the keeping of their data.
* Bad, because S6 grows, and the maintainer or the agency has to give a property with real traffic.

### A task of the stand of its own for GA4

* Good, because S6 stays as planned, for Meta and TikTok.
* Bad, because GA4 waits for a task that no release plans yet.

### The points left unverified

* Good, because it costs nothing now.
* Bad, because a deployment learns them by its data, and the window of 14 days stays a reading.

## More Information

* **Implementation status, as of November 2026:** none of this record is built. C6b ([#226](https://github.com/agoranomos/agoranomos/issues/226)) loads the properties into the raw layer with their history, C6c takes them into the core and the marts, and C8b ([#228](https://github.com/agoranomos/agoranomos/issues/228)) carries the flags into `core.loads`. The maintainer accepted the record on 22 November 2026.
* **Not checked yet.** S6 (#220), if the maintainer accepts it, or a deployment, settles each of these on a live property:
    * How far back `runReport` returns the days of a property. The retention of user data "does not affect standard aggregated reports" and "only affects explorations and funnel reports", and the dimensions of this shape are those of standard reports, so the 36 months of ADR-0034 are likely there, but no page says so of the Data API.
    * Whether the project needs a billing account for the Data API. Google's page on the billing of APIs says that "Some Google APIs charge for usage" and names none, and the quickstart of the Data API does not mention billing; ADR-0032 found that the Google Ads API costs nothing in October 2026, and no page read says the same of the Data API.
    * The steps of adding a service account as a user of a property: the quickstart shows those of a user account only, and the guide of the MCP server of Google asks for the role Viewer on the property for "your user or service account". The guide of C6b checks them in the interface of GA4.
    * Whether `orderBys` is needed for pages that follow one order, which no page read says, and what a property with a data restriction such as "No Revenue Metrics" returns for `totalRevenue`.
    * The share of "(other)", thresholding and sampling on a real property, and the tokens that a daily load and a portion of a quarter cost.
    * The window of 14 days, against a month of the journal of a live property.
    * Which exchange rate GA4 uses to convert past data after a change of the currency of a property: the page on currencies gives the rate of the day before for transactions in a local currency, and says nothing of the rate of a change.
* **The facts behind this record**, read on 20 November 2026 for #226 and again on 20 November 2026 for this record, as of November 2026; Google's pages carry no date of their own, but for the blog post on HyperLogLog++:
    * Versions: the Data API has the channels `v1alpha` and `v1beta`; of Beta, "No breaking changes are expected in this channel", and of Alpha, "expect to encounter breaking changes before the API is publicly released"; no date of deprecation ([Data API](https://developers.google.com/analytics/devguides/reporting/data/v1)). The newest entries of the changelog are of 14 September 2026 and add the reasons of data truncation to the answers ([Changelog](https://developers.google.com/analytics/devguides/reporting/data/v1/changelog)).
    * `runReport`: `POST https://analyticsdata.googleapis.com/v1beta/{property=properties/*}:runReport`, with the scopes `analytics.readonly` or `analytics`; for `limit`, "If unspecified, 10,000 rows are returned" and "The API returns a maximum of 250,000 rows per request, no matter how many you ask for"; `offset` is "The row count of the start row"; for `keepEmptyRows`, "If false or unspecified, each row with all metrics equal to 0 will not be returned"; `returnPropertyQuota` "Toggles whether to return the current state of this Google Analytics property's quota"; the page gives no earliest date and no largest number of date ranges ([runReport](https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/properties/runReport)). `rowCount` is "The total number of rows in the query result", and `propertyQuota` "This Google Analytics property's quota state including this request" ([RunReportResponse](https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/RunReportResponse)). A metric value is `"value": string` ([Row](https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/Row)). "you can use up to nine dimensions per request" ([Basics](https://developers.google.com/analytics/devguides/reporting/data/v1/basics)).
    * The answer: `dataLossFromOtherRow`, "If true, indicates some buckets of dimension combinations are rolled into "(other)" row"; `subjectToThresholding`, a report that returns only data that meets the minimum aggregation thresholds, which does not mean that rows were removed; `samplingMetadatas`, one for each date range of a sampled report; `dataTruncationReasons`, "If set, indicate there is data truncation in the report"; `currencyCode`, the currency of the request or the current one of the property; `timeZone`, "The property's current timezone" ([ResponseMetaData](https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/ResponseMetaData)).
    * The dimensions and metrics: `sessionCampaignId`, "The marketing campaign ID for a session. Includes Google Ads Campaigns, Manual Campaigns, & other Campaigns."; `sessionGoogleAdsCampaignId`, "The Campaign ID for the Google Ads Campaign that led to this session."; `sessionManualCampaignId`, "Populated by `utm_id` URL parameter."; `date`, "formatted as YYYYMMDD"; `keyEvents`, "Marking an event as a key event affects reports from time of creation." ([API schema](https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema)). `conversions` gave way to `keyEvents` on 6 May 2024 ([Changelog](https://developers.google.com/analytics/devguides/reporting/data/v1/changelog)).
    * Quotas of a standard property: "Core Tokens Per Property Per Day" 200,000, "Per Hour" 40,000, "Core Tokens Per Project Per Property Per Hour" 14,000, "Core Concurrent Requests Per Property" 10, "Core Server Errors Per Project Per Property Per Hour" 10; "Server Errors are 500 and 503 codes"; "most requests will charge 10 or fewer tokens"; "If any quota bucket reaches 0 remaining, subsequent requests to that property return a 429 RESOURCE_EXHAUSTED error"; "All requests consume property quotas"; "All daily quotas are refreshed at midnight Pacific Standard Time" and "All hourly quotas are refreshed within an hour but not necessarily on the whole hour boundaries"; the dimensions potentially thresholded are `userAgeBracket`, `userGender`, `brandingInterest`, `audienceId` and `audienceName` ([Quotas](https://developers.google.com/analytics/devguides/reporting/data/v1/quotas)). On 11 May 2023, "requests for more than 6 months of data can charge 1,000 or more quota tokens", and on 28 June 2024, "a limit of 10,000 API server errors allowed per project, per property, within a 15-minute window", which "includes all requests resulting in a response with any code other than `500` or `200`" ([Changelog](https://developers.google.com/analytics/devguides/reporting/data/v1/changelog)).
    * The Admin API: `GET https://analyticsadmin.googleapis.com/v1beta/{name=properties/*}`, with the scopes `analytics.readonly` or `analytics.edit` ([properties.get](https://developers.google.com/analytics/devguides/config/admin/v1/rest/v1beta/properties/get)); `name` is `properties/{property_id}`; `timeZone` is the "Reporting Time Zone, used as the day boundary for reports", and "Changing the time zone only affects data going forward, and is not applied retroactively"; `currencyCode` is "The currency type used in reports involving monetary values" ([Property](https://developers.google.com/analytics/devguides/config/admin/v1/rest/v1beta/properties)).
    * The sign-in: the project enables `analyticsadmin.googleapis.com` and `analyticsdata.googleapis.com`, the scope is `analytics.readonly`, and "your user or service account" needs at least the role Viewer, `roles/analytics.viewer`, on the property ([MCP guide](https://developers.google.com/analytics/devguides/MCP)). A service account exchanges a signed JWT at `https://oauth2.googleapis.com/token`, the example gives `"expires_in": 3600`, and "When possible, the HTTP header is preferable, because query strings tend to be visible in server logs" ([Service accounts](https://developers.google.com/identity/protocols/oauth2/service-account)). The role Viewer "Can see settings and data", the least of the roles that see data, and the restrictions No Cost Metrics and No Revenue Metrics can hide those metrics from a user ([Access and roles](https://support.google.com/analytics/answer/9305587)). The security baseline with `constraints/iam.managed.disableServiceAccountKeyCreation` is enforced for organizations created "on or after May 3, 2024" ([Secure-by-default organizations](https://docs.cloud.google.com/resource-manager/docs/secure-by-default-organizations)). "Some Google APIs charge for usage" ([APIs and billing](https://support.google.com/googleapi/answer/6158867)).
    * Freshness: the daily data of a standard property takes "12 hours" to process, and "Data processing can take 24-48 hours"; "Some data can arrive late, potentially up to 7 days delayed"; "Attribution credit for key events can change for up to 12 days after the key event is recorded"; "Data in reports and API queries may change after the daily data becomes available"; the intraday data of a standard property takes 2 to 6 hours ([Data freshness](https://support.google.com/analytics/answer/11198161)). "The key event lookback window you choose also applies to session attribution", 90 days by default for most key events, and "Changes to the conversion window apply going forward" ([Lookback window](https://support.google.com/analytics/answer/16291704)).
    * Retention, currency and the quality of an answer: "the data retention setting does not affect standard aggregated reports" and "only affects explorations and funnel reports" ([Data retention](https://support.google.com/analytics/answer/7667196)). "Changes to the global currency type will affect future as well as historical data, and previous data will be converted to the newly configured currency" ([Currency reference](https://support.google.com/analytics/answer/9796179)). "(other)" appears "when the number of rows in a table exceeds the table's row limit", and "Any dimension with more than 500 values should be considered a high-cardinality dimension" ([The (other) row](https://support.google.com/analytics/answer/13208658)). "The quota limit for event level queries is 10 million events for standard Google Analytics properties" ([Data sampling](https://support.google.com/analytics/answer/13331292)). Data may be withheld in "an API call that includes demographic data" ([Data thresholds](https://support.google.com/analytics/answer/9383630)). "Google Analytics 4 properties use HyperLogLog++ (HLL++) algorithm to estimate cardinality for most used metrics", sessions with a precision of 12 ([HLL++ in Google Analytics](https://developers.google.com/analytics/blog/2022/hll), October 2022).
    * Libraries: `google-analytics-data` 0.23.3 and `google-analytics-admin` 0.30.2, both uploaded on 1 October 2026 under Apache-2.0, require `google-api-core[grpc]`, `grpcio`, `proto-plus` and `protobuf`; `google-auth` is 2.62.2, uploaded on 10 November 2026 ([google-analytics-data](https://pypi.org/project/google-analytics-data/), [google-analytics-admin](https://pypi.org/project/google-analytics-admin/), [google-auth](https://pypi.org/project/google-auth/)). The Airflow image pins `dlt[postgres]` 1.32.0 (`airflow/requirements.txt`), and `google_ads.py` already signs in with `google.oauth2.service_account`.
    * Not read: the reference of `MetricValue` (404 on 20 November 2026), whose form the page of `Row` gives; the older quickstart with the steps of a service account as a user of a property, which the brief of #226 saw in a search snippet only; the listing of the Data API in the console of Google Cloud, which needs a sign-in, so its price.
* Related decisions: the first connector and its rules for a real platform ([ADR-0032](0032-connect-google-ads-first-with-the-reference-rates-of-the-ecb.md)), the connector of Meta, the model of this record ([ADR-0050](0050-connect-meta-with-an-app-and-a-system-user-of-the-deployment.md)), one business and its file of clients ([ADR-0024](0024-one-deployment-is-one-business.md)), the shape macros ([ADR-0025](0025-model-advertising-statistics-once.md)), the key events beside the conversions and the history of 36 months ([ADR-0034](0034-define-a-basic-data-set-and-the-modules-of-an-extended-one.md)), G0a, G1, G1a and quarantine ([ADR-0035](0035-keep-four-layers-and-erase-the-raw-layer-after-acceptance.md)), the fact on text keys and the currency of each row ([ADR-0036](0036-model-the-core-as-narrow-facts-and-versioned-dimensions.md)), the windows and the journal of loads ([ADR-0038](0038-load-by-copy-and-write-the-core-by-whole-days.md)), the core changed by migrations ([ADR-0044](0044-keep-the-core-for-good-and-change-it-by-migrations.md)), the pin and the acceptance ([ADR-0046](0046-pin-the-loads-before-the-transform-and-accept-them-before-the-cleanup.md)), and the current day ([ADR-0051](0051-take-the-current-day-as-a-load-of-the-kind-intraday.md)).
* Revisit this record when S6 or a deployment settles a point above, when Google publishes a date of deprecation of `v1beta` or a `v1`, changes the quotas of a standard property, or when a deployment asks for the campaign IDs of Google Ads and of `utm_id` apart.
