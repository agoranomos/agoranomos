---
status: proposed
date: 2026-10-29
decision-makers: Andrei Gromov
---

# Connect Google Ads first, with the reference rates of the European Central Bank

## Context and Problem Statement

Phase 4 of the roadmap brings the connectors of real platforms, one source in each minor release, starting with v0.4.0. Until now only the demo runs. [ADR-0025](0025-model-advertising-statistics-once.md) gave it four fictional platforms in the shapes of the APIs of Google Ads (v25), Meta (v26), TikTok (v1.3) and GA4 (v1beta), and a fictional source of the reference rates of the euro; each shape has macros in `dbt/macros/shapes/` that the demo and the connector of its platform are to share. ADR-0025 also left two things to the first real connector: ad groups and ads, each from a report of its own, and a source of exchange rates, without which a deployment reports in USD or EUR only.

Every source keeps the contract of [ADR-0004](0004-ingest-with-dlt.md): a date window in the time zone of the account, safe repeats, explicit retries and loud failures. The architecture adds four rules for real platforms: an app of each deployment, tokens that expire, asynchronous reports and daily quotas. [ADR-0024](0024-one-deployment-is-one-business.md) makes one deployment serve one business, whose `extensions/model/clients.toml` names its clients and the ad accounts of each.

Google changed the access to its API in 2026. Developer tokens were sunset on 9 September 2026: the access level now belongs to the Google Cloud project whose credentials call the API, and no developer token or manager account is needed. Since November 2024, a service account also reaches the Google Ads API directly, as a user of an account, without domain-wide delegation (see More Information).

Which source comes first, how does a deployment connect it, which accounts does it load and how, and where do the exchange rates come from?

## Decision Drivers

* A deployment connects the source with steps that one person can follow, without waiting for a review by a person at the platform.
* The source serves one business (ADR-0024): a manager account of an agency, which reaches the accounts of many businesses, brings into a deployment only the accounts of its business.
* The connector fills the model that the demo fills, through the same shape macros (ADR-0025), so that the dashboards, the tests and the skills of the demo hold for real data.
* The daily load fits the quota of the lowest access level that reads real accounts.
* No heavy dependency next to Airflow, every package pinned, and every secret in `.env` (AGENTS.md).
* Many of the businesses for which Agoranomos is built spend on the platform.

## Considered Options

For the first source:

* Google Ads
* Meta Ads
* GA4, the web analytics of Google
* TikTok, Microsoft Advertising or LinkedIn

For the client of the API:

* The REST interface of the Google Ads API, through the REST client of dlt
* The Python library `google-ads`
* The verified source `google_ads` of dlt

For the sign-in:

* A service account of the project of the deployment, with its key in `.env`
* The refresh token of a person

For the accounts that the source loads:

* The accounts of `google_ads` that `extensions/model/clients.toml` names
* Every account that the credentials reach
* A list in a file of its own

For the exchange rates:

* The data API of the European Central Bank
* Frankfurter, an open API that also serves the rates of the ECB
* A commercial API with a key

## Decision Outcome

Chosen options: "Google Ads", "The REST interface of the Google Ads API, through the REST client of dlt", "A service account of the project of the deployment", "The accounts of `google_ads` that `extensions/model/clients.toml` names" and "The data API of the European Central Bank", because Google Ads is the one large ad platform that a deployment can connect to production accounts without a review by a person, its reports are synchronous, its shape is the one that `demo_search` already has, and the ECB publishes the reference rates that the model already expects, free and without a key.

In practice this means:

* **The first source.** v0.4.0 connects Google Ads as the platform `google_ads`, into the schemas `raw_google_ads` and `raw_google_ads_staging`, through the shape macros that `demo_search` calls. Each later source gets a record of its own; More Information lists what each candidate needs as of October 2026.
* **The project of the deployment.** Each deployment creates a Google Cloud project of its own, turns the Google Ads API on in it, which gives Test access to test accounts, and applies for Explorer access, which reads production accounts up to 2,880 operations a day and is granted after an automated review in most cases. A deployment that outgrows it applies for Basic access, 15,000 operations a day, after the brand verification of its project.
* **The sign-in.** A service account of that project is added as a user of each Google Ads account of the business, or of its manager account, in Admin > Access and security, with any access level but Email only. Its key, the JSON file that Google Cloud makes, goes into `.env` with the other secrets, and the source makes an access token from it at the start of each run. `GOOGLE_ADS_LOGIN_CUSTOMER_ID` names the manager account when the service account reaches the accounts through one, and stays empty when it is a user of each account. `./agoranomos` is to write the key into `.env` from the file, check that it signs in, and never print it.
* **The accounts.** The source loads the accounts of the platform `google_ads` that `extensions/model/clients.toml` names, and no other. A manager account of an agency reaches the accounts of all its businesses, and the file keeps the deployment to its own. A Google Ads account is therefore loaded only once its client is named, and the default client `unassigned` of ADR-0024 never applies to one.
* **The client of the API.** The source calls `googleAds:searchStream` of the REST interface with the REST client of dlt and explicit retries, and makes its access token with google-auth, which the Airflow image already holds and `airflow/requirements.txt` is to pin. dlt normalizes the JSON of the REST interface into the columns that the shape macros read, such as `metrics__cost_micros` from `metrics.costMicros`, with the INT64 numbers as strings. The library `google-ads` is not used.
* **The version of the API.** Each release calls the latest major version: v26 for v0.4.0 (as of October 2026), with the shape macros of `google_ads_v25` moved to v26 where a field changed. Google sunsets a major version about 12 months after its release. The source names its version and the month of its sunset in one place; a unit test of the repository fails from 90 days before that month, and each run of the source warns in its log from then on, so that the project and a deployment on an old release both know in time.
* **The reports.** For each account, a run makes one SearchStream request for each report, over its whole window: the account (`customer`), its campaigns, ad groups (`ad_group`) and ads (`ad_group_ad`), the daily statistics of each of those three, and their daily conversions by conversion action, each in a request of its own, as `segments.conversion_action` cannot be selected together with `metrics.cost_micros`. The statistics of ad groups and ads go into new marts of the families `ad_stats_<grain>_daily` and `ad_conversions_<grain>_daily`, each from its own report and never a sum of a lower level (ADR-0025). Performance Max reports no ad groups and no ads, so its campaigns count at the grain of campaigns only, and its asset groups are left to a record of their own. `demo_search` gets ad groups and ads in the same release, so that the stack tests build the new marts.
* **The windows and the quota.** A daily run loads, for each account, the days from the day after its last load, or from 90 days back if that is earlier, to yesterday in the time zone of the account, as `demo_search` does, because conversions keep coming for up to 90 days after a click. A SearchStream request counts as one operation however many rows it returns, so the window costs no more than one day: ten operations for each account and day, and Explorer access carries the daily load of about 280 accounts. The first load of an account takes the last 18 months, like the demo, one month in each request of statistics, about 110 operations. A failed request counts too, so the retries have a limit.
* **Tokens that expire.** A key of a service account has no expiry unless a policy of the organization sets one, and each run makes a new access token from it. Google revokes the access of a project that has made no call for 90 days, which a daily load prevents. A key that no longer signs in, or an account that removed the service account, fails the run with the account and the error of the API, and Pipeline health shows the account as late.
* **The exchange rates.** A source `ecb` loads the daily reference rates of the euro from the data API of the ECB, the series `EXR/D..EUR.SP00.A` as CSV with no key, into `raw_ecb`, in the shape `euro_rates` that `demo_fx` has. The ECB publishes them at about 16:00 CET on each TARGET working day, for 29 currencies as of October 2026. Each run reloads the last 7 days. With them, `AGORANOMOS_REPORTING_CURRENCY` can be EUR or any currency that the ECB publishes. The rates load in every deployment that has a real source. The demo alone keeps its fictional rates and asks the ECB nothing, so the stack tests of the demo need no network. Where both sources have a rate for a day, the model takes the rate of the ECB. The description of `fx_rates_daily` names the ECB as the source, as its terms of reuse ask.

The other options were not chosen for these reasons:

* **Meta Ads first.** Reading the ad accounts of one's own business needs no App Review of the permissions, but the default access tier of the Marketing API, Limited access, is "For development only. Not for production apps running for live advertisers", and Full access needs App Review after at least 500 calls in 15 days. A deployment would wait for a review before its data could be trusted.
* **GA4 first.** It has the easiest sign-in of all, a service account with the Viewer role on a property, and 200,000 tokens a day for a standard property. But it brings sessions, not spend, and the marts of advertising would stay on the demo.
* **TikTok, Microsoft Advertising or LinkedIn first.** Each needs an app approved or registered first. TikTok reviews a developer app in 2 to 3 business days. Microsoft Advertising needs a developer token and an app registered in Microsoft Entra ID (Azure Active Directory), and its reports are asynchronous. LinkedIn grants its Advertising API on application.
* **The library `google-ads`.** It returns protobuf messages rather than the JSON that the shape macros read, and it brings gRPC. The Google provider of the Airflow image holds version 32.0.0 (as of October 2026), while v26 needs 34.0.0, so the library would need a virtual environment of its own, as dbt has, for what one REST request does.
* **The verified source `google_ads` of dlt** is a community source, "tested only once", that loads the customers, the IDs and labels of campaigns, change events and customer clients, and no statistics. It requires a developer token, and its service accounts need domain-wide delegation. Its code last changed on 1 October 2024.
* **The refresh token of a person.** Google recommends the service account for an app that manages accounts that its owner can already reach. A refresh token expires in 7 days while the consent screen of the project is in Testing, stops after six months unused, and since 2026 the person has to sign in with a passkey to make one, which can take up to 7 days to work after it is created. It stays an option for a deployment whose organization forbids keys, in a record of its own if one needs it.
* **Every account that the credentials reach** would bring the accounts of the other businesses of an agency into the deployment, against ADR-0024.
* **A list in a file of its own** would name each account twice, once to load it and once for its client.
* **Frankfurter** now blends the rates of 106 central banks by default and serves the rates of the ECB from its own servers: a third party between the deployment and the source, under the terms of the ECB all the same.
* **A commercial API** needs a key and an account for rates that the ECB publishes free.

### Consequences

* Good, because a deployment connects its first real platform with no developer token, no manager account and, in most cases, no review by a person at Google.
* Good, because the demo and the connector share the shape macros, so the dashboards, the tests and the skills of the demo hold for real data.
* Good, because a request costs one operation whatever its window, so the reload of 90 days fits Explorer access.
* Good, because the reporting currency can be any of the 29 currencies of the ECB.
* Bad, because the key of a service account is a long-lived secret: whoever reads `.env` or a copy of it reads every Google Ads account of the deployment.
* Bad, because in an organization of Google Cloud created on or after 3 May 2024, the policy `constraints/iam.managed.disableServiceAccountKeyCreation` forbids keys of service accounts by default, and an administrator of the policies of the organization has to allow them for the project.
* Bad, because Google rejects Explorer and Basic access for projects on the free trial of Google Cloud or with a suspended or disabled billing account (a known issue as of October 2026), so a project may need a billing account in good standing, although the API costs nothing.
* Bad, because one service account can be a user of at most 20 Google Ads accounts: a business with more adds it to a manager account and sets `GOOGLE_ADS_LOGIN_CUSTOMER_ID`.
* Bad, because the connector moves to a new major version of the API at least once a year, and a deployment that stays on an old release stops loading when its version is sunset.
* Bad, because the marts of ad groups and ads do not add up to the campaigns where a campaign has neither, as in Performance Max.
* Bad, because an account in a currency that the ECB does not publish, such as the tenge (KZT) or the dirham (AED), gets no rate. Such a currency fails the tests of the models whose rows it converts, and the marts keep their last good data until a source of its rates comes, in a record of its own.
* Bad, because a Google Ads account that the file of clients does not name is not loaded at all, while ADR-0024 counts an account that no file names as Unassigned.

## More Information

* **Implementation status, as of October 2026:** nothing of this record is built. The demo platforms `demo_search` and `demo_fx` have the shapes of the Google Ads API v25 and of the reference rates of the euro, and the macros `google_ads_v25` and `euro_rates` of `dbt/macros/shapes/` read them.
* The facts behind this record, read on 29 October 2026:
    * Google Ads API, access: "Developer tokens were sunset on September 9, 2026", and "Your API access levels are determined by the Google Cloud project you used to generate your OAuth credentials"; a developer token in the headers is "optional and ignored by the API servers" ([Developer token](https://developers.google.com/google-ads/api/docs/api-policy/developer-token)). Test access comes when the API is turned on in the project and reaches test accounts only; Explorer access gives "2,880 operations / day for production accounts", by an automated review in most cases; Basic access gives 15,000 a day, and brand verification is "a prerequisite"; Standard access has no limit, after a manual audit of about 10 business days ([Access levels](https://developers.google.com/google-ads/api/docs/api-policy/access-levels)). Explorer access cannot use the services of account creation, user management, planning and billing; projects on the Cloud Free Trial or with a suspended or disabled billing account are rejected for Explorer and Basic access, a known issue; Google revokes the access of a project unused for 90 days ([Developer token](https://developers.google.com/google-ads/api/docs/api-policy/developer-token)).
    * Google Ads API, quotas: "A Search or SearchStream request counts as one operation", "irrespective of the number of batches"; a request with a valid `next_page_token` is not counted; a failed request counts; a day is "a sliding 24 hour time period" ([Quotas](https://developers.google.com/google-ads/api/docs/best-practices/quotas)).
    * Google Ads API, service accounts: since 27 November 2024 a service account needs no Google Workspace and no domain-wide impersonation, and an offline app needs no refresh token ([the blog of the Google Ads API team](https://ads-developers.googleblog.com/2024/11/improved-support-for-service-accounts.html)). An admin adds its email in Admin > Access and security; "You can associate up to 20 Google Ads accounts (including Google Ads manager accounts) with a single email address"; "service accounts don't support the Email only access level"; `login-customer-id` is the 10-digit ID of the manager account, set only when the service account reaches accounts through one ([Service accounts](https://developers.google.com/google-ads/api/docs/oauth/service-accounts)). "Google recommends using the service account workflow over the single-user authentication workflow" ([Single-user authentication](https://developers.google.com/google-ads/api/docs/oauth/single-user-authentication)).
    * Google Ads API, refresh tokens: a consent screen in Testing "is issued a refresh token expiring in 7 days"; a token stops after six months unused, and after 100 tokens of one account for one client the oldest is invalidated ([Using OAuth 2.0](https://developers.google.com/identity/protocols/oauth2)); the person who makes a refresh token for the Google Ads API must sign in with a passkey, which can take up to 7 days to work after it is created ([Security requirements](https://developers.google.com/google-ads/api/docs/oauth/security-requirements)).
    * Google Ads API, versions: v26 came out on 14 October 2026 and is the latest; v25 (22 July 2026) is to be sunset in August 2027 and v26 in November 2027; "Major versions have a lifespan of around 12 months", and the sunset dates are tentative ([Sunset dates](https://developers.google.com/google-ads/api/docs/sunset-dates)).
    * Google Ads API, reports: a query goes to `GoogleAdsService.SearchStream` or `GoogleAdsService.Search`, and the API has no asynchronous report jobs ([Reporting](https://developers.google.com/google-ads/api/docs/reporting/overview)); campaigns, ad groups and ads are `campaign`, `ad_group` and `ad_group_ad`; for Performance Max, "querying resources such as ad_group or ad_group_ad won't return any data" ([Performance Max reporting](https://developers.google.com/google-ads/api/performance-max/reporting)); `segments.conversion_action` cannot be selected with `metrics.cost_micros`, `metrics.clicks` or `metrics.impressions` ([Segments v26](https://developers.google.com/google-ads/api/fields/v26/segments)).
    * The library `google-ads` 34.0.0 (15 October 2026) calls v23 to v26, and v26 needs 34.0.0 at least ([PyPI](https://pypi.org/project/google-ads/), [Sunset dates](https://developers.google.com/google-ads/api/docs/sunset-dates)). The Airflow image of v0.3.2 holds `apache-airflow-providers-google` 22.5.0, `google-ads` 32.0.0 and `google-auth` 2.58.0, as `importlib.metadata` of its scheduler shows.
    * The verified source `google_ads` of dlt: "This source is a Community source and was tested only once"; it loads `customers`, `campaigns`, `change_events` and `customer_clients`, takes a `dev_token`, and impersonates an email for a service account ([verified-sources](https://github.com/dlt-hub/verified-sources/tree/master/sources/google_ads)). dlt 1.31.0 came out on 7 October 2026 ([PyPI](https://pypi.org/project/dlt/)).
    * Google Cloud: the security baseline, with `constraints/iam.managed.disableServiceAccountKeyCreation`, is "enforced for all organizations created on or after May 3, 2024" ([Secure-by-default organizations](https://docs.cloud.google.com/resource-manager/docs/secure-by-default-organizations)).
    * The ECB: the data API serves `https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A` for one currency and `D..EUR.SP00.A` for all, with `format=csvdata` and `updatedAfter` ([Data API](https://data.ecb.europa.eu/help/api/data)); a request without a key returned CSV. "The reference rates are usually updated at around 16:00 CET every working day, except on TARGET closing days", and "are published for information purposes only" ([Euro foreign exchange reference rates](https://www.ecb.europa.eu/stats/policy_and_exchange_rates/euro_reference_exchange_rates/html/index.en.html)); TARGET closes on weekends, 1 January, Good Friday, Easter Monday, 1 May, 25 and 26 December ([T2](https://www.ecb.europa.eu/paym/target/t2/html/index.en.html)). The series of 28 October 2026 had 29 currencies: AUD, BRL, CAD, CHF, CNY, CZK, DKK, GBP, HKD, HUF, IDR, ILS, INR, ISK, JPY, KRW, MXN, MYR, NOK, NZD, PHP, PLN, RON, SEK, SGD, THB, TRY, USD and ZAR. Its information may be used freely if it appears "accurately and the ECB must be cited as the source" ([Disclaimer and copyright](https://www.ecb.europa.eu/services/using-our-site/disclaimer/html/index.en.html)).
    * Frankfurter: "a free exchange rates API for 228 currencies", from "106 central banks and official sources", with the rates of the ECB at `/v2/providers/ecb/rates`, under the MIT License for its code ([Frankfurter](https://frankfurter.dev/)).
    * The other candidates: Meta's Limited access is "Automatically granted when you add the Marketing API product", and "For development only. Not for production apps running for live advertisers"; Full access needs App Review and "at least 500 Marketing API calls in the last 15 days" ([Authorization](https://developers.facebook.com/documentation/ads-commerce/marketing-api/get-started/authorization)); a system user token can be non-expiring or valid for 60 days ([System users](https://developers.facebook.com/docs/business-management-apis/system-users/install-apps-and-generate-tokens)). TikTok reviews a developer app in "2 to 3 business days", and its long-term access token "does not expire" ([Create a developer app](https://business-api.tiktok.com/portal/docs/create-a-developer-app/v1.3), [Long-term access token](https://business-api.tiktok.com/portal/docs/obtain-a-long-term-access-token/v1.3)). The GA4 Data API takes a service account with the Viewer role on a property, and a standard property has 200,000 core tokens a day ([Quotas](https://developers.google.com/analytics/devguides/reporting/data/v1/quotas)). Microsoft Advertising needs "a developer token and valid user credentials", and "Reporting is an asynchronous process" ([Get started](https://learn.microsoft.com/en-us/advertising/guides/get-started), [Request and download a report](https://learn.microsoft.com/en-us/advertising/guides/request-download-report)). LinkedIn grants its Advertising API to developers who apply under the Products tab of their app ([Quick start](https://learn.microsoft.com/en-us/linkedin/marketing/quick-start)).
* Related decisions: the contract of each source ([ADR-0004](0004-ingest-with-dlt.md)), one deployment for one business and its file of clients ([ADR-0024](0024-one-deployment-is-one-business.md)), the model and its shapes ([ADR-0025](0025-model-advertising-statistics-once.md)), and the roles that load and read ([ADR-0007](0007-ai-access-via-mcp-read-only.md)).
* Revisit this record when Google changes its access levels or its service accounts, when a deployment cannot create a key, or when a business needs the rates of a currency that the ECB does not publish.
