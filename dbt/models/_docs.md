{#- The descriptions of the columns that more than one model has: a column takes the block of its
    own name, so that it says the same in every model (docs/reference/describe-the-marts.md). -#}

{% docs report_date %}
The day that the row covers, in the time zone of its account: of the ad account for advertising statistics, whose time_zone ad_accounts gives, and of the property for web analytics. It is not the day that the data was loaded. A sum over accounts in different time zones adds up days that cover different hours.
{% enddocs %}

{% docs platform %}
The code of the platform that the row comes from, or, in the settings of the model, of the account or the conversion action that they name, such as demo_search; ad_platforms lists the platforms of the model. The platforms of the demo are fictional and have codes of their own; the codes of real platforms come with their connectors.
{% enddocs %}

{% docs platform_name %}
The name of the platform of the row, as ad_platforms gives it.
{% enddocs %}

{% docs kind %}
What a platform gives: ads, advertising statistics; web_analytics, sessions of a site; or exchange_rates, the rates of currencies.
{% enddocs %}

{% docs mimics %}
For a fictional platform of the demo, the API whose shape its data follows, with its version, such as Google Ads API v25.
{% enddocs %}

{% docs is_demo %}
Whether the platform is a fictional one of the demo.
{% enddocs %}

{% docs client_id %}
The ID of a client of the business, a brand, shop or legal entity, as the settings of the model name it: those of the deployment in extensions/model/clients.toml, or the file of the demo. In a row of an account or of its data, the client that the account belongs to; unassigned for an account that no setting names.
{% enddocs %}

{% docs client_name %}
The name of the client of the row, as the settings of the model give it: in a row of an account or of its data, of the client that the account belongs to; Unassigned for an account that no setting names.
{% enddocs %}

{% docs is_default %}
Whether the client is the default one, Unassigned, which holds the accounts that no setting names.
{% enddocs %}

{% docs origin %}
Where the row of the settings comes from: extensions, the settings of this deployment in its extensions folder; demo, the file of the demo in the base package; or base, for the default client Unassigned, which the model adds itself.
{% enddocs %}

{% docs account_id %}
The ID of the ad account that the row belongs to, as the ad platform gives it, as text; in the settings of the clients, also the ID of a property of web analytics. platform and account_id together identify an account.
{% enddocs %}

{% docs account_sk %}
The surrogate key of the account in the core, account_sk of core.dim_account, which never changes for an account and which every fact of the core carries.
{% enddocs %}

{% docs account_name %}
The current name of the ad account, as the ad platform gives it, where anyone with access to the account can change it.
{% enddocs %}

{% docs campaign_id %}
The ID of the campaign, as the ad platform gives it, as text. platform, account_id and campaign_id together identify a campaign on any platform.
{% enddocs %}

{% docs campaign_name %}
The current name of the campaign, the same on every day of the campaign, so that a group by name does not split a renamed campaign; ad_campaign_names holds the names that it had. The name comes from the ad platform, where anyone with access to the account can change it.
{% enddocs %}

{% docs loaded_campaign_name %}
The name of the campaign that the row carried when it was loaded. A platform gives the current name for every day of a report, so rows loaded at different times can carry different names of one campaign.
{% enddocs %}

{% docs observed_name %}
A name that the campaign, the ad group or the ad had, from valid_from until valid_to, as the loads saw it; none for an ad of a type that has no name on its platform, such as a responsive search ad of Google.
{% enddocs %}

{% docs current_name %}
The name of the campaign, the ad group or the ad of the row as its newest version in the lists of its account has it, or, for one that only the statistics name, as its newest row of statistics carries it; none for an ad of a type that has no name on its platform, such as a responsive search ad of Google.
{% enddocs %}

{% docs currency %}
A currency, as a three-letter code of ISO 4217, such as USD. In a row of statistics or sessions, the currency of its ad account or property, in whose units its amounts of money are: amounts in rows with different codes are in different units, and the columns that end in _reporting add them up in one. In the rates into the reporting currency, as in fx_rates_daily, the currency that a rate converts from.
{% enddocs %}

{% docs reporting_currency %}
The currency of the columns that end in _reporting, as a three-letter code of ISO 4217: the reporting currency of the deployment, which the setting AGORANOMOS_REPORTING_CURRENCY sets, the same in every row.
{% enddocs %}

{% docs impressions %}
How many times the ads of the row, of its campaign, its ad group or its ad, were shown on the day, as the ad platform counts them. It counts showings, not people: one person can see an ad many times, so impressions are not reach.
{% enddocs %}

{% docs clicks %}
How many times the ads of the row, of its campaign, its ad group or its ad, were clicked on the day with a click that leads to the site, as the ad platform counts them: the clicks of Google, the clicks (destination) of TikTok, and the link clicks of Meta, whose own clicks count every click. It counts clicks, not people, and not the visits that web analytics records.
{% enddocs %}

{% docs spend %}
What the ads of the row, of its campaign, its ad group or its ad, cost on the day, as the ad platform reports it in its statistics, in the currency of the account (see currency), exact to six decimal places. Also called cost or ad spend. It is not the amount that the platform invoices, which can differ from it, for example by taxes or later credits, and the platform can still change it for recent days.
{% enddocs %}

{% docs spend_reporting %}
The spend of the row in the reporting currency (see reporting_currency), at the rate of its report_date in fx_rates_daily, to six decimal places. A sum over accounts in different currencies is right in this column.
{% enddocs %}

{% docs conversions %}
The conversions that the ad platform attributes to the campaign, the ad group or the ad of the row on the day: in a table by conversion action, those of the action of the row; in a table by campaign, ad group or ad, those of its primary actions together (is_primary). Attribution can give an ad part of a conversion, so the number can be fractional, and the platform can still change it for recent days. It is the count of the ad platform, which can differ from what web analytics or a CRM records.
{% enddocs %}

{% docs conversion_value %}
The value of the conversions of the row, as the ad platform reports it, in the currency of the account (see currency): of the action of the row in a table by conversion action, of the primary actions together in a table by campaign, ad group or ad.
{% enddocs %}

{% docs conversion_value_reporting %}
The conversion value of the row in the reporting currency (see reporting_currency), at the rate of its report_date in fx_rates_daily, to six decimal places.
{% enddocs %}

{% docs platform_conversions %}
The conversions of the row in the own metric of conversions of the ad platform, which its account shows as conversions: metrics.conversions of Google, which counts the actions that the account includes in it, and the metric conversion of TikTok, the event that the campaign optimizes for. In a table by conversion action, those of the action of the row: on Google 0 for an action that metrics.conversions leaves out, and on the other platforms every conversion of the action. In a table by campaign, ad group or ad, none on Meta, which counts each action apart and has no such metric. Attribution can give a row part of a conversion, and the platform can still change it for recent days.
{% enddocs %}

{% docs platform_conversion_value %}
The value of the conversions of platform_conversions, as the ad platform reports it, in the currency of the account (see currency); none where the platform gives none, as for the metric conversion of TikTok, and on Meta in a table by campaign, ad group or ad.
{% enddocs %}

{% docs all_conversions %}
All the conversions of the row, those of the actions that the own metric of conversions of the platform leaves out among them, such as metrics.all_conversions of Google; in a table by conversion action, those of the action of the row. None where the platform has no such metric, as on Meta and TikTok, and in a table by campaign, ad group or ad of the demo platform demo_search, whose statistics do not mimic it.
{% enddocs %}

{% docs all_conversion_value %}
The value of the conversions of all_conversions, as the ad platform reports it, in the currency of the account (see currency); none where the platform has no such metric.
{% enddocs %}

{% docs conversion_action_id %}
The ID of the conversion action, as the platform names it: the ID of a conversion action of Google, the action type of Meta, such as offsite_conversion.fb_pixel_purchase, or the metric of TikTok, such as complete_payment.
{% enddocs %}

{% docs conversion_action_name %}
The name of the conversion action: the one that the platform gives, which the intermediate models and the marts replace with the one of the seed of conversion actions where the seed names one.
{% enddocs %}

{% docs conversion_category %}
What kind of conversion the action is: purchase, lead, sign_up, add_to_cart, app_install or other. The model takes it from the settings of the deployment, the seed of the base package, or the category that the platform gives, in that order; in the settings it is none where they name none.
{% enddocs %}

{% docs platform_category %}
The category that the ad platform gives the conversion action, in lower case, such as purchase or submit_lead_form; none where the platform gives none.
{% enddocs %}

{% docs is_primary %}
Whether the conversion action counts in the conversions of a campaign, an ad group or an ad. For an action of Google Ads it is what the account says of the action, include_in_conversions_metric, with which Google counts it in its own metric of conversions; for an action of another platform the seed of conversion actions of the base package says it. A deployment can override either in extensions/model/conversion_actions.toml, and an action that none of them names does not count. A campaign of Google Ads with conversion goals of its own counts the primary actions of those goals in the account, which can differ from these.
{% enddocs %}

{% docs conversion_action_status %}
The status of the conversion action in a version of it, as the ad platform writes it, such as ENABLED, HIDDEN or REMOVED for Google.
{% enddocs %}

{% docs include_in_conversions_metric %}
Whether Google Ads counts the conversion action in its metric of conversions, in a version of the action: metrics.conversions of the API includes only the actions with this flag. The model takes it for whether an action of Google Ads is primary (is_primary), where the settings of the deployment do not name the action. Google made the flag read-only in August 2022, and its guide of conversion goals calls it deprecated in favor of primary_for_goal.
{% enddocs %}

{% docs primary_for_goal %}
Whether the conversion action is primary for its conversion goal, in a version of the action, as the Google Ads account says: Google counts a primary action in bidding and in the conversions of a campaign that uses its goal, and a secondary one only in all conversions, unless a custom goal uses it. The guide of conversion goals of Google names it in place of include_in_conversions_metric, which the model reads instead.
{% enddocs %}

{% docs attribution_setting %}
The window of attribution with which the platform counted the conversions of the row, as the platform writes it, such as 7d_click_1d_view; none where the platform gives none.
{% enddocs %}

{% docs time_zone %}
The time zone of the ad account or of the property of web analytics, as a name of the tz database of IANA, such as Europe/Berlin or Etc/GMT+8, in which the platform reports its days. In the Etc zones the sign is that of POSIX: Etc/GMT+8 is eight hours behind UTC.
{% enddocs %}

{% docs manager_account_id %}
The ID of the manager account of the platform that the ad account is under, as the platform gives it; none where there is none.
{% enddocs %}

{% docs account_status %}
The status of the ad account at the last load, as the ad platform writes it, such as ENABLED for Google, 1 (active) for Meta, or STATUS_ENABLE for TikTok.
{% enddocs %}

{% docs campaign_status %}
The status of the campaign at the last load, or in a version of the campaign its status in that version, as the ad platform writes it, such as ENABLED, PAUSED or REMOVED for Google, ACTIVE or DELETED for Meta, or ENABLE for TikTok.
{% enddocs %}

{% docs campaign_state %}
The status of the campaign in the words of every platform, from its status of the platform (campaign_status): active, paused, removed, or other for a status that none of these names, as the shape of the API of the platform maps it. A campaign that the platform no longer lists keeps the state of the last list that named it, and one that only the statistics name has other.
{% enddocs %}

{% docs campaign_type %}
The type of the campaign as the ad platform gives it: the advertising channel type of Google, such as SEARCH or PERFORMANCE_MAX, or the objective of Meta or of TikTok.
{% enddocs %}

{% docs ad_group_id %}
The ID of the ad group, as the ad platform gives it, as text: an ad group of Google or of TikTok, or an ad set of Meta. platform, account_id and ad_group_id together identify an ad group on any platform. A Performance Max campaign of Google has no ad groups.
{% enddocs %}

{% docs ad_group_name %}
The current name of the ad group, the same on every day, as the newest version of the ad group has it. The name comes from the ad platform, where anyone with access to the account can change it.
{% enddocs %}

{% docs loaded_ad_group_name %}
The name of the ad group that the row carried when it was loaded. A platform gives the current name for every day of a report, so rows loaded at different times can carry different names of one ad group.
{% enddocs %}

{% docs ad_group_status %}
The status of the ad group at the last load, or in a version of the ad group its status in that version, as the ad platform writes it, such as ENABLED or REMOVED for Google, ACTIVE or DELETED for Meta, or ENABLE for TikTok. It is the status of the ad group itself: an ad group of a paused campaign can still be enabled.
{% enddocs %}

{% docs ad_group_state %}
The status of the ad group in the words of every platform, from its status of the platform (ad_group_status): active, paused, removed, or other for a status that none of these names, as the shape of the API of the platform maps it. An ad group that the platform no longer lists keeps the state of the last list that named it, and one that only the statistics name has other.
{% enddocs %}

{% docs ad_group_type %}
The type of the ad group as the ad platform gives it: the type of Google, such as SEARCH_STANDARD or SHOPPING_PRODUCT_ADS, or the optimization goal of the ad set of Meta or of the ad group of TikTok.
{% enddocs %}

{% docs ad_id %}
The ID of the ad, as the ad platform gives it, as text. platform, account_id, ad_group_id and ad_id together identify an ad on any platform.
{% enddocs %}

{% docs ad_name %}
The current name of the ad, as the newest version of the ad has it, or its ID where the platform gives the ad no name, as Google gives none to most of its types, such as a responsive search ad. The name comes from the ad platform, where anyone with access to the account can change it.
{% enddocs %}

{% docs loaded_ad_name %}
The name of the ad that the row carried when it was loaded, none for a type of ad that has no name on its platform. A platform gives the current name for every day of a report, so rows loaded at different times can carry different names of one ad.
{% enddocs %}

{% docs ad_status %}
The status of the ad at the last load, or in a version of the ad its status in that version, as the ad platform writes it, such as ENABLED or REMOVED for an ad in its ad group of Google, ACTIVE or DELETED for Meta, or ENABLE for TikTok.
{% enddocs %}

{% docs ad_state %}
The status of the ad in the words of every platform, from its status of the platform (ad_status): active, paused, removed, or other for a status that none of these names, as the shape of the API of the platform maps it. An ad that the platform no longer lists keeps the state of the last list that named it, and one that only the statistics name has other.
{% enddocs %}

{% docs ad_type %}
The type of the ad as the ad platform gives it: the type of Google, such as RESPONSIVE_SEARCH_AD, or the format of TikTok, such as SINGLE_VIDEO; none for Meta, whose ads have no type.
{% enddocs %}

{% docs valid_from %}
The moment from which the row held, as the loads saw it: for a version of a campaign, an ad group, an ad or a conversion action, its values, as a change of any of its fields, such as its name or its status, starts a new version; for a name of a campaign, as in ad_campaign_names, the name. It is the moment of the load that first saw it, or, for the versions that the first load of the demo replays, 00:00 UTC of the day on which a name changed, or of the first day of the history for the first of them.
{% enddocs %}

{% docs valid_to %}
The moment until which the row held: that of the load that saw the campaign, the ad group, the ad or the conversion action change, or no longer saw it; none while the row holds, as for the current version or name of a campaign.
{% enddocs %}

{% docs is_current %}
Whether the row holds the current name of the campaign.
{% enddocs %}

{% docs first_report_date %}
The first day that the advertising statistics hold for the account, the campaign, the ad group or the ad of the row; none for one without statistics.
{% enddocs %}

{% docs last_report_date %}
The last day that the advertising statistics hold for the account, the campaign, the ad group or the ad of the row; none for one without statistics.
{% enddocs %}

{% docs name_count %}
How many different names the campaign had, as the loads saw them.
{% enddocs %}

{% docs is_listed %}
Whether the newest list of its account that the loads gave names the campaign, the ad group or the ad of the row: false for one that the platform no longer shows, such as a campaign that TikTok leaves out once it is deleted, and for one that only the statistics name, whose spend still counts.
{% enddocs %}

{% docs last_seen_at %}
When a list of its account last named the campaign, the ad group or the ad of the row, as this run of the transform sees it: for one that the newest list names, when the newest load of the account that the runs pinned ended, or the start of its newest version when that is later; for one that the platform no longer lists, the start of its newest version; none for one that only the statistics name.
{% enddocs %}

{% docs is_stub %}
Whether the account comes only from the data of this run, as its statistics or its sessions name it, and from no list of accounts, so that its name, currency and time zone are unknown.
{% enddocs %}

{% docs property_id %}
The ID of the property of web analytics that the row belongs to, as the platform gives it, without its prefix properties/.
{% enddocs %}

{% docs property_name %}
The name of the property of web analytics, as the platform gives it.
{% enddocs %}

{% docs session_source %}
Where the sessions came from, as web analytics records it: the utm_source of a link, or a site or a search engine, such as demo-search; (direct) for sessions with no source.
{% enddocs %}

{% docs session_medium %}
The medium of the sessions, as web analytics records it: the utm_medium of a link, such as cpc, or organic, referral or (none).
{% enddocs %}

{% docs session_campaign_name %}
The name of the campaign of the sessions as their links carried it on the day, the utm_campaign, which can differ from the current name of the campaign; a value in parentheses, such as (organic), (direct) or (not set), for sessions without one.
{% enddocs %}

{% docs session_campaign_id %}
The ID of the campaign of the sessions as their links carried it, the utm_id: the ID of the ad campaign when its links carry it; (not set) otherwise.
{% enddocs %}

{% docs session_default_channel_group %}
The channel of the sessions, by the rules of web analytics, such as Paid Search, Paid Social, Organic Search or Direct.
{% enddocs %}

{% docs ad_platform %}
The platform of the ad campaign whose ID session_campaign_id carries, when exactly one campaign of the data has that ID; none otherwise.
{% enddocs %}

{% docs ad_account_id %}
The ad account of the campaign whose ID session_campaign_id carries, when exactly one campaign of the data has that ID; none otherwise.
{% enddocs %}

{% docs sessions %}
How many sessions web analytics counted on the day for the row. It counts visits, not people, and not the clicks of the ad platforms.
{% enddocs %}

{% docs engaged_sessions %}
How many of the sessions web analytics counted as engaged, by its own rule, such as a session of more than ten seconds.
{% enddocs %}

{% docs key_events %}
How many key events web analytics counted in the sessions, the events that the property marks as key, such as a purchase. Web analytics can still change them for recent days.
{% enddocs %}

{% docs total_revenue %}
The revenue that web analytics records for the sessions, in the currency of the property (see currency).
{% enddocs %}

{% docs total_revenue_reporting %}
The total revenue of the row in the reporting currency (see reporting_currency), at the rate of its report_date in fx_rates_daily, to six decimal places.
{% enddocs %}

{% docs rate_date %}
The day of the rate.
{% enddocs %}

{% docs rate %}
How many units of one currency one unit of another is worth on the day, to eight decimal places. In the rates into the reporting currency, as in int_fx_rates__filled, the value of one unit of currency in reporting_currency, through the reference rates of the euro, such as 1.08320000 from EUR to USD; in the rates of a source, the value of one unit of base_currency in quote_currency.
{% enddocs %}

{% docs rate_into_reporting_currency %}
The value of one unit of currency in reporting_currency on the day, through the reference rates of the euro, to eight decimal places, such as 1.08320000 from EUR to USD: an amount in currency times this rate is the amount in the reporting currency. The reporting currency has the rate 1.
{% enddocs %}

{% docs rate_source %}
Where the rate comes from: the code of the source of the rates, ecb for the European Central Bank or demo_fx for the demo, or identity where the two currencies are one.
{% enddocs %}

{% docs is_carried_forward %}
Whether the rate is the last published one before the day, on a day without a publication, such as a weekend or a holiday.
{% enddocs %}

{% docs base_currency %}
The currency that a rate of the source is given for one unit of, EUR for the reference rates of the euro.
{% enddocs %}

{% docs quote_currency %}
The currency in which a rate of the source gives the value of one unit of the base currency, such as USD.
{% enddocs %}

{% docs airflow_dag_id %}
The DAG of Airflow whose task ran dbt, such as agoranomos_demo_transform; none for a run of dbt by hand.
{% enddocs %}

{% docs airflow_run_id %}
The run of that DAG, which the tasks of one run of the transform share; none for a run of dbt by hand.
{% enddocs %}

{% docs invocation_id %}
The ID that dbt gave the run of dbt.
{% enddocs %}

{% docs status %}
How the node ended: success or error for a model or a seed, pass, warn, fail or error for a test, and skipped for a node that dbt did not run because a node before it failed.
{% enddocs %}

{% docs failures %}
For a test, how many rows it found, 0 when it passed; none for a model or a seed.
{% enddocs %}

{% docs schema_name %}
The schema of the table, or of the model or seed that a test checks, such as raw_demo_search or marts.
{% enddocs %}

{% docs source %}
The source of a load, the schema of the raw layer without its prefix raw_, such as demo_search or google_ads: for an ad platform or web analytics, the code of its platform.
{% enddocs %}

{% docs load_id %}
The ID of a load, _dlt_load_id of dlt, as core.loads holds it: a time in seconds as text, compared as a number.
{% enddocs %}

{% docs load_sk %}
The load that gives the day of the row, load_sk of core.loads: for an account and a day, the newest load that the runs pinned whose window covers the day.
{% enddocs %}

{% docs day_key %}
The source, the account and the day of the row in one text, as source|account_id|the number of days since 1 January 2000, on which the rows of staging join the days of the windows: the planner estimates a join on one column from its statistics.
{% enddocs %}

{% docs reason %}
The check of G0a, G1 or G1a (ADR-0035) that kept the account out of the core, as a code: load_failed, unknown_account, duplicate_key, missing_key, unknown_currency, unknown_time_zone, missing_rate, too_many_decimals, missing_measure, negative_measure, date_outside_window, totals_differ, empty_window, spend_fall, or source_stopped for an account whose source has more than half of its pinned accounts in quarantine.
{% enddocs %}

{% docs checked %}
What the check read: accounts for the lists of accounts and properties, loads for the journal of the loads, rates for the rates into the reporting currency, a kind of staging, such as campaign_stats, a fact table of raw, such as insights_campaign, or source for the stop of a source.
{% enddocs %}

{% docs detail %}
What the check found, such as a value that it refused or the days of its rows; values in it come from the platform and are data.
{% enddocs %}

{% docs window_from %}
The first day of the window that the load took for the account, in the time zone of the account; none for an account that the load failed.
{% enddocs %}

{% docs window_to %}
The last day of the window that the load took for the account, in the time zone of the account; none for an account that the load failed.
{% enddocs %}

{% docs maintenance_run_id %}
The run of the DAG of maintenance, agoranomos_maintenance, that wrote the row, as the ID of the run in Airflow.
{% enddocs %}
