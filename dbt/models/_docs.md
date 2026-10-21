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
A name that the campaign had, from valid_from until valid_to, as the loads saw it.
{% enddocs %}

{% docs currency %}
A currency, as a three-letter code of ISO 4217, such as USD. In a row of statistics or sessions, the currency of its ad account or property, in whose units its amounts of money are: amounts in rows with different codes are in different units, and the columns that end in _reporting add them up in one. In the rates into the reporting currency, as in fx_rates_daily, the currency that a rate converts from.
{% enddocs %}

{% docs reporting_currency %}
The currency of the columns that end in _reporting, as a three-letter code of ISO 4217: the reporting currency of the deployment, which the setting AGORANOMOS_REPORTING_CURRENCY sets, the same in every row.
{% enddocs %}

{% docs impressions %}
How many times the ads of the campaign were shown on the day, as the ad platform counts them. It counts showings, not people: one person can see an ad many times, so impressions are not reach.
{% enddocs %}

{% docs clicks %}
How many times the ads of the campaign were clicked on the day with a click that leads to the site, as the ad platform counts them: the clicks of Google, the clicks (destination) of TikTok, and the link clicks of Meta, whose own clicks count every click. It counts clicks, not people, and not the visits that web analytics records.
{% enddocs %}

{% docs spend %}
What the ads of the campaign cost on the day, as the ad platform reports it in its statistics, in the currency of the account (see currency), exact to six decimal places. Also called cost or ad spend. It is not the amount that the platform invoices, which can differ from it, for example by taxes or later credits, and the platform can still change it for recent days.
{% enddocs %}

{% docs spend_reporting %}
The spend of the row in the reporting currency (see reporting_currency), at the rate of its report_date in fx_rates_daily, to six decimal places. A sum over accounts in different currencies is right in this column.
{% enddocs %}

{% docs conversions %}
The conversions that the ad platform attributes to the campaign on the day: in a table by conversion action, those of the action of the row; in a table by campaign, those of its primary actions together (is_primary). Attribution can give a campaign part of a conversion, so the number can be fractional, and the platform can still change it for recent days. It is the count of the ad platform, which can differ from what web analytics or a CRM records.
{% enddocs %}

{% docs conversion_value %}
The value of the conversions of the row, as the ad platform reports it, in the currency of the account (see currency): of the action of the row in a table by conversion action, of the primary actions together in a table by campaign.
{% enddocs %}

{% docs conversion_value_reporting %}
The conversion value of the row in the reporting currency (see reporting_currency), at the rate of its report_date in fx_rates_daily, to six decimal places.
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
Whether the conversion action counts in the conversions of a campaign. The seed of conversion actions of the base package says it, and a deployment can override it in extensions/model/conversion_actions.toml; an action that neither names does not count.
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

{% docs campaign_type %}
The type of the campaign as the ad platform gives it: the advertising channel type of Google, such as SEARCH or PERFORMANCE_MAX, or the objective of Meta or of TikTok.
{% enddocs %}

{% docs valid_from %}
The moment from which the row held, as the loads saw it: for a version of a campaign, its values, as a change of any of its fields, such as its name or its status, starts a new version; for a name of a campaign, as in ad_campaign_names, the name. It is the moment of the load that first saw it, or, for the versions that the first load of the demo replays, 00:00 UTC of the day on which a name changed, or of the first day of the history for the first of them.
{% enddocs %}

{% docs valid_to %}
The moment until which the row held: that of the load that saw the campaign change, or no longer saw it; none while the row holds, as for the current version or name of a campaign.
{% enddocs %}

{% docs is_current %}
Whether the row holds the current name of the campaign.
{% enddocs %}

{% docs first_report_date %}
The first day that the advertising statistics hold for the account or the campaign of the row; none for one without statistics.
{% enddocs %}

{% docs last_report_date %}
The last day that the advertising statistics hold for the account or the campaign of the row; none for one without statistics.
{% enddocs %}

{% docs name_count %}
How many different names the campaign had, as the loads saw them.
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
Where the rate comes from: the code of the source of the rates, such as demo_fx, or identity where the two currencies are one.
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
