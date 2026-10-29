{#- The shape of the Google Ads API v25, as dlt loads its responses (nested objects joined by
    __, names in snake_case): the macros that turn its raw tables into the columns of staging,
    for the demo platform demo_search now and for the connector of Google Ads later (ADR-0025).
    Each takes the relation of a raw table and the code of its platform. -#}

{% macro google_ads_campaign_stats(relation, platform) %}
select
    cast(segments__date as date) as report_date,
    '{{ platform }}' as platform,
    customer__id as account_id,
    campaign__id as campaign_id,
    campaign__name as loaded_campaign_name,
    {{ to_count('metrics__impressions') }} as impressions,
    {{ to_count('metrics__clicks') }} as clicks,
    {{ micros_to_number('metrics__cost_micros') }} as spend
from {{ relation }}
{% endmacro %}

{#- The report by conversion action, a query of its own: every conversion of the action, also of
    one that does not count in metrics.conversions; which actions count is the seed's to say. -#}
{% macro google_ads_campaign_conversions(relation, platform) %}
select
    cast(segments__date as date) as report_date,
    '{{ platform }}' as platform,
    customer__id as account_id,
    campaign__id as campaign_id,
    split_part(segments__conversion_action, '/', 4) as conversion_action_id,
    segments__conversion_action_name as conversion_action_name,
    lower(segments__conversion_action_category) as platform_category,
    cast(null as text) as attribution_setting,
    cast(metrics__all_conversions as numeric(18,6)) as conversions,
    cast(metrics__all_conversions_value as numeric(18,6)) as conversion_value
from {{ relation }}
{% endmacro %}

{#- The client accounts of a manager account; the manager itself has no statistics. -#}
{% macro google_ads_accounts(relation, platform) %}
select
    '{{ platform }}' as platform,
    customer_client__id as account_id,
    customer_client__descriptive_name as account_name,
    customer_client__currency_code as currency,
    customer_client__time_zone as time_zone,
    split_part(customer_client__resource_name, '/', 2) as manager_account_id,
    customer_client__status as account_status
from {{ relation }}
where not customer_client__manager
{% endmacro %}

{#- An account as its own customer resource gives it, for the connector of Google Ads, which reads
    each account that clients.toml names on its own (ADR-0032): the columns of google_ads_accounts,
    with no manager account. -#}
{% macro google_ads_customers(relation, platform) %}
select
    '{{ platform }}' as platform,
    customer__id as account_id,
    customer__descriptive_name as account_name,
    customer__currency_code as currency,
    customer__time_zone as time_zone,
    cast(null as text) as manager_account_id,
    customer__status as account_status
from {{ relation }}
where not customer__manager
{% endmacro %}

{#- The versions of the campaigns that dlt keeps (scd2). -#}
{% macro google_ads_campaigns(relation, platform) %}
select
    '{{ platform }}' as platform,
    customer__id as account_id,
    campaign__id as campaign_id,
    campaign__name as observed_name,
    campaign__status as campaign_status,
    campaign__advertising_channel_type as campaign_type,
    _dlt_valid_from as valid_from,
    _dlt_valid_to as valid_to
from {{ relation }}
{% endmacro %}
