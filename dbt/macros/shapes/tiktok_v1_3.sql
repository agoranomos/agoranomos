{#- The shape of the TikTok API for Business v1.3: the integrated report of type BASIC by
    campaign_id and stat_time_day, its dimensions and metrics as nested objects, every metric a
    string, "-" for one that means nothing. -#}

{% macro tiktok_campaign_stats(relation, platform) %}
select
    cast(left(cast(dimensions__stat_time_day as text), 10) as date) as report_date,
    '{{ platform }}' as platform,
    advertiser_id as account_id,
    dimensions__campaign_id as campaign_id,
    metrics__campaign_name as loaded_campaign_name,
    {{ to_count('metrics__impressions') }} as impressions,
    {#- Clicks (destination), to the site. -#}
    {{ to_count('metrics__clicks') }} as clicks,
    {{ to_number('metrics__spend') }} as spend
from {{ relation }}
{% endmacro %}

{#- Two actions: complete payments, with their value in total_complete_payment_rate, which despite
    its name is the value of the purchases, and the event that the campaign optimizes for. A row
    with a value and no whole payment keeps its value. -#}
{% macro tiktok_campaign_conversions(relation, platform) %}
with actions as (
    select
        cast(left(cast(dimensions__stat_time_day as text), 10) as date) as report_date,
        advertiser_id as account_id,
        dimensions__campaign_id as campaign_id,
        'complete_payment' as conversion_action_id,
        'Complete payments' as conversion_action_name,
        'purchase' as platform_category,
        {{ to_number('metrics__complete_payment') }} as conversions,
        {{ to_number('metrics__total_complete_payment_rate') }} as conversion_value
    from {{ relation }}
    union all
    select
        cast(left(cast(dimensions__stat_time_day as text), 10) as date),
        advertiser_id,
        dimensions__campaign_id,
        'conversion',
        'Conversions (the optimization event)',
        cast(null as text),
        {{ to_number('metrics__conversion') }},
        cast(null as numeric(18,6))
    from {{ relation }}
)
select
    report_date,
    '{{ platform }}' as platform,
    account_id,
    campaign_id,
    conversion_action_id,
    conversion_action_name,
    platform_category,
    cast(null as text) as attribution_setting,
    conversions,
    coalesce(conversion_value, 0) as conversion_value
from actions
where conversions > 0 or conversion_value > 0
{% endmacro %}

{% macro tiktok_accounts(relation, platform) %}
select
    '{{ platform }}' as platform,
    advertiser_id as account_id,
    name as account_name,
    currency,
    timezone as time_zone,
    cast(null as text) as manager_account_id,
    status as account_status
from {{ relation }}
{% endmacro %}

{% macro tiktok_campaigns(relation, platform) %}
select
    '{{ platform }}' as platform,
    advertiser_id as account_id,
    campaign_id,
    campaign_name as observed_name,
    operation_status as campaign_status,
    objective_type as campaign_type,
    _dlt_valid_from as valid_from,
    _dlt_valid_to as valid_to
from {{ relation }}
{% endmacro %}
