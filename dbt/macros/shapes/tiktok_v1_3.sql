{#- The shape of the TikTok API for Business v1.3: the integrated report of type BASIC by
    campaign_id, adgroup_id or ad_id and stat_time_day, its dimensions and metrics as nested
    objects, every metric a string, "-" for one that means nothing; the IDs of the levels above
    are metrics. -#}

{#- The campaign of a row of a report at a level, campaign, ad_group or ad, and the IDs below it,
    with the name of the level that the row carried when it was loaded. -#}
{% macro tiktok_level(level, names=true) %}
    {%- if level == 'campaign' %}
    dimensions__campaign_id as campaign_id,
    {%- if names %}
    {{ to_name('metrics__campaign_name') }} as loaded_campaign_name,
    {%- endif %}
    {%- elif level == 'ad_group' %}
    metrics__campaign_id as campaign_id,
    dimensions__adgroup_id as ad_group_id,
    {%- if names %}
    {{ to_name('metrics__adgroup_name') }} as loaded_ad_group_name,
    {%- endif %}
    {%- elif level == 'ad' %}
    metrics__campaign_id as campaign_id,
    metrics__adgroup_id as ad_group_id,
    dimensions__ad_id as ad_id,
    {%- if names %}
    {{ to_name('metrics__ad_name') }} as loaded_ad_name,
    {%- endif %}
    {%- else %}
    {{ exceptions.raise_compiler_error('a level is campaign, ad_group or ad, not ' ~ level) }}
    {%- endif %}
{%- endmacro %}

{% macro tiktok_stats(relation, platform, level='campaign') %}
{%- call nonzero(['impressions', 'clicks', 'spend', 'platform_conversions', 'platform_conversion_value']) %}
select
    cast(left(cast(dimensions__stat_time_day as text), 10) as date) as report_date,
    '{{ platform }}' as platform,
    advertiser_id as account_id,
    {{- tiktok_level(level) }}
    {{ to_count('metrics__impressions') }} as impressions,
    {#- Clicks (destination), to the site. -#}
    {{ to_count('metrics__clicks') }} as clicks,
    {{ to_number('metrics__spend') }} as spend,
    {#- The conversions of the optimization event, the metric conversion of TikTok, which has no
        value of its own. #}
    {{ to_number('metrics__conversion') }} as platform_conversions,
    cast(null as numeric(18,6)) as platform_conversion_value,
    {#- TikTok has no count of all its conversions. #}
    cast(null as numeric(18,6)) as all_conversions,
    cast(null as numeric(18,6)) as all_conversion_value
from {{ relation }}
{%- endcall %}
{% endmacro %}

{#- Two actions: complete payments, with their value in total_complete_payment_rate, which despite
    its name is the value of the purchases, and the event that the campaign optimizes for. A row
    with a value and no whole payment keeps its value. -#}
{% macro tiktok_conversions(relation, platform, level='campaign') %}
with actions as (
    select
        cast(left(cast(dimensions__stat_time_day as text), 10) as date) as report_date,
        advertiser_id as account_id,
        {{- tiktok_level(level, names=false) }}
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
        {{- tiktok_level(level, names=false) }}
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
    {%- if level != 'campaign' %}
    ad_group_id,
    {%- endif %}
    {%- if level == 'ad' %}
    ad_id,
    {%- endif %}
    conversion_action_id,
    conversion_action_name,
    platform_category,
    cast(null as text) as attribution_setting,
    conversions,
    coalesce(conversion_value, 0) as conversion_value,
    conversions as platform_conversions,
    coalesce(conversion_value, 0) as platform_conversion_value,
    cast(null as numeric(18,6)) as all_conversions,
    cast(null as numeric(18,6)) as all_conversion_value
from actions
where conversions > 0 or conversion_value > 0
{% endmacro %}

{% macro tiktok_accounts(relation, platform) %}
select
    '{{ platform }}' as platform,
    advertiser_id as account_id,
    {{ to_name('name') }} as account_name,
    currency,
    timezone as time_zone,
    cast(null as text) as manager_account_id,
    status as account_status
from {{ relation }}
{% endmacro %}

{#- The common state of a campaign, an ad group or an ad of TikTok, from its operation_status
    (ADR-0036): ENABLE is active, DISABLE paused and DELETE removed, the values with which the API
    sets the status of each level (StatusOptType of the official SDK of the API for Business, as of
    November 2026); any other value is other. -#}
{% macro tiktok_state(status) -%}
    case {{ status }}
        when 'ENABLE' then 'active'
        when 'DISABLE' then 'paused'
        when 'DELETE' then 'removed'
        else 'other'
    end
{%- endmacro %}

{#- The campaigns of the newest list of each account (newest_list), with the load of the list. -#}
{% macro tiktok_campaigns(relation, platform) %}
select
    '{{ platform }}' as platform,
    advertiser_id as account_id,
    campaign_id,
    {{ to_name('campaign_name') }} as observed_name,
    operation_status as campaign_status,
    {{ tiktok_state('operation_status') }} as campaign_state,
    objective_type as campaign_type,
    _dlt_load_id as load_id
from {{ relation }}
{% endmacro %}

{#- The ad groups of the newest list of each account (newest_list), with the load of the list.
    Their type is their optimization goal. -#}
{% macro tiktok_ad_groups(relation, platform) %}
select
    '{{ platform }}' as platform,
    advertiser_id as account_id,
    campaign_id,
    adgroup_id as ad_group_id,
    {{ to_name('adgroup_name') }} as observed_name,
    operation_status as ad_group_status,
    {{ tiktok_state('operation_status') }} as ad_group_state,
    optimization_goal as ad_group_type,
    _dlt_load_id as load_id
from {{ relation }}
{% endmacro %}

{#- The ads of the newest list of each account (newest_list), with the load of the list. Their
    type is their format. -#}
{% macro tiktok_ads(relation, platform) %}
select
    '{{ platform }}' as platform,
    advertiser_id as account_id,
    campaign_id,
    adgroup_id as ad_group_id,
    ad_id,
    {{ to_name('ad_name') }} as observed_name,
    operation_status as ad_status,
    {{ tiktok_state('operation_status') }} as ad_state,
    ad_format as ad_type,
    _dlt_load_id as load_id
from {{ relation }}
{% endmacro %}
