{#- The shape of the TikTok API for Business v1.3: the integrated report of type BASIC by
    campaign_id, adgroup_id or ad_id and stat_time_day, its dimensions and metrics as nested
    objects, every metric a string, "-" for one that means nothing; the IDs of the levels above
    are metrics. The macros serve the demo platform demo_video and the connector of TikTok,
    tiktok_ads, which calls v1.3 for every request (ADR-0053). Each takes the relation of a raw
    table and the code of its platform. The IDs of the campaigns, ad groups and ads go through
    to_id, as TikTok writes them as strings and the core keeps them as bigint (ADR-0036): one that
    does not fit is NULL, never cut, and G1 holds its advertiser in quarantine with invalid_id. -#}

{#- The campaign of a row of a report at a level, campaign, ad_group or ad, and the IDs below it,
    with the name of the level that the row carried when it was loaded. -#}
{% macro tiktok_level(level, names=true) %}
    {%- if level == 'campaign' %}
    {{ to_id('dimensions__campaign_id') }} as campaign_id,
    {%- if names %}
    {{ to_name('metrics__campaign_name') }} as loaded_campaign_name,
    {%- endif %}
    {%- elif level == 'ad_group' %}
    {{ to_id('metrics__campaign_id') }} as campaign_id,
    {{ to_id('dimensions__adgroup_id') }} as ad_group_id,
    {%- if names %}
    {{ to_name('metrics__adgroup_name') }} as loaded_ad_group_name,
    {%- endif %}
    {%- elif level == 'ad' %}
    {{ to_id('metrics__campaign_id') }} as campaign_id,
    {{ to_id('metrics__adgroup_id') }} as ad_group_id,
    {{ to_id('dimensions__ad_id') }} as ad_id,
    {%- if names %}
    {{ to_name('metrics__ad_name') }} as loaded_ad_name,
    {%- endif %}
    {%- else %}
    {{ exceptions.raise_compiler_error('a level is campaign, ad_group or ad, not ' ~ level) }}
    {%- endif %}
{%- endmacro %}

{#- The statistics of a level by day. Their conversions are those of the optimization event, so
    the campaigns pass conversions, the staging model of their conversions, and a campaign day
    whose measures are zero stays when it has complete payments or their value (nonzero). -#}
{% macro tiktok_stats(relation, platform, level='campaign', conversions=none) %}
{%- call nonzero(['impressions', 'clicks', 'spend', 'platform_conversions', 'platform_conversion_value'], conversions) %}
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

{#- The totals of an advertiser by day, from the integrated report at the level
    AUCTION_ADVERTISER, which returns the data of all the ads of the advertiser: the platform side
    of G0a (ADR-0035, ADR-0053), for the connector of TikTok, which reads them in a request of
    their own; the demo has none. Clicks are the clicks (destination), as tiktok_stats takes them.
    load_id is the load of the row, as core.platform_totals_daily keeps it. -#}
{% macro tiktok_totals(relation, platform) %}
select
    cast(left(cast(dimensions__stat_time_day as text), 10) as date) as report_date,
    '{{ platform }}' as platform,
    advertiser_id as account_id,
    {{ to_count('metrics__impressions') }} as total_impressions,
    {{ to_count('metrics__clicks') }} as total_clicks,
    {{ to_number('metrics__spend') }} as total_spend,
    _dlt_load_id as load_id
from {{ relation }}
{% endmacro %}

{#- The values of secondary_status with which TikTok shows a deleted campaign, ad group or ad,
    or one of a deleted campaign or ad group, named one by one, as the values *_STATUS_NOT_DELETE
    of the filters end in _DELETE too (ADR-0053, "The common state"; the enumerations of the
    lists in the official SDK of the API for Business, as of November 2026). -#}
{% macro tiktok_deleted() -%}
    ('CAMPAIGN_STATUS_DELETE', 'ADGROUP_STATUS_DELETE', 'ADGROUP_STATUS_CAMPAIGN_DELETE',
    'AD_STATUS_DELETE', 'AD_STATUS_CAMPAIGN_DELETE', 'AD_STATUS_ADGROUP_DELETE')
{%- endmacro %}

{#- The common state of a campaign, an ad group or an ad of TikTok (ADR-0036, ADR-0053): removed
    when its secondary_status is a value of a deletion (tiktok_deleted), as TikTok sends no
    DELETE in operation_status; otherwise from its operation_status, ENABLE active and DISABLE
    paused; anything else, FROZEN among them, or none, other. An ad group or an ad of a deleted
    campaign is removed too, and one of a paused campaign keeps its own state. -#}
{% macro tiktok_state(secondary_status, operation_status) -%}
    case
        when {{ secondary_status }} in {{ tiktok_deleted() }} then 'removed'
        when {{ operation_status }} = 'ENABLE' then 'active'
        when {{ operation_status }} = 'DISABLE' then 'paused'
        else 'other'
    end
{%- endmacro %}

{#- The status of a campaign, an ad group or an ad of TikTok as the core keeps it beside its state:
    the secondary_status of a deletion, which says what was deleted, or else its operation_status,
    ENABLE, DISABLE or FROZEN. -#}
{% macro tiktok_status(secondary_status, operation_status) -%}
    case
        when {{ secondary_status }} in {{ tiktok_deleted() }} then {{ secondary_status }}
        else {{ operation_status }}
    end
{%- endmacro %}

{#- The campaigns of the newest list of each account (newest_list), with the load of the list. A
    row with an ID that does not fit bigint is left out, as the account goes to quarantine with
    invalid_id (to_id, quarantine_raw_ids), so that the tests of the list hold; the same goes for
    the ad groups and the ads. -#}
{% macro tiktok_campaigns(relation, platform) %}
select
    '{{ platform }}' as platform,
    advertiser_id as account_id,
    {{ to_id('campaign_id') }} as campaign_id,
    {{ to_name('campaign_name') }} as observed_name,
    {{ tiktok_status('secondary_status', 'operation_status') }} as campaign_status,
    {{ tiktok_state('secondary_status', 'operation_status') }} as campaign_state,
    objective_type as campaign_type,
    _dlt_load_id as load_id
from {{ relation }}
where {{ id_fits('campaign_id') }}
{% endmacro %}

{#- The ad groups of the newest list of each account (newest_list), with the load of the list.
    Their type is their optimization goal. -#}
{% macro tiktok_ad_groups(relation, platform) %}
select
    '{{ platform }}' as platform,
    advertiser_id as account_id,
    {{ to_id('campaign_id') }} as campaign_id,
    {{ to_id('adgroup_id') }} as ad_group_id,
    {{ to_name('adgroup_name') }} as observed_name,
    {{ tiktok_status('secondary_status', 'operation_status') }} as ad_group_status,
    {{ tiktok_state('secondary_status', 'operation_status') }} as ad_group_state,
    optimization_goal as ad_group_type,
    _dlt_load_id as load_id
from {{ relation }}
where {{ id_fits('campaign_id') }} and {{ id_fits('adgroup_id') }}
{% endmacro %}

{#- The ads of the newest list of each account (newest_list), with the load of the list. Their
    type is their format. -#}
{% macro tiktok_ads(relation, platform) %}
select
    '{{ platform }}' as platform,
    advertiser_id as account_id,
    {{ to_id('campaign_id') }} as campaign_id,
    {{ to_id('adgroup_id') }} as ad_group_id,
    {{ to_id('ad_id') }} as ad_id,
    {{ to_name('ad_name') }} as observed_name,
    {{ tiktok_status('secondary_status', 'operation_status') }} as ad_status,
    {{ tiktok_state('secondary_status', 'operation_status') }} as ad_state,
    ad_format as ad_type,
    _dlt_load_id as load_id
from {{ relation }}
where {{ id_fits('campaign_id') }} and {{ id_fits('adgroup_id') }} and {{ id_fits('ad_id') }}
{% endmacro %}
