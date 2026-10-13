{#- The shape of the Meta Marketing API v26.0, as dlt loads its insights at the level of
    campaigns: the lists actions and action_values become the child tables
    <insights>__actions and <insights>__action_values, joined to their row by _dlt_parent_id, with
    the windows of attribution as columns whose names start with an underscore, such as
    _7d_click (ADR-0025; the windows stay in raw). -#}

{% macro meta_campaign_stats(relation, platform) %}
select
    cast(date_start as date) as report_date,
    '{{ platform }}' as platform,
    account_id,
    campaign_id,
    campaign_name as loaded_campaign_name,
    {{ to_count('impressions') }} as impressions,
    {#- Clicks to the site: the clicks of Meta count every click (ADR-0025). -#}
    {{ to_count('inline_link_clicks') }} as clicks,
    {{ to_number('spend') }} as spend
from {{ relation }}
{% endmacro %}

{#- Each action of each row, with the count and the value of the default attribution of the
    account, which value holds. -#}
{% macro meta_campaign_conversions(insights, actions, action_values, platform) %}
with counted as (
    select _dlt_parent_id as parent_id, action_type, {{ to_number('value') }} as conversions
    from {{ actions }}
),
valued as (
    select _dlt_parent_id as parent_id, action_type, {{ to_number('value') }} as conversion_value
    from {{ action_values }}
),
joined as (
    select
        coalesce(c.parent_id, v.parent_id) as parent_id,
        coalesce(c.action_type, v.action_type) as action_type,
        coalesce(c.conversions, 0) as conversions,
        coalesce(v.conversion_value, 0) as conversion_value
    from counted as c
    full outer join valued as v on v.parent_id = c.parent_id and v.action_type = c.action_type
)
select
    cast(i.date_start as date) as report_date,
    '{{ platform }}' as platform,
    i.account_id,
    i.campaign_id,
    j.action_type as conversion_action_id,
    j.action_type as conversion_action_name,
    cast(null as text) as platform_category,
    i.attribution_setting,
    j.conversions,
    j.conversion_value
from joined as j
join {{ insights }} as i on i._dlt_id = j.parent_id
{% endmacro %}

{% macro meta_accounts(relation, platform) %}
select
    '{{ platform }}' as platform,
    account_id,
    name as account_name,
    currency,
    timezone_name as time_zone,
    cast(null as text) as manager_account_id,
    cast(account_status as text) as account_status
from {{ relation }}
{% endmacro %}

{% macro meta_campaigns(relation, platform) %}
select
    '{{ platform }}' as platform,
    account_id,
    id as campaign_id,
    name as observed_name,
    status as campaign_status,
    objective as campaign_type,
    _dlt_valid_from as valid_from,
    _dlt_valid_to as valid_to
from {{ relation }}
{% endmacro %}
