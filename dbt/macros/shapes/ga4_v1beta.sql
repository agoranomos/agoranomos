{#- The shape of the GA4 Data API v1beta, a row of runReport as a dictionary of its dimensions and
    metrics, which the connector adds the property to, and the properties of the GA4 Admin API. -#}

{% macro ga4_properties(relation, platform) %}
select
    '{{ platform }}' as platform,
    split_part(name, '/', 2) as property_id,
    display_name as property_name,
    time_zone,
    currency_code as currency
from {{ relation }}
{% endmacro %}

{% macro ga4_sessions_daily(relation, platform) %}
select
    to_date(cast("date" as text), 'YYYYMMDD') as report_date,
    '{{ platform }}' as platform,
    split_part(property, '/', 2) as property_id,
    session_source,
    session_medium,
    session_campaign_name,
    session_campaign_id,
    session_default_channel_group,
    {{ to_count('sessions') }} as sessions,
    {{ to_count('engaged_sessions') }} as engaged_sessions,
    {{ to_number('key_events') }} as key_events,
    {{ to_number('total_revenue') }} as total_revenue
from {{ relation }}
{% endmacro %}
