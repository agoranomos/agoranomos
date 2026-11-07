{#- core.dim_ad_group_versions, the versions of the ad groups (ADR-0036): a snapshot of dbt over the
    view int_ad_groups__current, as dim_campaign_versions is over that of the campaigns. No
    connector gives the dates or the budget of an ad group yet, so they are none. -#}
{% snapshot dim_ad_group_versions %}
select
    account_sk::integer as account_sk,
    ad_group_id::bigint as ad_group_id,
    campaign_id::bigint as campaign_id,
    current_name::text as ad_group_name,
    ad_group_status::text as ad_group_status,
    ad_group_state::text as ad_group_state,
    ad_group_type::text as ad_group_type,
    cast(null as date) as start_date,
    cast(null as date) as end_date,
    cast(null as numeric(18,6)) as budget,
    cast(null as text) as budget_period
from {{ ref('int_ad_groups__current') }}
{% endsnapshot %}
