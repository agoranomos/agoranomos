{#- core.dim_ad_versions, the versions of the ads (ADR-0036): a snapshot of dbt over the view
    int_ads__current, as dim_campaign_versions is over that of the campaigns. An ad is defined only
    inside its ad group, which is part of its key. -#}
{% snapshot dim_ad_versions %}
select
    account_sk::integer as account_sk,
    ad_group_id::bigint as ad_group_id,
    ad_id::bigint as ad_id,
    campaign_id::bigint as campaign_id,
    current_name::text as ad_name,
    ad_status::text as ad_status,
    ad_state::text as ad_state,
    ad_type::text as ad_type
from {{ ref('int_ads__current') }}
{% endsnapshot %}
