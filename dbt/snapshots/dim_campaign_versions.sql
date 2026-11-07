{#- core.dim_campaign_versions, the versions of the campaigns (ADR-0036): a snapshot of dbt over the
    view int_campaigns__current, with the strategy check on the columns of _snapshots.yml, and the
    shared settings of dbt_project.yml. It reads a view of staging and never a table of raw, which
    may not exist yet: dbt build creates the snapshot empty on a warehouse with no load, once the
    views under it exist, while dbt snapshot alone fails there (#187). No connector gives the dates
    or the budget of a campaign yet, so they are none, and a connector that gives them later changes
    no version but those whose values it brings. -#}
{% snapshot dim_campaign_versions %}
select
    account_sk::integer as account_sk,
    campaign_id::bigint as campaign_id,
    current_name::text as campaign_name,
    campaign_status::text as campaign_status,
    campaign_state::text as campaign_state,
    campaign_type::text as campaign_type,
    cast(null as date) as start_date,
    cast(null as date) as end_date,
    cast(null as numeric(18,6)) as budget,
    cast(null as text) as budget_period
from {{ ref('int_campaigns__current') }}
{% endsnapshot %}
