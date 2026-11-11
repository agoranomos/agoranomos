-- The names that each campaign had, with the time from which and until which it had each, a view
-- over the versions of the core (ADR-0036, ADR-0037): core.dim_campaign_versions changes with the
-- status and the type as well, so consecutive versions of one name are one period here. A version
-- starts when a run of the transform saw it, in UTC, and the one that holds has no end. A campaign
-- has few versions, so the periods are grouped here, while a view of a fact stays a plain join.
with marked as (
    select
        v.account_sk,
        v.campaign_id,
        v.campaign_name,
        v.dbt_valid_from,
        nullif(v.dbt_valid_to, cast('9999-12-31 00:00:00' as timestamp)) as dbt_valid_to,
        case
            when lag(v.campaign_name) over (
                partition by v.account_sk, v.campaign_id order by v.dbt_valid_from
            ) is distinct from v.campaign_name then 1
            else 0
        end as starts_a_name
    from {{ ref('dim_campaign_versions') }} as v
),

periods as (
    select
        *,
        sum(starts_a_name) over (
            partition by account_sk, campaign_id order by dbt_valid_from
        ) as period
    from marked
),

collapsed as (
    select
        account_sk,
        campaign_id,
        campaign_name,
        min(dbt_valid_from) as valid_from,
        case when bool_or(dbt_valid_to is null) then null else max(dbt_valid_to) end as valid_to
    from periods
    group by account_sk, campaign_id, campaign_name, period
)

select
    a.platform,
    a.account_id,
    coalesce(m.campaign_id_text, cast(n.campaign_id as text)) as campaign_id,
    n.campaign_name as observed_name,
    n.valid_from at time zone 'UTC' as valid_from,
    n.valid_to at time zone 'UTC' as valid_to,
    n.valid_to is null as is_current
from collapsed as n
left join {{ ref('dim_account') }} as a
    on a.account_sk = n.account_sk
left join {{ ref('dim_campaign') }} as m
    on m.account_sk = n.account_sk and m.campaign_id = n.campaign_id
