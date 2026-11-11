-- The campaigns of every platform, a view over the core (ADR-0037): the rows of core.dim_campaign,
-- with their current name, status, state and type, whether the platform still lists them, the days
-- that their statistics cover and how many names they had, which the dimension keeps.
select
    a.platform,
    p.platform_name,
    a.client_id,
    c.client_name,
    a.account_id,
    a.account_name,
    m.campaign_id_text as campaign_id,
    m.campaign_name,
    m.campaign_status,
    m.campaign_type,
    m.first_report_date,
    m.last_report_date,
    cast(m.name_count as bigint) as name_count,
    m.campaign_state,
    m.is_listed
from {{ ref('dim_campaign') }} as m
left join {{ ref('dim_account') }} as a
    on a.account_sk = m.account_sk
left join {{ ref('dim_client') }} as c
    on c.client_id = a.client_id
left join {{ platforms(ref('seed_ad_platforms')) }}
    on p.platform = a.platform
