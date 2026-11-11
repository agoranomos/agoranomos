-- The ad groups of every platform, a view over the core (ADR-0037): the rows of
-- core.dim_ad_group, with their campaign, their current name, status, state and type, whether the
-- platform still lists them, and the days that their statistics cover.
select
    a.platform,
    p.platform_name,
    a.client_id,
    c.client_name,
    a.account_id,
    a.account_name,
    coalesce(m.campaign_id_text, cast(g.campaign_id as text)) as campaign_id,
    m.campaign_name,
    g.ad_group_id_text as ad_group_id,
    g.ad_group_name,
    g.ad_group_status,
    g.ad_group_type,
    g.first_report_date,
    g.last_report_date,
    g.ad_group_state,
    g.is_listed
from {{ ref('dim_ad_group') }} as g
left join {{ ref('dim_account') }} as a
    on a.account_sk = g.account_sk
left join {{ ref('dim_client') }} as c
    on c.client_id = a.client_id
left join {{ platforms(ref('seed_ad_platforms')) }}
    on p.platform = a.platform
left join {{ ref('dim_campaign') }} as m
    on m.account_sk = g.account_sk and m.campaign_id = g.campaign_id
