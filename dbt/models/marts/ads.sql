-- The ads of every platform, a view over the core (ADR-0037): the rows of core.dim_ad, with their
-- campaign and ad group, their current name, status, state and type, whether the platform still
-- lists them, and the days that their statistics cover. An ad without a name on its platform,
-- such as a responsive search ad of Google, shows its ID.
select
    a.platform,
    p.platform_name,
    a.client_id,
    c.client_name,
    a.account_id,
    a.account_name,
    coalesce(m.campaign_id_text, cast(d.campaign_id as text)) as campaign_id,
    m.campaign_name,
    coalesce(g.ad_group_id_text, cast(d.ad_group_id as text)) as ad_group_id,
    g.ad_group_name,
    d.ad_id_text as ad_id,
    coalesce(d.ad_name, d.ad_id_text) as ad_name,
    d.ad_status,
    d.ad_type,
    d.first_report_date,
    d.last_report_date,
    d.ad_state,
    d.is_listed
from {{ ref('dim_ad') }} as d
left join {{ ref('dim_account') }} as a
    on a.account_sk = d.account_sk
left join {{ ref('dim_client') }} as c
    on c.client_id = a.client_id
left join {{ platforms(ref('seed_ad_platforms')) }}
    on p.platform = a.platform
left join {{ ref('dim_campaign') }} as m
    on m.account_sk = d.account_sk and m.campaign_id = d.campaign_id
left join {{ ref('dim_ad_group') }} as g
    on g.account_sk = d.account_sk and g.ad_group_id = d.ad_group_id
