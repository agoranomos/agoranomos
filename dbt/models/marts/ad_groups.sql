-- The ad groups of every platform, with their current name, status and type, their campaign, and
-- the days that their statistics cover (ADR-0034).
select
    g.platform,
    p.platform_name,
    a.client_id,
    a.client_name,
    g.account_id,
    a.account_name,
    g.campaign_id,
    c.campaign_name,
    g.ad_group_id,
    g.ad_group_name,
    g.ad_group_status,
    g.ad_group_type,
    min(s.report_date) as first_report_date,
    max(s.report_date) as last_report_date
from {{ ref('int_ad_groups__unioned') }} as g
join {{ ref('int_ad_platforms__unioned') }} as p on p.platform = g.platform
left join {{ ref('int_ad_accounts__unioned') }} as a
    on a.platform = g.platform and a.account_id = g.account_id
left join {{ ref('int_ad_campaigns__unioned') }} as c
    on c.platform = g.platform and c.account_id = g.account_id and c.campaign_id = g.campaign_id
left join {{ ref('int_ad_stats_ad_group__unioned') }} as s
    on s.platform = g.platform and s.account_id = g.account_id and s.ad_group_id = g.ad_group_id
group by g.platform, p.platform_name, a.client_id, a.client_name, g.account_id, a.account_name,
    g.campaign_id, c.campaign_name, g.ad_group_id, g.ad_group_name, g.ad_group_status,
    g.ad_group_type
