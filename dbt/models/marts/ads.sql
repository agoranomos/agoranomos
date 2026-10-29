-- The ads of every platform, with their current name, status and type, their campaign and ad
-- group, and the days that their statistics cover (ADR-0034).
select
    d.platform,
    p.platform_name,
    a.client_id,
    a.client_name,
    d.account_id,
    a.account_name,
    d.campaign_id,
    c.campaign_name,
    d.ad_group_id,
    g.ad_group_name,
    d.ad_id,
    d.ad_name,
    d.ad_status,
    d.ad_type,
    min(s.report_date) as first_report_date,
    max(s.report_date) as last_report_date
from {{ ref('int_ads__unioned') }} as d
join {{ ref('int_ad_platforms__unioned') }} as p on p.platform = d.platform
left join {{ ref('int_ad_accounts__unioned') }} as a
    on a.platform = d.platform and a.account_id = d.account_id
left join {{ ref('int_ad_campaigns__unioned') }} as c
    on c.platform = d.platform and c.account_id = d.account_id and c.campaign_id = d.campaign_id
left join {{ ref('int_ad_groups__unioned') }} as g
    on g.platform = d.platform and g.account_id = d.account_id and g.ad_group_id = d.ad_group_id
left join {{ ref('int_ad_stats_ad__unioned') }} as s
    on s.platform = d.platform and s.account_id = d.account_id
    and s.ad_group_id = d.ad_group_id and s.ad_id = d.ad_id
group by d.platform, p.platform_name, a.client_id, a.client_name, d.account_id, a.account_name,
    d.campaign_id, c.campaign_name, d.ad_group_id, g.ad_group_name, d.ad_id, d.ad_name,
    d.ad_status, d.ad_type
