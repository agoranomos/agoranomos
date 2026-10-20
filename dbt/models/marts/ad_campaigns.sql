-- The campaigns of every platform, with their current name, status and type, and the days that
-- their statistics cover.
select
    c.platform,
    p.platform_name,
    a.client_id,
    a.client_name,
    c.account_id,
    a.account_name,
    c.campaign_id,
    c.campaign_name,
    c.campaign_status,
    c.campaign_type,
    min(s.report_date) as first_report_date,
    max(s.report_date) as last_report_date,
    c.name_count
from {{ ref('int_ad_campaigns__unioned') }} as c
join {{ ref('int_ad_platforms__unioned') }} as p on p.platform = c.platform
left join {{ ref('int_ad_accounts__unioned') }} as a
    on a.platform = c.platform and a.account_id = c.account_id
left join {{ ref('int_ad_stats__unioned') }} as s
    on s.platform = c.platform and s.account_id = c.account_id and s.campaign_id = c.campaign_id
group by c.platform, p.platform_name, a.client_id, a.client_name, c.account_id, a.account_name,
    c.campaign_id, c.campaign_name, c.campaign_status, c.campaign_type, c.name_count
