-- The campaigns of every platform, with their current name, status and type, and the days that
-- their statistics cover. The days come down to one row for each campaign before the join: joined
-- with every row of the statistics, PostgreSQL 18.6 ran the views of the statistics once for each
-- campaign, a minute for the 47 campaigns of the demo (checked on the stand, October 2026).
with stats as (
    select
        platform,
        account_id,
        campaign_id,
        min(report_date) as first_report_date,
        max(report_date) as last_report_date
    from {{ ref('int_ad_stats__unioned') }}
    group by platform, account_id, campaign_id
)

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
    s.first_report_date,
    s.last_report_date,
    c.name_count
from {{ ref('int_ad_campaigns__unioned') }} as c
join {{ ref('int_ad_platforms__unioned') }} as p on p.platform = c.platform
left join {{ ref('int_ad_accounts__unioned') }} as a
    on a.platform = c.platform and a.account_id = c.account_id
left join stats as s
    on s.platform = c.platform and s.account_id = c.account_id and s.campaign_id = c.campaign_id
