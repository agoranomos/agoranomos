-- The ad accounts of every platform, with their client, currency and time zone, and the days that
-- their statistics cover.
select
    a.platform,
    p.platform_name,
    a.account_id,
    a.account_name,
    a.client_id,
    a.client_name,
    a.currency,
    a.time_zone,
    a.manager_account_id,
    a.account_status,
    min(s.report_date) as first_report_date,
    max(s.report_date) as last_report_date
from {{ ref('int_ad_accounts__unioned') }} as a
join {{ ref('int_ad_platforms__unioned') }} as p on p.platform = a.platform
left join {{ ref('int_ad_stats__unioned') }} as s
    on s.platform = a.platform and s.account_id = a.account_id
group by a.platform, p.platform_name, a.account_id, a.account_name, a.client_id, a.client_name,
    a.currency, a.time_zone, a.manager_account_id, a.account_status
