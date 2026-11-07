-- The accounts of the ad platforms and the properties of web analytics whose data this run of the
-- transform takes into the core (ADR-0036): every account that a list of accounts gives, and each
-- one that only the statistics of campaigns or the sessions of web analytics name, as a stub, so
-- that each row of a fact has its account; with the first and the last day of the statistics of
-- its campaigns that this run reads. The registry core.dim_account takes its rows from here, and
-- the views of the entities take their accounts from here, so an account that is to stay out of
-- the core in a run, as one in quarantine (ADR-0035), is left out here once and for all of them.
with listed as (
    select platform, account_id, account_name, currency, time_zone, manager_account_id, account_status
    from {{ ref('stg_demo_search__accounts') }}
    union all
    select platform, account_id, account_name, currency, time_zone, manager_account_id, account_status
    from {{ ref('stg_demo_social__accounts') }}
    union all
    select platform, account_id, account_name, currency, time_zone, manager_account_id, account_status
    from {{ ref('stg_demo_video__accounts') }}
    union all
    select platform, account_id, account_name, currency, time_zone, manager_account_id, account_status
    from {{ ref('stg_google_ads__accounts') }}
    union all
    select
        platform,
        property_id,
        property_name,
        currency,
        time_zone,
        cast(null as text),
        cast(null as text)
    from {{ ref('stg_demo_web__properties') }}
),

stats as (
    select platform, account_id, report_date from {{ ref('stg_demo_search__campaign_stats') }}
    union all
    select platform, account_id, report_date from {{ ref('stg_demo_social__campaign_stats') }}
    union all
    select platform, account_id, report_date from {{ ref('stg_demo_video__campaign_stats') }}
    union all
    select platform, account_id, report_date from {{ ref('stg_google_ads__campaign_stats') }}
),

reported as (
    select
        platform,
        account_id,
        min(report_date) as first_report_date,
        max(report_date) as last_report_date
    from stats
    group by platform, account_id
),

named as (
    select platform, account_id from listed
    union
    select platform, account_id from reported
    union
    select distinct platform, property_id from {{ ref('stg_demo_web__sessions_daily') }}
)

select
    n.platform,
    n.account_id,
    l.account_name,
    l.currency,
    l.time_zone,
    l.manager_account_id,
    l.account_status,
    l.account_id is null as is_stub,
    r.first_report_date,
    r.last_report_date
from named as n
left join listed as l
    on l.platform = n.platform and l.account_id = n.account_id
left join reported as r
    on r.platform = n.platform and r.account_id = n.account_id
