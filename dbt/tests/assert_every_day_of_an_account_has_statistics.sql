-- Every day, between the first and the last day of each account in the windows of the run, on
-- which the account has no row of the statistics of its campaigns: a day that a load missed. A
-- warning, as a day on which every campaign of an account paused has no row either, and a row
-- whose measures are all zero is not kept (ADR-0036).
{{ config(severity='warn') }}
with rows as (
    select platform, account_id, report_date
    from {{ ref('ad_stats_campaign_daily') }}
    where report_date >= {{ window_start() }}
),

bounds as (
    select platform, account_id, min(report_date) as first_day, max(report_date) as last_day
    from rows
    group by platform, account_id
),

expected as (
    select
        platform,
        account_id,
        cast(generate_series(first_day, last_day, interval '1 day') as date) as missing_day
    from bounds
)

select e.*
from expected as e
where not exists (
    select
    from rows as r
    where r.platform = e.platform and r.account_id = e.account_id and r.report_date = e.missing_day
)
