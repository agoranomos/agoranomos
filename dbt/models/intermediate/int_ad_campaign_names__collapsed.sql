-- The names that each campaign had, with the time from which and until which it had each: the
-- versions that dlt keeps change with the status as well, so consecutive versions of one name
-- are one period.
with versions as (
    select * from {{ ref('stg_demo_search__campaigns') }}
    union all
    select * from {{ ref('stg_demo_social__campaigns') }}
    union all
    select * from {{ ref('stg_demo_video__campaigns') }}
),

marked as (
    select
        *,
        case
            when lag(observed_name) over (
                partition by platform, account_id, campaign_id order by valid_from
            ) is distinct from observed_name then 1
            else 0
        end as starts_a_name
    from versions
),

periods as (
    select
        *,
        sum(starts_a_name) over (
            partition by platform, account_id, campaign_id order by valid_from
        ) as period
    from marked
)

select
    platform,
    account_id,
    campaign_id,
    observed_name,
    min(valid_from) as valid_from,
    case when bool_or(valid_to is null) then null else max(valid_to) end as valid_to
from periods
group by platform, account_id, campaign_id, observed_name, period
