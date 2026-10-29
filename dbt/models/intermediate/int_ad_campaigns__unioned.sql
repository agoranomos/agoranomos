-- The campaigns of every platform as of the last load: the newest version of each, whether
-- current or retired, as dlt keeps them, and how many names each had.
with versions as (
    select * from {{ ref('stg_demo_search__campaigns') }}
    union all
    select * from {{ ref('stg_demo_social__campaigns') }}
    union all
    select * from {{ ref('stg_demo_video__campaigns') }}
    union all
    select * from {{ ref('stg_google_ads__campaigns') }}
),

newest as (
    select
        *,
        row_number() over (
            partition by platform, account_id, campaign_id order by valid_from desc
        ) as recency
    from versions
),

names as (
    select platform, account_id, campaign_id, count(distinct observed_name) as name_count
    from versions
    group by platform, account_id, campaign_id
)

select
    n.platform,
    n.account_id,
    n.campaign_id,
    n.observed_name as campaign_name,
    n.campaign_status,
    n.campaign_type,
    c.name_count
from newest as n
join names as c
    on c.platform = n.platform and c.account_id = n.account_id and c.campaign_id = n.campaign_id
where n.recency = 1
