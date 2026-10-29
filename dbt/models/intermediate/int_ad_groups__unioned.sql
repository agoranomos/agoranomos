-- The ad groups of every platform as of the last load: the newest version of each, whether
-- current or no longer listed, as dlt keeps them (ADR-0034).
-- Its tests check each ad group against the campaigns of the platforms.
-- depends_on: {{ ref('int_ad_campaigns__unioned') }}
with versions as (
    select * from {{ ref('stg_demo_search__ad_groups') }}
    union all
    select * from {{ ref('stg_demo_social__ad_groups') }}
    union all
    select * from {{ ref('stg_demo_video__ad_groups') }}
    union all
    select * from {{ ref('stg_google_ads__ad_groups') }}
),

newest as (
    select
        *,
        row_number() over (
            partition by platform, account_id, ad_group_id order by valid_from desc
        ) as recency
    from versions
)

select
    platform,
    account_id,
    campaign_id,
    ad_group_id,
    observed_name as ad_group_name,
    ad_group_status,
    ad_group_type
from newest
where recency = 1
