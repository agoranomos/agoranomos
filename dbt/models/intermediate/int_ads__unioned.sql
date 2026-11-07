-- The ads of every platform as of the last load: the newest version of each, whether current or no
-- longer listed, as dlt keeps them, named by its ID where the platform gives it no name (ADR-0034).
-- Its tests check each ad against the ad groups of the platforms.
-- depends_on: {{ ref('int_ad_groups__unioned') }}
with versions as (
    select *
    from (
        select * from {{ ref('stg_demo_search__ads') }}
        union all
        select * from {{ ref('stg_demo_social__ads') }}
        union all
        select * from {{ ref('stg_demo_video__ads') }}
        union all
        select * from {{ ref('stg_google_ads__ads') }}
    ) as unioned
    -- An account in quarantine in this run stays out of the marts, as out of the core (ADR-0035).
    where {{ not_in_quarantine(ref('int_g1_failures'), 'unioned.platform', 'unioned.account_id') }}
),

newest as (
    select
        *,
        row_number() over (
            partition by platform, account_id, ad_group_id, ad_id order by valid_from desc
        ) as recency
    from versions
)

select
    platform,
    account_id,
    campaign_id,
    ad_group_id,
    ad_id,
    coalesce(observed_name, ad_id) as ad_name,
    ad_status,
    ad_type
from newest
where recency = 1
