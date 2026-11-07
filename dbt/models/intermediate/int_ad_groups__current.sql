-- The ad groups whose data this run of the transform takes into the core, ad sets of Meta among
-- them, one row each, with their campaign and their current attributes as this run sees them
-- (ADR-0036): the newest version of each ad group that a list of its account gave, whether the
-- newest list still names it or not, and each ad group that only the statistics of ad groups name,
-- as a stub, with the name and the campaign of its newest row of statistics, so that each row of a
-- fact has its ad group; with the surrogate of its account, the common state of its status,
-- whether the newest list names it, when a list last named it, and the first and the last day of
-- the statistics that this run reads. Only the accounts of int_accounts__current enter (ADR-0035).
-- The versions of the ad groups, the snapshot dim_ad_group_versions, and the dimension
-- core.dim_ad_group read it.
with versions as (
    select
        platform, account_id, campaign_id, ad_group_id, observed_name, ad_group_status,
        ad_group_state, ad_group_type, valid_from, valid_to
    from {{ ref('stg_demo_search__ad_groups') }}
    union all
    select
        platform, account_id, campaign_id, ad_group_id, observed_name, ad_group_status,
        ad_group_state, ad_group_type, valid_from, valid_to
    from {{ ref('stg_demo_social__ad_groups') }}
    union all
    select
        platform, account_id, campaign_id, ad_group_id, observed_name, ad_group_status,
        ad_group_state, ad_group_type, valid_from, valid_to
    from {{ ref('stg_demo_video__ad_groups') }}
    union all
    select
        platform, account_id, campaign_id, ad_group_id, observed_name, ad_group_status,
        ad_group_state, ad_group_type, valid_from, valid_to
    from {{ ref('stg_google_ads__ad_groups') }}
),

-- The newest version of each ad group of the lists: the current one while the newest list of its
-- account names it, else the one that a later list closed.
newest as (
    select distinct on (platform, account_id, ad_group_id) *
    from versions
    order by platform, account_id, ad_group_id, valid_from desc
),

stats as (
    select platform, account_id, campaign_id, ad_group_id, report_date, loaded_ad_group_name
    from {{ ref('stg_demo_search__ad_group_stats') }}
    union all
    select platform, account_id, campaign_id, ad_group_id, report_date, loaded_ad_group_name
    from {{ ref('stg_demo_social__ad_group_stats') }}
    union all
    select platform, account_id, campaign_id, ad_group_id, report_date, loaded_ad_group_name
    from {{ ref('stg_demo_video__ad_group_stats') }}
    union all
    select platform, account_id, campaign_id, ad_group_id, report_date, loaded_ad_group_name
    from {{ ref('stg_google_ads__ad_group_stats') }}
),

reported as (
    select
        platform,
        account_id,
        ad_group_id,
        (array_agg(campaign_id order by report_date desc))[1] as campaign_id,
        min(report_date) as first_report_date,
        max(report_date) as last_report_date,
        (array_agg(loaded_ad_group_name order by report_date desc))[1] as reported_name
    from stats
    group by platform, account_id, ad_group_id
),

ad_groups as (
    select platform, account_id, ad_group_id from newest
    union
    select platform, account_id, ad_group_id from reported
),

-- When the newest load of each account that the runs pinned ended: the lists of the entities of an
-- account come in the same load as its statistics (ADR-0038).
loaded as (
    select source as platform, account_id, max(finished_at) as loaded_at
    from {{ source('core', 'loads') }}
    where outcome = 'loaded'
    group by source, account_id
),

accounts as (
    select a.platform, a.account_id, d.account_sk
    from {{ ref('int_accounts__current') }} as a
    join {{ ref('dim_account') }} as d
        on d.platform = a.platform and d.account_id = a.account_id
)

select
    g.platform,
    g.account_id,
    a.account_sk,
    g.ad_group_id,
    case when n.ad_group_id is not null then n.campaign_id else r.campaign_id end as campaign_id,
    case when n.ad_group_id is not null then n.observed_name else r.reported_name end
        as current_name,
    n.ad_group_status,
    coalesce(n.ad_group_state, 'other') as ad_group_state,
    n.ad_group_type,
    n.ad_group_id is not null and n.valid_to is null as is_listed,
    case
        when n.ad_group_id is null then null
        when n.valid_to is null then greatest(n.valid_from, l.loaded_at)
        else n.valid_from
    end as last_seen_at,
    r.first_report_date,
    r.last_report_date
from ad_groups as g
join accounts as a
    on a.platform = g.platform and a.account_id = g.account_id
left join newest as n
    on n.platform = g.platform and n.account_id = g.account_id and n.ad_group_id = g.ad_group_id
left join reported as r
    on r.platform = g.platform and r.account_id = g.account_id and r.ad_group_id = g.ad_group_id
left join loaded as l
    on l.platform = g.platform and l.account_id = g.account_id
