-- The ads whose data this run of the transform takes into the core, one row each, with their
-- campaign, their ad group and their current attributes as this run sees them (ADR-0036): the
-- newest version of each ad that a list of its account gave, whether the newest list still names
-- it or not, and each ad that only the statistics of ads name, as a stub, with the name and the
-- campaign of its newest row of statistics, so that each row of a fact has its ad; with the
-- surrogate of its account, the common state of its status, whether the newest list names it,
-- when a list last named it, and the first and the last day of the statistics that this run
-- reads. An ad is defined only inside its ad group, which is part of its key, and an ad that its
-- platform gives no name has none here. Only the accounts of int_accounts__current enter
-- (ADR-0035). The versions of the ads, the snapshot dim_ad_versions, and the dimension core.dim_ad
-- read it.
with versions as (
    select
        platform, account_id, campaign_id, ad_group_id, ad_id, observed_name, ad_status, ad_state,
        ad_type, valid_from, valid_to
    from {{ ref('stg_demo_search__ads') }}
    union all
    select
        platform, account_id, campaign_id, ad_group_id, ad_id, observed_name, ad_status, ad_state,
        ad_type, valid_from, valid_to
    from {{ ref('stg_demo_social__ads') }}
    union all
    select
        platform, account_id, campaign_id, ad_group_id, ad_id, observed_name, ad_status, ad_state,
        ad_type, valid_from, valid_to
    from {{ ref('stg_demo_video__ads') }}
    union all
    select
        platform, account_id, campaign_id, ad_group_id, ad_id, observed_name, ad_status, ad_state,
        ad_type, valid_from, valid_to
    from {{ ref('stg_google_ads__ads') }}
),

-- The newest version of each ad of the lists: the current one while the newest list of its
-- account names it, else the one that a later list closed.
newest as (
    select distinct on (platform, account_id, ad_group_id, ad_id) *
    from versions
    order by platform, account_id, ad_group_id, ad_id, valid_from desc
),

stats as (
    select platform, account_id, campaign_id, ad_group_id, ad_id, report_date, loaded_ad_name
    from {{ ref('stg_demo_search__ad_stats') }}
    union all
    select platform, account_id, campaign_id, ad_group_id, ad_id, report_date, loaded_ad_name
    from {{ ref('stg_demo_social__ad_stats') }}
    union all
    select platform, account_id, campaign_id, ad_group_id, ad_id, report_date, loaded_ad_name
    from {{ ref('stg_demo_video__ad_stats') }}
    union all
    select platform, account_id, campaign_id, ad_group_id, ad_id, report_date, loaded_ad_name
    from {{ ref('stg_google_ads__ad_stats') }}
),

reported as (
    select
        platform,
        account_id,
        ad_group_id,
        ad_id,
        (array_agg(campaign_id order by report_date desc))[1] as campaign_id,
        min(report_date) as first_report_date,
        max(report_date) as last_report_date,
        (array_agg(loaded_ad_name order by report_date desc))[1] as reported_name
    from stats
    group by platform, account_id, ad_group_id, ad_id
),

ads as (
    select platform, account_id, ad_group_id, ad_id from newest
    union
    select platform, account_id, ad_group_id, ad_id from reported
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
    e.platform,
    e.account_id,
    a.account_sk,
    e.ad_group_id,
    e.ad_id,
    case when n.ad_id is not null then n.campaign_id else r.campaign_id end as campaign_id,
    case when n.ad_id is not null then n.observed_name else r.reported_name end as current_name,
    n.ad_status,
    coalesce(n.ad_state, 'other') as ad_state,
    n.ad_type,
    n.ad_id is not null and n.valid_to is null as is_listed,
    case
        when n.ad_id is null then null
        when n.valid_to is null then greatest(n.valid_from, l.loaded_at)
        else n.valid_from
    end as last_seen_at,
    r.first_report_date,
    r.last_report_date
from ads as e
join accounts as a
    on a.platform = e.platform and a.account_id = e.account_id
left join newest as n
    on n.platform = e.platform and n.account_id = e.account_id
    and n.ad_group_id = e.ad_group_id and n.ad_id = e.ad_id
left join reported as r
    on r.platform = e.platform and r.account_id = e.account_id
    and r.ad_group_id = e.ad_group_id and r.ad_id = e.ad_id
left join loaded as l
    on l.platform = e.platform and l.account_id = e.account_id
