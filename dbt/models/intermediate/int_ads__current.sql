-- The ads whose data this run of the transform takes into the core, one row each, with their
-- campaign, their ad group and their current attributes as this run sees them (ADR-0036): each ad
-- that the newest list of its account names, as staging reads it from the newest load of the
-- account that a run pinned (newest_list); each ad of the core of an account whose newest list this
-- run reads that the list no longer names, with the attributes that the core holds, as one that
-- the platform no longer lists; and each ad that only the statistics of ads name, as a stub, with
-- the name and the campaign of its newest row of statistics, so that each row of a fact has its
-- ad. With the surrogate of its account, the common state of its status, whether the newest list
-- names it, when a list last named it, and the first and the last day of the statistics that this
-- run reads. An ad is defined only inside its ad group, which is part of its key, and an ad that
-- its platform gives no name has none here. Only the accounts of int_accounts__current enter
-- (ADR-0035). The versions of the ads, the snapshot dim_ad_versions, and the dimension core.dim_ad
-- read it, and it reads core.dim_ad as the runs before this one left it.
with listed as (
    select
        platform, account_id, campaign_id, ad_group_id, ad_id, observed_name, ad_status, ad_state,
        ad_type
    from {{ ref('stg_demo_search__ads') }}
    union all
    select
        platform, account_id, campaign_id, ad_group_id, ad_id, observed_name, ad_status, ad_state,
        ad_type
    from {{ ref('stg_demo_social__ads') }}
    union all
    select
        platform, account_id, campaign_id, ad_group_id, ad_id, observed_name, ad_status, ad_state,
        ad_type
    from {{ ref('stg_demo_video__ads') }}
    union all
    select
        platform, account_id, campaign_id, ad_group_id, ad_id, observed_name, ad_status, ad_state,
        ad_type
    from {{ ref('stg_google_ads__ads') }}
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

-- The accounts whose newest list this run reads, and when the load of that list ended: the lists
-- of the entities of an account come in the same load as its statistics (ADR-0038), and a list is
-- read even when it names no ad.
lists as (
    {{ newest_lists('raw_demo_search', source('core', 'loads')) }}
    union all
    {{ newest_lists('raw_demo_social', source('core', 'loads')) }}
    union all
    {{ newest_lists('raw_demo_video', source('core', 'loads')) }}
    union all
    {{ newest_lists('raw_google_ads', source('core', 'loads')) }}
),

accounts as (
    select a.platform, a.account_id, d.account_sk
    from {{ ref('int_accounts__current') }} as a
    join {{ ref('dim_account') }} as d
        on d.platform = a.platform and d.account_id = a.account_id
),

-- The ads of the core of the accounts of this run, as the runs before this one left them.
known as (
    select
        a.platform, a.account_id, k.ad_group_id::text as ad_group_id, k.ad_id_text,
        k.campaign_id::text as campaign_id, k.ad_name, k.ad_status, k.ad_state, k.ad_type,
        k.last_seen_at, k.is_listed
    from {{ source('core', 'dim_ad') }} as k
    join accounts as a
        on a.account_sk = k.account_sk
),

ads as (
    select platform, account_id, ad_group_id, ad_id from listed
    union
    select platform, account_id, ad_group_id, ad_id from reported
    union
    select k.platform, k.account_id, k.ad_group_id, k.ad_id_text
    from known as k
    join lists as l
        on l.source = k.platform and l.account_id = k.account_id
)

select
    e.platform,
    e.account_id,
    a.account_sk,
    e.ad_group_id,
    e.ad_id,
    -- The newest list gives the attributes of an ad that it names; the core keeps those of an ad
    -- that a list named before; a stub takes its name and campaign from the statistics.
    case
        when n.ad_id is not null then n.campaign_id
        when k.last_seen_at is not null then k.campaign_id
        else coalesce(r.campaign_id, k.campaign_id)
    end as campaign_id,
    case
        when n.ad_id is not null then n.observed_name
        when k.last_seen_at is not null then k.ad_name
        else coalesce(r.reported_name, k.ad_name)
    end as current_name,
    case when n.ad_id is not null then n.ad_status else k.ad_status end as ad_status,
    case when n.ad_id is not null then n.ad_state else coalesce(k.ad_state, 'other') end
        as ad_state,
    case when n.ad_id is not null then n.ad_type else k.ad_type end as ad_type,
    -- An account whose newest list this run does not read keeps what the core says of it.
    case
        when n.ad_id is not null then true
        when l.account_id is not null then false
        else coalesce(k.is_listed, false)
    end as is_listed,
    case when n.ad_id is not null then l.finished_at else k.last_seen_at end as last_seen_at,
    r.first_report_date,
    r.last_report_date
from ads as e
join accounts as a
    on a.platform = e.platform and a.account_id = e.account_id
left join listed as n
    on n.platform = e.platform and n.account_id = e.account_id
    and n.ad_group_id = e.ad_group_id and n.ad_id = e.ad_id
left join reported as r
    on r.platform = e.platform and r.account_id = e.account_id
    and r.ad_group_id = e.ad_group_id and r.ad_id = e.ad_id
left join known as k
    on k.platform = e.platform and k.account_id = e.account_id
    and k.ad_group_id = e.ad_group_id and k.ad_id_text = e.ad_id
left join lists as l
    on l.source = e.platform and l.account_id = e.account_id
