-- The ad groups whose data this run of the transform takes into the core, ad sets of Meta among
-- them, one row each, with their campaign and their current attributes as this run sees them
-- (ADR-0036): each ad group that the newest list of its account names, as staging reads it from
-- the newest load of the account that a run pinned (newest_list); each ad group of the core of an
-- account whose newest list this run reads that the list no longer names, with the attributes that
-- the core holds, as one that the platform no longer lists; and each ad group that only the
-- statistics of ad groups name, as a stub, with the name and the campaign of its newest row of
-- statistics, so that each row of a fact has its ad group. With the surrogate of its account, the
-- common state of its status, whether the newest list names it, when a list last named it, and the
-- first and the last day of the statistics that this run reads. Only the accounts of
-- int_accounts__current enter (ADR-0035). The versions of the ad groups, the snapshot
-- dim_ad_group_versions, and the dimension core.dim_ad_group read it, and it reads
-- core.dim_ad_group as the runs before this one left it.
with listed as (
    select
        platform, account_id, campaign_id, ad_group_id, observed_name, ad_group_status,
        ad_group_state, ad_group_type
    from {{ ref('stg_demo_search__ad_groups') }}
    union all
    select
        platform, account_id, campaign_id, ad_group_id, observed_name, ad_group_status,
        ad_group_state, ad_group_type
    from {{ ref('stg_demo_social__ad_groups') }}
    union all
    select
        platform, account_id, campaign_id, ad_group_id, observed_name, ad_group_status,
        ad_group_state, ad_group_type
    from {{ ref('stg_demo_video__ad_groups') }}
    union all
    select
        platform, account_id, campaign_id, ad_group_id, observed_name, ad_group_status,
        ad_group_state, ad_group_type
    from {{ ref('stg_google_ads__ad_groups') }}
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

-- The accounts whose newest list this run reads, and when the load of that list ended: the lists
-- of the entities of an account come in the same load as its statistics (ADR-0038), and a list is
-- read even when it names no ad group.
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

-- The ad groups of the core of the accounts of this run, as the runs before this one left them.
known as (
    select
        a.platform, a.account_id, k.ad_group_id_text, k.campaign_id::text as campaign_id,
        k.ad_group_name, k.ad_group_status, k.ad_group_state, k.ad_group_type, k.last_seen_at,
        k.is_listed
    from {{ source('core', 'dim_ad_group') }} as k
    join accounts as a
        on a.account_sk = k.account_sk
),

ad_groups as (
    select platform, account_id, ad_group_id from listed
    union
    select platform, account_id, ad_group_id from reported
    union
    select k.platform, k.account_id, k.ad_group_id_text
    from known as k
    join lists as l
        on l.source = k.platform and l.account_id = k.account_id
)

select
    g.platform,
    g.account_id,
    a.account_sk,
    g.ad_group_id,
    -- The newest list gives the attributes of an ad group that it names; the core keeps those of
    -- an ad group that a list named before; a stub takes its name and campaign from the
    -- statistics.
    case
        when n.ad_group_id is not null then n.campaign_id
        when k.last_seen_at is not null then k.campaign_id
        else coalesce(r.campaign_id, k.campaign_id)
    end as campaign_id,
    case
        when n.ad_group_id is not null then n.observed_name
        when k.last_seen_at is not null then k.ad_group_name
        else coalesce(r.reported_name, k.ad_group_name)
    end as current_name,
    case when n.ad_group_id is not null then n.ad_group_status else k.ad_group_status end
        as ad_group_status,
    case
        when n.ad_group_id is not null then n.ad_group_state
        else coalesce(k.ad_group_state, 'other')
    end as ad_group_state,
    case when n.ad_group_id is not null then n.ad_group_type else k.ad_group_type end
        as ad_group_type,
    -- An account whose newest list this run does not read keeps what the core says of it.
    case
        when n.ad_group_id is not null then true
        when l.account_id is not null then false
        else coalesce(k.is_listed, false)
    end as is_listed,
    case when n.ad_group_id is not null then l.finished_at else k.last_seen_at end
        as last_seen_at,
    r.first_report_date,
    r.last_report_date
from ad_groups as g
join accounts as a
    on a.platform = g.platform and a.account_id = g.account_id
left join listed as n
    on n.platform = g.platform and n.account_id = g.account_id and n.ad_group_id = g.ad_group_id
left join reported as r
    on r.platform = g.platform and r.account_id = g.account_id and r.ad_group_id = g.ad_group_id
left join known as k
    on k.platform = g.platform and k.account_id = g.account_id
    and k.ad_group_id_text = g.ad_group_id
left join lists as l
    on l.source = g.platform and l.account_id = g.account_id
