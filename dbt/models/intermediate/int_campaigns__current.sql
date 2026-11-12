-- The campaigns whose data this run of the transform takes into the core, one row each, with their
-- current attributes as this run sees them (ADR-0036): each campaign that the newest list of its
-- account names, as staging reads it from the newest load of the account that a run pinned
-- (newest_list); each campaign of the core of an account whose newest list this run reads that the
-- list no longer names, with the attributes that the core holds, as one that the platform no
-- longer lists; and each campaign that only the statistics of campaigns name, as a stub, with the
-- name of its newest row of statistics, so that each row of a fact has its campaign. With the
-- surrogate of its account, the common state of its status, whether the newest list names it, when
-- a list last named it, and the first and the last day of the statistics that this run reads. The
-- conversions of a campaign come from rows of its statistics on every platform of the model, so
-- they name no other campaign. Only the accounts of int_accounts__current enter (ADR-0035). The
-- versions of the campaigns, the snapshot dim_campaign_versions, and the dimension core.dim_campaign
-- read it, and it reads core.dim_campaign as the runs before this one left it.
with listed as (
    select
        platform, account_id, campaign_id, observed_name, campaign_status, campaign_state,
        campaign_type
    from {{ ref('stg_demo_search__campaigns') }}
    union all
    select
        platform, account_id, campaign_id, observed_name, campaign_status, campaign_state,
        campaign_type
    from {{ ref('stg_demo_social__campaigns') }}
    union all
    select
        platform, account_id, campaign_id, observed_name, campaign_status, campaign_state,
        campaign_type
    from {{ ref('stg_demo_video__campaigns') }}
    union all
    select
        platform, account_id, campaign_id, observed_name, campaign_status, campaign_state,
        campaign_type
    from {{ ref('stg_google_ads__campaigns') }}
),

stats as (
    select platform, account_id, campaign_id, report_date, loaded_campaign_name
    from {{ ref('stg_demo_search__campaign_stats') }}
    union all
    select platform, account_id, campaign_id, report_date, loaded_campaign_name
    from {{ ref('stg_demo_social__campaign_stats') }}
    union all
    select platform, account_id, campaign_id, report_date, loaded_campaign_name
    from {{ ref('stg_demo_video__campaign_stats') }}
    union all
    select platform, account_id, campaign_id, report_date, loaded_campaign_name
    from {{ ref('stg_google_ads__campaign_stats') }}
),

reported as (
    select
        platform,
        account_id,
        campaign_id,
        min(report_date) as first_report_date,
        max(report_date) as last_report_date,
        (array_agg(loaded_campaign_name order by report_date desc))[1] as reported_name
    from stats
    group by platform, account_id, campaign_id
),

-- The accounts whose newest list this run reads, and when the load of that list ended: the lists
-- of the entities of an account come in the same load as its statistics (ADR-0038), and a list is
-- read even when it names no campaign.
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

-- The campaigns of the core of the accounts of this run, as the runs before this one left them.
known as (
    select
        a.platform, a.account_id, k.campaign_id_text, k.campaign_name, k.campaign_status,
        k.campaign_state, k.campaign_type, k.last_seen_at, k.is_listed
    from {{ source('core', 'dim_campaign') }} as k
    join accounts as a
        on a.account_sk = k.account_sk
),

campaigns as (
    select platform, account_id, campaign_id from listed
    union
    select platform, account_id, campaign_id from reported
    union
    select k.platform, k.account_id, k.campaign_id_text
    from known as k
    join lists as l
        on l.source = k.platform and l.account_id = k.account_id
)

select
    c.platform,
    c.account_id,
    a.account_sk,
    c.campaign_id,
    -- The newest list gives the attributes of a campaign that it names; the core keeps those of a
    -- campaign that a list named before; a stub takes its name from the statistics.
    case
        when n.campaign_id is not null then n.observed_name
        when k.last_seen_at is not null then k.campaign_name
        else coalesce(r.reported_name, k.campaign_name)
    end as current_name,
    case when n.campaign_id is not null then n.campaign_status else k.campaign_status end
        as campaign_status,
    case
        when n.campaign_id is not null then n.campaign_state
        else coalesce(k.campaign_state, 'other')
    end as campaign_state,
    case when n.campaign_id is not null then n.campaign_type else k.campaign_type end
        as campaign_type,
    -- An account whose newest list this run does not read keeps what the core says of it.
    case
        when n.campaign_id is not null then true
        when l.account_id is not null then false
        else coalesce(k.is_listed, false)
    end as is_listed,
    case when n.campaign_id is not null then l.finished_at else k.last_seen_at end
        as last_seen_at,
    r.first_report_date,
    r.last_report_date
from campaigns as c
join accounts as a
    on a.platform = c.platform and a.account_id = c.account_id
left join listed as n
    on n.platform = c.platform and n.account_id = c.account_id and n.campaign_id = c.campaign_id
left join reported as r
    on r.platform = c.platform and r.account_id = c.account_id and r.campaign_id = c.campaign_id
left join known as k
    on k.platform = c.platform and k.account_id = c.account_id
    and k.campaign_id_text = c.campaign_id
left join lists as l
    on l.source = c.platform and l.account_id = c.account_id
