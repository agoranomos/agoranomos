-- The campaigns whose data this run of the transform takes into the core, one row each, with their
-- current attributes as this run sees them (ADR-0036): the newest version of each campaign that a
-- list of its account gave, whether the newest list still names it or not, and each campaign that
-- only the statistics of campaigns name, as a stub, with the name of its newest row of statistics,
-- so that each row of a fact has its campaign; with the surrogate of its account, the common state
-- of its status, whether the newest list names it, when a list last named it, and the first and
-- the last day of the statistics that this run reads. The conversions of a campaign come from rows
-- of its statistics on every platform of the model, so they name no other campaign. Only the
-- accounts of int_accounts__current enter (ADR-0035). The versions of the campaigns, the snapshot
-- dim_campaign_versions, and the dimension core.dim_campaign read it.
with versions as (
    select
        platform, account_id, campaign_id, observed_name, campaign_status, campaign_state,
        campaign_type, valid_from, valid_to
    from {{ ref('stg_demo_search__campaigns') }}
    union all
    select
        platform, account_id, campaign_id, observed_name, campaign_status, campaign_state,
        campaign_type, valid_from, valid_to
    from {{ ref('stg_demo_social__campaigns') }}
    union all
    select
        platform, account_id, campaign_id, observed_name, campaign_status, campaign_state,
        campaign_type, valid_from, valid_to
    from {{ ref('stg_demo_video__campaigns') }}
    union all
    select
        platform, account_id, campaign_id, observed_name, campaign_status, campaign_state,
        campaign_type, valid_from, valid_to
    from {{ ref('stg_google_ads__campaigns') }}
),

-- The newest version of each campaign of the lists: the current one while the newest list of its
-- account names it, else the one that a later list closed.
newest as (
    select distinct on (platform, account_id, campaign_id) *
    from versions
    order by platform, account_id, campaign_id, valid_from desc
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

campaigns as (
    select platform, account_id, campaign_id from newest
    union
    select platform, account_id, campaign_id from reported
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
    c.platform,
    c.account_id,
    a.account_sk,
    c.campaign_id,
    case when n.campaign_id is not null then n.observed_name else r.reported_name end
        as current_name,
    n.campaign_status,
    coalesce(n.campaign_state, 'other') as campaign_state,
    n.campaign_type,
    n.campaign_id is not null and n.valid_to is null as is_listed,
    case
        when n.campaign_id is null then null
        when n.valid_to is null then greatest(n.valid_from, l.loaded_at)
        else n.valid_from
    end as last_seen_at,
    r.first_report_date,
    r.last_report_date
from campaigns as c
join accounts as a
    on a.platform = c.platform and a.account_id = c.account_id
left join newest as n
    on n.platform = c.platform and n.account_id = c.account_id and n.campaign_id = c.campaign_id
left join reported as r
    on r.platform = c.platform and r.account_id = c.account_id and r.campaign_id = c.campaign_id
left join loaded as l
    on l.platform = c.platform and l.account_id = c.account_id
