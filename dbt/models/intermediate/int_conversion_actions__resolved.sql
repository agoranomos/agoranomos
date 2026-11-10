-- Every conversion action of the data or of the seed, with its name, its category and whether it
-- is primary: the settings of the deployment first, then, for Google Ads, what the account says
-- of the action, then the seed of the base package, then what the platform says (ADR-0025); and
-- what the platform says of it, its status, its category and its flag of the metric of
-- conversions, for the registry of conversion actions of the core (ADR-0036).
with found as (
    select platform, conversion_action_id, conversion_action_name, platform_category
    from {{ ref('stg_demo_search__campaign_conversions') }}
    union all
    select platform, conversion_action_id, conversion_action_name, platform_category
    from {{ ref('stg_demo_social__campaign_conversions') }}
    union all
    select platform, conversion_action_id, conversion_action_name, platform_category
    from {{ ref('stg_demo_video__campaign_conversions') }}
    union all
    select platform, conversion_action_id, conversion_action_name, platform_category
    from {{ ref('stg_google_ads__campaign_conversions') }}
),

observed as (
    select
        platform,
        conversion_action_id,
        max(conversion_action_name) as conversion_action_name,
        max(platform_category) as platform_category
    from found
    group by platform, conversion_action_id
),

-- Whether Google counts each action in its metric of conversions (include_in_conversions_metric),
-- as the newest list of conversion actions of each Google Ads account that this run reads gives
-- it: an action that a manager account owns can be listed by more than one account, and counts
-- when one of them counts it; an action that an account lists without the flag does not count.
newest as (
    select
        platform,
        conversion_action_id,
        max(conversion_action_name) as conversion_action_name,
        max(platform_category) as platform_category,
        max(conversion_action_status) as conversion_action_status,
        coalesce(bool_or(include_in_conversions_metric), false) as include_in_conversions_metric
    from {{ ref('stg_google_ads__conversion_actions') }}
    group by platform, conversion_action_id
),

-- An action that a list named before and that no list of this run names keeps what the core holds
-- of it, as the runs before this one left it: only a list gives the flag, so an action of the core
-- with one came from a list.
listed as (
    select * from newest
    union all
    select
        k.platform,
        k.conversion_action_id,
        k.conversion_action_name,
        k.platform_category,
        k.conversion_action_status,
        k.include_in_conversions_metric
    from {{ source('core', 'dim_conversion_action') }} as k
    where k.include_in_conversions_metric is not null
        and not exists (
            select from newest as n
            where n.platform = k.platform and n.conversion_action_id = k.conversion_action_id
        )
),

actions as (
    select platform, conversion_action_id from observed
    union
    select platform, conversion_action_id from listed
    union
    select platform, conversion_action_id from {{ ref('seed_conversion_actions') }}
)

select
    a.platform,
    a.conversion_action_id,
    coalesce(
        s.conversion_action_name,
        l.conversion_action_name,
        o.conversion_action_name,
        a.conversion_action_id
    ) as conversion_action_name,
    coalesce(
        x.conversion_category,
        s.conversion_category,
        case coalesce(o.platform_category, l.platform_category)
            when 'purchase' then 'purchase'
            when 'submit_lead_form' then 'lead'
            when 'phone_call_lead' then 'lead'
            when 'signup' then 'sign_up'
            when 'add_to_cart' then 'add_to_cart'
            when 'download' then 'app_install'
        end,
        'other'
    ) as conversion_category,
    coalesce(
        x.is_primary,
        case when a.platform = 'google_ads' then l.include_in_conversions_metric end,
        s.is_primary,
        false
    ) as is_primary,
    l.conversion_action_status,
    coalesce(o.platform_category, l.platform_category) as platform_category,
    l.include_in_conversions_metric
from actions as a
left join observed as o
    on o.platform = a.platform and o.conversion_action_id = a.conversion_action_id
left join listed as l
    on l.platform = a.platform and l.conversion_action_id = a.conversion_action_id
left join {{ ref('seed_conversion_actions') }} as s
    on s.platform = a.platform and s.conversion_action_id = a.conversion_action_id
left join {{ ref('stg_settings__conversion_actions') }} as x
    on x.platform = a.platform and x.conversion_action_id = a.conversion_action_id
