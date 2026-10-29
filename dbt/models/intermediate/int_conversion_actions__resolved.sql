-- Every conversion action of the data or of the seed, with its name, its category and whether it
-- is primary: the settings of the deployment first, then the seed of the base package, then what
-- the platform says (ADR-0025).
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

actions as (
    select platform, conversion_action_id from observed
    union
    select platform, conversion_action_id from {{ ref('seed_conversion_actions') }}
)

select
    a.platform,
    a.conversion_action_id,
    coalesce(s.conversion_action_name, o.conversion_action_name, a.conversion_action_id)
        as conversion_action_name,
    coalesce(
        x.conversion_category,
        s.conversion_category,
        case o.platform_category
            when 'purchase' then 'purchase'
            when 'submit_lead_form' then 'lead'
            when 'phone_call_lead' then 'lead'
            when 'signup' then 'sign_up'
            when 'add_to_cart' then 'add_to_cart'
            when 'download' then 'app_install'
        end,
        'other'
    ) as conversion_category,
    coalesce(x.is_primary, s.is_primary, false) as is_primary
from actions as a
left join observed as o
    on o.platform = a.platform and o.conversion_action_id = a.conversion_action_id
left join {{ ref('seed_conversion_actions') }} as s
    on s.platform = a.platform and s.conversion_action_id = a.conversion_action_id
left join {{ ref('stg_settings__conversion_actions') }} as x
    on x.platform = a.platform and x.conversion_action_id = a.conversion_action_id
