-- The conversions of every platform by campaign, day and conversion action, with the category of
-- each action and whether it is primary, and the client and the currency of the account. Its
-- tests check each campaign against the campaigns of the platforms.
-- depends_on: {{ ref('int_ad_campaigns__unioned') }}
with conversions as (
    select * from {{ ref('stg_demo_search__campaign_conversions') }}
    union all
    select * from {{ ref('stg_demo_social__campaign_conversions') }}
    union all
    select * from {{ ref('stg_demo_video__campaign_conversions') }}
    union all
    select * from {{ ref('stg_google_ads__campaign_conversions') }}
)

select
    c.report_date,
    c.platform,
    a.client_id,
    a.client_name,
    c.account_id,
    a.account_name,
    c.campaign_id,
    c.conversion_action_id,
    r.conversion_action_name,
    r.conversion_category,
    r.is_primary,
    c.attribution_setting,
    a.currency,
    c.conversions,
    c.conversion_value
from conversions as c
join {{ ref('int_conversion_actions__resolved') }} as r
    on r.platform = c.platform and r.conversion_action_id = c.conversion_action_id
left join {{ ref('int_ad_accounts__unioned') }} as a
    on a.platform = c.platform and a.account_id = c.account_id
