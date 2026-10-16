-- Conversions by day, campaign and conversion action on every platform, with the category of each
-- action and whether it counts in the conversions of the campaign.
select
    c.report_date,
    c.platform,
    p.platform_name,
    c.client_id,
    c.client_name,
    c.account_id,
    c.account_name,
    c.campaign_id,
    m.campaign_name,
    c.conversion_action_id,
    c.conversion_action_name,
    c.conversion_category,
    c.is_primary,
    c.attribution_setting,
    c.currency,
    cast(c.conversions as numeric(18,6)) as conversions,
    cast(c.conversion_value as numeric(18,6)) as conversion_value,
    f.reporting_currency,
    cast(round(c.conversion_value * f.rate, 6) as numeric(18,6)) as conversion_value_reporting
from {{ ref('int_ad_conversions__unioned') }} as c
join {{ ref('int_ad_platforms__unioned') }} as p on p.platform = c.platform
join {{ ref('int_ad_campaigns__unioned') }} as m
    on m.platform = c.platform and m.account_id = c.account_id and m.campaign_id = c.campaign_id
join {{ ref('int_fx_rates__filled') }} as f on f.rate_date = c.report_date and f.currency = c.currency
