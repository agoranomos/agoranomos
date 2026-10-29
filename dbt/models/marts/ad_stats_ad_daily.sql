-- Advertising statistics by day and ad on every platform, with money in the currency of the account
-- and in the reporting currency, and the conversions of the primary actions (ADR-0034).
select
    s.report_date,
    s.platform,
    p.platform_name,
    s.client_id,
    s.client_name,
    s.account_id,
    s.account_name,
    s.campaign_id,
    s.campaign_name,
    s.ad_group_id,
    s.ad_group_name,
    s.ad_id,
    s.ad_name,
    s.currency,
    s.impressions,
    s.clicks,
    cast(s.spend as numeric(18,6)) as spend,
    cast(s.conversions as numeric(18,6)) as conversions,
    cast(s.conversion_value as numeric(18,6)) as conversion_value,
    f.reporting_currency,
    cast(round(s.spend * f.rate, 6) as numeric(18,6)) as spend_reporting,
    cast(round(s.conversion_value * f.rate, 6) as numeric(18,6)) as conversion_value_reporting
from {{ ref('int_ad_stats_ad__unioned') }} as s
join {{ ref('int_ad_platforms__unioned') }} as p on p.platform = s.platform
join {{ ref('int_fx_rates__filled') }} as f on f.rate_date = s.report_date and f.currency = s.currency
