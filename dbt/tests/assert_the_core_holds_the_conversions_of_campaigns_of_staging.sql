{{ config(tags=['critical']) }}
-- G2 of ADR-0035, after the write: the conversions of the campaigns by action that the core holds
-- on each account and day of the windows of this run add up to what staging reads for them
-- (macros/facts.sql). A row for each account and day whose sums differ: an error, as the write lost
-- or doubled rows, which leaves the run red.
{{ fact_sums_differ(ref('core_ad_conversions_campaign_daily'), [ref('stg_demo_search__campaign_conversions'), ref('stg_demo_social__campaign_conversions'), ref('stg_demo_video__campaign_conversions'), ref('stg_google_ads__campaign_conversions')], ref('int_window_days'), 'account_id', {'platform_conversions': 'conversions', 'all_conversions': 'all_conversions', 'platform_conversion_value': 'conversion_value', 'all_conversion_value': 'all_conversion_value'}) }}
