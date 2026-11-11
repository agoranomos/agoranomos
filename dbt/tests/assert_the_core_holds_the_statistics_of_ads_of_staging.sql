{{ config(tags=['critical']) }}
-- G2 of ADR-0035, after the write: the statistics of the ads that the core holds on each account
-- and day of the windows of this run add up to what staging reads for them (macros/facts.sql). A
-- row for each account and day whose sums differ: an error, as the write lost or doubled rows,
-- which leaves the run red.
{{ fact_sums_differ(ref('core_ad_stats_ad_daily'), [ref('stg_demo_search__ad_stats'), ref('stg_demo_social__ad_stats'), ref('stg_demo_video__ad_stats'), ref('stg_google_ads__ad_stats')], ref('int_window_days'), 'account_id', {'impressions': 'impressions', 'clicks': 'clicks', 'spend': 'spend', 'platform_conversions': 'conversions', 'platform_conversion_value': 'conversion_value', 'all_conversions': 'all_conversions', 'all_conversion_value': 'all_conversion_value'}) }}
