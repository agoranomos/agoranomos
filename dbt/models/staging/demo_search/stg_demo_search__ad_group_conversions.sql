-- stg_demo_search__ad_group_conversions.sql: the conversions of its ad groups by day and action.
{{ google_ads_conversions(newest_load(demo_source('raw_demo_search', 'ad_group_conversions'), 'raw_demo_search', ref('stg_loads__newest_days')), 'demo_search', 'ad_group') }}
