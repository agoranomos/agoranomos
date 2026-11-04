-- stg_demo_search__ad_conversions.sql: the conversions of its ads by day and action.
{{ google_ads_conversions(newest_load(demo_source('raw_demo_search', 'ad_conversions'), 'raw_demo_search', ref('stg_loads__newest_days')), 'demo_search', 'ad') }}
