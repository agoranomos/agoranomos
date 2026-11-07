-- stg_demo_search__ad_stats.sql: the statistics of its ads by day.
{{ google_ads_stats(newest_load(demo_source('raw_demo_search', 'ad_stats'), 'raw_demo_search', ref('stg_loads__newest_days')), 'demo_search', 'ad') }}
