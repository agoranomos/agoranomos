-- stg_demo_social__ad_stats.sql: the statistics of its ads by day.
{{ meta_stats(newest_load(demo_source('raw_demo_social', 'insights_ad'), 'raw_demo_social', ref('stg_loads__newest_days')), 'demo_social', 'ad') }}
