-- stg_demo_social__ad_stats.sql: the statistics of its ads by day.
{{ meta_stats(source('raw_demo_social', 'insights_ad'), 'demo_social', 'ad') }}
