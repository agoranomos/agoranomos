-- stg_demo_social__ad_group_stats.sql: the statistics of its ad groups by day.
{{ meta_stats(newest_load(demo_source('raw_demo_social', 'insights_adset'), 'raw_demo_social', ref('stg_loads__newest_days')), 'demo_social', 'ad_group') }}
