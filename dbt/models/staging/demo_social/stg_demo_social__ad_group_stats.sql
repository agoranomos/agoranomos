-- stg_demo_social__ad_group_stats.sql: the statistics of its ad groups by day.
{{ meta_stats(demo_source('raw_demo_social', 'insights_adset'), 'demo_social', 'ad_group') }}
