-- stg_demo_video__ad_group_stats.sql: the statistics of its ad groups by day.
{{ tiktok_stats(newest_load(demo_source('raw_demo_video', 'report_adgroup_daily'), 'raw_demo_video', ref('stg_loads__newest_days')), 'demo_video', 'ad_group') }}
