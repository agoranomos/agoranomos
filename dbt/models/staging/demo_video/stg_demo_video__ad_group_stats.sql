-- stg_demo_video__ad_group_stats.sql: the statistics of its ad groups by day.
{{ tiktok_stats(source('raw_demo_video', 'report_adgroup_daily'), 'demo_video', 'ad_group') }}
