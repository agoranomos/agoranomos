-- stg_demo_video__campaign_stats.sql: the statistics of the campaigns of demo_video by day.
{{ tiktok_stats(newest_load(demo_source('raw_demo_video', 'report_campaign_daily'), 'raw_demo_video', ref('stg_loads__newest_days')), 'demo_video') }}
