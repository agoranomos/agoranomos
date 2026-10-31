-- stg_demo_video__campaign_stats.sql: the statistics of the campaigns of demo_video by day.
{{ tiktok_stats(demo_source('raw_demo_video', 'report_campaign_daily'), 'demo_video') }}
