-- stg_demo_video__ad_stats.sql: the statistics of its ads by day.
{{ tiktok_stats(source('raw_demo_video', 'report_ad_daily'), 'demo_video', 'ad') }}
