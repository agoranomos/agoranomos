-- stg_demo_social__campaign_stats.sql: the statistics of the campaigns of demo_social by day.
{{ meta_stats(newest_load(demo_source('raw_demo_social', 'insights_campaign'), 'raw_demo_social', ref('stg_loads__newest_days')), 'demo_social') }}
