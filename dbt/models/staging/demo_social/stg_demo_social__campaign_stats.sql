-- stg_demo_social__campaign_stats.sql: the statistics of the campaigns of demo_social by day.
{{ meta_campaign_stats(source('raw_demo_social', 'insights_campaign'), 'demo_social') }}
