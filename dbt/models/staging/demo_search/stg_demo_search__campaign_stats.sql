-- stg_demo_search__campaign_stats.sql: the statistics of the campaigns of demo_search by day.
{{ google_ads_stats(newest_load(demo_source('raw_demo_search', 'campaign_stats'), 'raw_demo_search', ref('stg_loads__newest_days')), 'demo_search') }}
