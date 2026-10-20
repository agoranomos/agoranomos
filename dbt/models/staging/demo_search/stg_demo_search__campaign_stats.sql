-- stg_demo_search__campaign_stats.sql: the statistics of the campaigns of demo_search by day.
{{ google_ads_campaign_stats(source('raw_demo_search', 'campaign_stats'), 'demo_search') }}
