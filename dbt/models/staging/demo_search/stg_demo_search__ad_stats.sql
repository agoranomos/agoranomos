-- stg_demo_search__ad_stats.sql: the statistics of its ads by day.
{{ google_ads_stats(demo_source('raw_demo_search', 'ad_stats'), 'demo_search', 'ad') }}
