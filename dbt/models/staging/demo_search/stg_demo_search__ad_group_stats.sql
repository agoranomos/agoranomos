-- stg_demo_search__ad_group_stats.sql: the statistics of its ad groups by day.
{{ google_ads_stats(demo_source('raw_demo_search', 'ad_group_stats'), 'demo_search', 'ad_group') }}
