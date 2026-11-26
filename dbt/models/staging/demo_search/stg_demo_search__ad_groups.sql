-- stg_demo_search__ad_groups.sql: its ad groups, as the newest list of each account gives them.
{{ google_ads_ad_groups(newest_list(demo_source('raw_demo_search', 'ad_groups'), 'raw_demo_search', ref('stg_loads__newest_lists')), 'demo_search') }}
