-- stg_demo_search__ads.sql: its ads, as the newest list of each account gives them.
{{ google_ads_ads(newest_list(demo_source('raw_demo_search', 'ads'), 'raw_demo_search', ref('stg_loads__newest_lists')), 'demo_search') }}
