-- stg_demo_search__campaigns.sql: its campaigns, as the newest list of each account gives them.
{{ google_ads_campaigns(newest_list(demo_source('raw_demo_search', 'campaigns'), 'raw_demo_search', source('core', 'loads')), 'demo_search') }}
