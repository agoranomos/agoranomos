-- stg_demo_search__campaigns.sql: the versions of its campaigns.
{{ google_ads_campaigns(source('raw_demo_search', 'campaigns'), 'demo_search') }}
