-- stg_demo_search__ad_conversions.sql: the conversions of its ads by day and action.
{{ google_ads_conversions(source('raw_demo_search', 'ad_conversions'), 'demo_search', 'ad') }}
