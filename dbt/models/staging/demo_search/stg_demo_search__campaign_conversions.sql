-- stg_demo_search__campaign_conversions.sql: its conversions by campaign, day and action.
{{ google_ads_conversions(source('raw_demo_search', 'campaign_conversions'), 'demo_search') }}
