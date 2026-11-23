-- stg_tiktok_ads__campaign_stats.sql: the statistics of the campaigns of tiktok_ads by day.
{{ tiktok_stats(newest_load(relation_or_empty(source('raw_tiktok_ads', 'report_campaign_daily'), {
    'advertiser_id': 'text', 'dimensions__campaign_id': 'text', 'dimensions__stat_time_day': 'text',
    'metrics__campaign_name': 'text', 'metrics__currency': 'text', 'metrics__spend': 'text',
    'metrics__impressions': 'text', 'metrics__clicks': 'text', 'metrics__conversion': 'text',
    'metrics__complete_payment': 'text', 'metrics__total_complete_payment_rate': 'text',
    '_dlt_load_id': 'text', '_dlt_id': 'text',
}), 'raw_tiktok_ads', ref('stg_loads__newest_days')), 'tiktok_ads') }}
