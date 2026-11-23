-- stg_tiktok_ads__account_totals.sql: the totals of each advertiser by day, for G0a.
{{ tiktok_totals(newest_load(relation_or_empty(source('raw_tiktok_ads', 'report_advertiser_daily'), {
    'advertiser_id': 'text', 'dimensions__advertiser_id': 'text',
    'dimensions__stat_time_day': 'text', 'metrics__currency': 'text', 'metrics__spend': 'text',
    'metrics__impressions': 'text', 'metrics__clicks': 'text', '_dlt_load_id': 'text',
    '_dlt_id': 'text',
}), 'raw_tiktok_ads', ref('stg_loads__newest_days')), 'tiktok_ads') }}
