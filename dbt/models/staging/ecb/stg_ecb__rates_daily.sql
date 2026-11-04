-- stg_ecb__rates_daily.sql: the reference rates of the euro of the ECB by day.
{{ ecb_exr(newest_load(relation_or_empty(source('raw_ecb', 'rates_daily'), {
    'key': 'text', 'freq': 'text', 'currency': 'text', 'currency_denom': 'text',
    'exr_type': 'text', 'exr_suffix': 'text', 'time_period': 'text', 'obs_value': 'text',
    '_dlt_load_id': 'text',
}), 'raw_ecb', ref('stg_loads__newest_days')), 'ecb') }}
