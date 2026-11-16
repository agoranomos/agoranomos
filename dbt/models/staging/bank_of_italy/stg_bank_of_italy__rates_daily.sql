-- stg_bank_of_italy__rates_daily.sql: the exchange rates of the Bank of Italy against the euro by
-- day.
{{ bank_of_italy_rates(newest_load(relation_or_empty(source('raw_bank_of_italy', 'rates_daily'), {
    'country': 'text', 'currency': 'text', 'iso_code': 'text', 'uic_code': 'text',
    'avg_rate': 'text', 'exchange_convention': 'text', 'exchange_convention_code': 'text',
    'reference_date': 'text', '_dlt_load_id': 'text',
}), 'raw_bank_of_italy', ref('stg_loads__newest_days')), 'bank_of_italy') }}
