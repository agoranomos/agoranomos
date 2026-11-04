-- stg_demo_fx__rates_daily.sql: the reference rates of the euro of demo_fx by day.
{{ euro_rates(newest_load(demo_source('raw_demo_fx', 'rates_daily'), 'raw_demo_fx', ref('stg_loads__newest_days')), 'demo_fx') }}
