-- stg_demo_fx__rates_daily.sql: the reference rates of the euro of demo_fx by day.
{{ euro_rates(source('raw_demo_fx', 'rates_daily'), 'demo_fx') }}
