{{ config(tags=['critical']) }}
-- G2 of ADR-0035, after the write: the sessions of web analytics that the core holds on each
-- account and day of the windows of this run add up to what staging reads for them
-- (macros/facts.sql). A row for each account and day whose sums differ: an error, as the write lost
-- or doubled rows, which leaves the run red.
{{ fact_sums_differ(ref('core_web_sessions_daily'), [ref('stg_demo_web__sessions_daily')], ref('int_window_days'), 'property_id', {'sessions': 'sessions', 'engaged_sessions': 'engaged_sessions', 'key_events': 'key_events', 'total_revenue': 'total_revenue'}) }}
