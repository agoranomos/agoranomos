-- stg_demo_web__properties.sql: its properties of web analytics, from the newest load of each.
{{ ga4_properties(newest_list(demo_source('raw_demo_web', 'properties'), 'raw_demo_web', source('core', 'loads'), "split_part(r.name, '/', 2)"), 'demo_web') }}
