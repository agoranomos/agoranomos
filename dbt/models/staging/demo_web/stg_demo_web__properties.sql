-- stg_demo_web__properties.sql: the properties of web analytics of demo_web.
{{ ga4_properties(source('raw_demo_web', 'properties'), 'demo_web') }}
