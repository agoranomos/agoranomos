-- The last load of each account of each platform, from the record that each load writes in the same
-- run as its rows (_agoranomos_reloads). It reads the sources only, so that it shows the loads also
-- when a model of staging fails.
with reloads as (
    select 'demo_fx' as platform, account_id, window_end, loaded_at
    from {{ source('raw_demo_fx', '_agoranomos_reloads') }}
    union all
    select 'demo_search' as platform, account_id, window_end, loaded_at
    from {{ source('raw_demo_search', '_agoranomos_reloads') }}
    union all
    select 'demo_social' as platform, account_id, window_end, loaded_at
    from {{ source('raw_demo_social', '_agoranomos_reloads') }}
    union all
    select 'demo_video' as platform, account_id, window_end, loaded_at
    from {{ source('raw_demo_video', '_agoranomos_reloads') }}
    union all
    select 'demo_web' as platform, account_id, window_end, loaded_at
    from {{ source('raw_demo_web', '_agoranomos_reloads') }}
)

select
    platform,
    account_id,
    max(window_end) as last_day_loaded,
    max(loaded_at) as last_loaded_at,
    -- When the view is read, not when it was built.
    round((extract(epoch from now() - max(loaded_at)) / 3600)::numeric, 1) as hours_since_load,
    count(*) as loads
from reloads
group by platform, account_id
