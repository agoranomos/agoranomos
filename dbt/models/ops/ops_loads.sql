-- The last load of each account of each platform, from the journal that each load writes in the
-- same run as its rows (_agoranomos_loads). It reads the sources only, so that it shows the loads
-- also when a model of staging fails. A journal that no load has written, as those of the ECB and
-- of Google Ads, whose DAGs start paused, or of the demo in a deployment without it, is empty.
with loads as (
    {%- for source_name, platform in [
        ('raw_demo_fx', 'demo_fx'),
        ('raw_demo_search', 'demo_search'),
        ('raw_demo_social', 'demo_social'),
        ('raw_demo_video', 'demo_video'),
        ('raw_demo_web', 'demo_web'),
        ('raw_ecb', 'ecb'),
        ('raw_google_ads', 'google_ads'),
    ] %}
    {%- if not loop.first %}
    union all
    {%- endif %}
    select '{{ platform }}' as platform, account_id, window_to, loaded_at
    from {{ newest_load_table(source_name, '_agoranomos_loads') }}
    {%- endfor %}
)

select
    platform,
    account_id,
    max(window_to) as last_day_loaded,
    max(loaded_at) as last_loaded_at,
    -- When the view is read, not when it was built.
    round((extract(epoch from now() - max(loaded_at)) / 3600)::numeric, 1) as hours_since_load,
    count(*) as loads
from loads
group by platform, account_id
