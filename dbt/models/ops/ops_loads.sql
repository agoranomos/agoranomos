-- The last load of each account of each platform, from the journal that each load writes in the
-- same run as its rows (_agoranomos_loads). It reads the journals and not staging, so that it shows
-- the loads also when a model of staging fails. A journal that no load has written, as those of
-- the ECB, of the Bank of Italy, of Google Ads, of Meta, of the NBK and of TikTok, whose DAGs start
-- paused, or of the demo in a deployment without it, is empty. The last day loaded, the last load
-- and the count are of the whole loads, of the kinds window and history, and a row without a kind
-- is of the kind window; the last load of the kind intraday, of the current day of the account,
-- comes beside them, as it takes days still in progress and would hide a daily load that stopped
-- (ADR-0051). The four flags of the answers of a source of web analytics (ADR-0054) are those of
-- the last whole load that took the account, and none for a journal without them, as those of the
-- ad platforms and of the rates. Whether the settings of the model name the account is as
-- core.dim_account records it in each run of the transform, from stg_settings__account_clients:
-- an account that left extensions/model/clients.toml keeps its last load here, as the maintenance
-- keeps the last load of each account, and the card Late accounts of the Ops collection and the
-- alert of late accounts of the watcher leave it out (agoranomos_pipelines/watcher.py). A source
-- of the rates, whose account is the base of its rates, and an account that the registry does not
-- hold yet have none.
with loads as (
    {%- for source_name, platform in [
        ('raw_bank_of_italy', 'bank_of_italy'),
        ('raw_demo_fx', 'demo_fx'),
        ('raw_demo_search', 'demo_search'),
        ('raw_demo_social', 'demo_social'),
        ('raw_demo_video', 'demo_video'),
        ('raw_demo_web', 'demo_web'),
        ('raw_ecb', 'ecb'),
        ('raw_ga4', 'ga4'),
        ('raw_google_ads', 'google_ads'),
        ('raw_meta_ads', 'meta_ads'),
        ('raw_nbk', 'nbk'),
        ('raw_tiktok_ads', 'tiktok_ads'),
    ] %}
    {%- if not loop.first %}
    union all
    {%- endif %}
    select
        '{{ platform }}' as platform,
        account_id,
        window_to,
        loaded_at,
        kind is not distinct from 'intraday' as is_intraday,
        {{ journal_flags(
            load_relation(source(source_name, '_agoranomos_loads')) if execute else none
        ) }}
    from {{ newest_load_table(source_name, '_agoranomos_loads') }}
    {%- endfor %}
)

select
    l.platform,
    l.account_id,
    max(window_to) filter (where not is_intraday) as last_day_loaded,
    max(loaded_at) filter (where not is_intraday) as last_loaded_at,
    -- When the view is read, not when it was built.
    round(
        (extract(epoch from now() - max(loaded_at) filter (where not is_intraday)) / 3600)::numeric,
        1
    ) as hours_since_load,
    count(*) filter (where not is_intraday) as loads,
    max(loaded_at) filter (where is_intraday) as last_intraday_at,
    round(
        (extract(epoch from now() - max(loaded_at) filter (where is_intraday)) / 3600)::numeric, 1
    ) as hours_since_intraday
    {%- for flag in load_flags() %},
    (array_agg({{ flag }} order by loaded_at desc)
        filter (where not is_intraday and loaded_at is not null))[1] as {{ flag }}
    {%- endfor %},
    a.is_active as is_in_settings
from loads as l
left join {{ ref('dim_account') }} as a
    on a.platform = l.platform and a.account_id = l.account_id
group by l.platform, l.account_id, a.is_active
