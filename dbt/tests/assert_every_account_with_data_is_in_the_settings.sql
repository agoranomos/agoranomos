-- An account of an ad platform or a property of web analytics that the settings of the model no
-- longer name and whose rows the core keeps (ADR-0044): the statistics of its campaigns or its
-- sessions stay in the core for good, and the views of the marts go on showing them, with the
-- client unassigned and is_account_active false, so that a mistake in the settings erases no
-- history that a platform may no longer serve. A warning, as it is the settings working as meant:
-- an account that the settings name again is active again, and ./agoranomos data forget, planned
-- for v0.9, is to delete its data. It runs with dim_account, which sets the activity of each
-- account from the settings on every run, and reads the facts as the runs before left them, as
-- the checks of G1a do.
{{ config(severity='warn') }}
select a.platform, a.account_id
from {{ ref('dim_account') }} as a
where not a.is_active
    and (
        exists (
            select
            from {{ source('core', 'ad_stats_campaign_daily') }} as f
            where f.account_sk = a.account_sk
        )
        or exists (
            select
            from {{ source('core', 'web_sessions_daily') }} as f
            where f.account_sk = a.account_sk
        )
    )
