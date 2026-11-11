-- The ad accounts of every platform, a view over the core (ADR-0037): the rows of core.dim_account
-- of the platforms whose kind is ads, with their client, currency and time zone, the days that
-- their statistics cover, and whether the settings still name them.
select
    a.platform,
    p.platform_name,
    a.account_id,
    a.account_name,
    a.client_id,
    c.client_name,
    cast(a.currency as text) as currency,
    a.time_zone,
    a.manager_account_id,
    a.account_status,
    a.first_report_date,
    a.last_report_date,
    a.is_active as is_account_active
from {{ ref('dim_account') }} as a
join {{ platforms(ref('seed_ad_platforms')) }}
    on p.platform = a.platform and p.kind = 'ads'
left join {{ ref('dim_client') }} as c
    on c.client_id = a.client_id
