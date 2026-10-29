-- The ad accounts of every platform, with their clients: an account that no setting names belongs
-- to the default client (ADR-0024). Its tests check each platform against the list of platforms.
-- depends_on: {{ ref('int_ad_platforms__unioned') }}
with accounts as (
    select * from {{ ref('stg_demo_search__accounts') }}
    union all
    select * from {{ ref('stg_demo_social__accounts') }}
    union all
    select * from {{ ref('stg_demo_video__accounts') }}
    union all
    select * from {{ ref('stg_google_ads__accounts') }}
),

mapped as (
    select
        platform,
        account_id,
        client_id,
        row_number() over (
            partition by platform, account_id order by case origin when 'extensions' then 0 else 1 end
        ) as preference
    from {{ ref('stg_settings__account_clients') }}
)

select
    a.platform,
    a.account_id,
    a.account_name,
    a.currency,
    a.time_zone,
    a.manager_account_id,
    a.account_status,
    c.client_id,
    c.client_name
from accounts as a
left join mapped as m
    on m.platform = a.platform and m.account_id = a.account_id and m.preference = 1
join {{ ref('int_clients__resolved') }} as c
    on c.client_id = coalesce(m.client_id, 'unassigned')
