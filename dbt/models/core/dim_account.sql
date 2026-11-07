-- core.dim_account, the registry of accounts (ADR-0036, ADR-0038): one row for each account of an
-- ad platform and each property of web analytics that the data of a run named, with its surrogate
-- key account_sk, which the facts of the core are to carry. The model merges into the table of the
-- migrations (dbt_project.yml), and only the rows that are new or that changed.
-- * A new account takes the next value of the sequence of the table, and an account keeps its key
--   for good: dbt writes every column of a table that it merges into, which an identity column
--   refuses, so the key is coalesce(the key that the table holds, nextval), and the merge leaves it
--   out of what it updates.
-- * An account that a list gives takes its attributes from the newest list; one that this run
--   reads from its data only keeps those that a list gave it before, and stays a stub until a list
--   gives it (int_accounts__current).
-- * Every run gives every row, those of the accounts that this run does not read too, the client
--   and the activity that the settings of the model say now: an account that leaves the settings
--   keeps its data and is no longer active (ADR-0044).
-- * The first and the last day of statistics only widen: this run reads the loads that the core has
--   not accepted (ADR-0035), so a day that the table holds stays in it.
{{ config(unique_key=['platform', 'account_id'], merge_exclude_columns=['account_sk']) }}
with existing as (
    select * from {{ this }}
),

merged as (
    select
        coalesce(c.platform, d.platform) as platform,
        coalesce(c.account_id, d.account_id) as account_id,
        d.account_sk,
        case when c.is_stub is false then c.account_name else d.account_name end as account_name,
        case when c.is_stub is false then c.currency else d.currency end as currency,
        case when c.is_stub is false then c.time_zone else d.time_zone end as time_zone,
        case when c.is_stub is false then c.manager_account_id else d.manager_account_id end
            as manager_account_id,
        case when c.is_stub is false then c.account_status else d.account_status end
            as account_status,
        coalesce(c.is_stub, true) and coalesce(d.is_stub, true) as is_stub,
        least(d.first_report_date, c.first_report_date) as first_report_date,
        greatest(d.last_report_date, c.last_report_date) as last_report_date
    from {{ ref('int_accounts__current') }} as c
    full join existing as d
        on d.platform = c.platform and d.account_id = c.account_id
),

-- The client of each account that the settings name, the deployment's where both name one.
mapped as (
    select
        platform,
        account_id,
        client_id,
        row_number() over (
            partition by platform, account_id order by case origin when 'extensions' then 0 else 1 end
        ) as preference
    from {{ ref('stg_settings__account_clients') }}
),

keyed as (
    select
        coalesce(m.account_sk, nextval('core.dim_account_account_sk_seq'))::integer as account_sk,
        m.platform::text as platform,
        m.account_id::text as account_id,
        m.account_name::text as account_name,
        coalesce(s.client_id, 'unassigned')::text as client_id,
        m.currency::char(3) as currency,
        m.time_zone::text as time_zone,
        m.manager_account_id::text as manager_account_id,
        m.account_status::text as account_status,
        -- Every platform of the model has the three levels of the core (ADR-0036).
        cast(null as text) as hierarchy_rule,
        s.client_id is not null as is_active,
        m.is_stub,
        m.first_report_date,
        m.last_report_date
    from merged as m
    left join mapped as s
        on s.platform = m.platform and s.account_id = m.account_id and s.preference = 1
)

select k.*
from keyed as k
left join existing as d
    on d.platform = k.platform and d.account_id = k.account_id
where d.account_sk is null
    or (
        k.account_name, k.client_id, k.currency, k.time_zone, k.manager_account_id,
        k.account_status, k.hierarchy_rule, k.is_active, k.is_stub, k.first_report_date,
        k.last_report_date
    ) is distinct from (
        d.account_name, d.client_id, d.currency, d.time_zone, d.manager_account_id,
        d.account_status, d.hierarchy_rule, d.is_active, d.is_stub, d.first_report_date,
        d.last_report_date
    )
