-- core.account_quarantine, the journal of quarantine (ADR-0035): one row for each load, account and
-- reason for which the checks of this run, int_g1_failures, kept the window of an account out of
-- the core, with what they found in each kind of data. The model merges into the table of the
-- migrations (dbt_project.yml), and only the rows that the table does not hold yet: a load that
-- stays pinned is checked again by every run until the core accepts it, and its row keeps the
-- moment and the run of dbt that first found it. A failure with no load, of an account that a list
-- gives and no load names, has no row here; int_g1_failures keeps it out of the core all the same.
-- Written before the dimensions, which wait for it (int_accounts__current).
{{ config(unique_key=['source', 'load_id', 'account_id', 'reason']) }}
with existing as (
    select source, load_id, account_id, reason from {{ this }}
),

found as (
    select
        source,
        load_id,
        account_id,
        reason,
        string_agg(checked || ': ' || detail, '; ' order by checked, detail) as detail,
        min(window_from) as window_from,
        max(window_to) as window_to
    from {{ ref('int_g1_failures') }}
    where load_id is not null
    group by source, load_id, account_id, reason
)

select
    f.source::text as source,
    f.load_id::text as load_id,
    f.account_id::text as account_id,
    f.reason::text as reason,
    f.detail::text as detail,
    f.window_from::date as window_from,
    f.window_to::date as window_to,
    '{{ invocation_id }}'::uuid as invocation_id,
    now()::timestamptz as found_at
from found as f
left join existing as q
    on q.source = f.source and q.load_id = f.load_id and q.account_id = f.account_id
    and q.reason = f.reason
where q.source is null
