-- The accounts that the last run of the transform kept out of the core, one row for each account
-- and reason (ADR-0035): what the checks of int_g1_failures found, the loads and the days that the
-- core did not take, and since when the journal core.account_quarantine has held the reason for a
-- load of the account that is still pinned. An account whose next load passes the checks leaves
-- the view with the next run; the journal keeps its rows.
with found as (
    select source, load_id, account_id, reason, checked, detail, window_from, window_to
    from {{ ref('int_g1_failures') }}
),

since as (
    select q.source, q.account_id, q.reason, min(q.found_at) as found_at
    from {{ ref('account_quarantine') }} as q
    join {{ source('core', 'loads') }} as l
        on l.source = q.source and l.load_id = q.load_id and l.account_id = q.account_id
    where l.status = 'pinned'
    group by q.source, q.account_id, q.reason
)

select
    f.source::text as platform,
    f.account_id::varchar as account_id,
    f.reason::text as reason,
    string_agg(distinct f.checked || ': ' || f.detail, '; ')::text as detail,
    count(distinct f.load_id) as loads_held,
    min(f.window_from) as held_from,
    max(f.window_to) as held_to,
    s.found_at as since
from found as f
left join since as s
    on s.source = f.source and s.account_id = f.account_id and s.reason = f.reason
group by f.source, f.account_id, f.reason, s.found_at
