-- How far back the loads of each account reach, and how much of its history has come (ADR-0038,
-- "The first load"): from the loads that the runs of the transform pinned into core.loads, the first
-- day of any whole load of the account, and its loads of the kind history, which come in portions
-- by quarter after its usual window. It reads core.loads alone, which the task of the pin writes
-- before the tasks of dbt, so that it shows the progress also when a model fails.
select
    source::text as platform,
    account_id::varchar as account_id,
    min(window_from) as first_day_loaded,
    count(*) filter (where kind = 'history') as portions,
    max(finished_at) filter (where kind = 'history') as last_portion_at
from {{ source('core', 'loads') }}
where outcome = 'loaded'
group by source, account_id
