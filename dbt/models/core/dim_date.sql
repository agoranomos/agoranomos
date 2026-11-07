-- core.dim_date, the calendar (ADR-0036): one row for each day from 1 January 2000, the first month
-- that the function of the partitions of the core makes a partition of a fact for (ADR-0039), to
-- the end of the year after the current one, in UTC. Each run adds only the days after the last
-- one that the table holds, so the first run writes about ten thousand days and a run in a new
-- year a year more.
{{ config(unique_key='calendar_date') }}
with days as (
    select d::date as calendar_date
    from generate_series(
        (select coalesce(max(calendar_date) + 1, date '2000-01-01') from {{ this }}),
        (date_trunc('year', current_date::timestamp) + interval '2 years' - interval '1 day')::date,
        interval '1 day'
    ) as d
)

select
    calendar_date,
    extract(year from calendar_date)::integer as year,
    extract(quarter from calendar_date)::integer as quarter,
    extract(month from calendar_date)::integer as month,
    date_trunc('month', calendar_date::timestamp)::date as month_start,
    extract(isoyear from calendar_date)::integer as iso_year,
    extract(week from calendar_date)::integer as iso_week,
    date_trunc('week', calendar_date::timestamp)::date as week_start,
    extract(isodow from calendar_date)::integer as day_of_week,
    extract(isodow from calendar_date) in (6, 7) as is_weekend
from days
