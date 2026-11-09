{#- The facts of the core (ADR-0036, ADR-0038): each run of the transform writes, into each fact,
    the days of the windows that it pinned, whole, by account and day. A model of a fact is the
    standard incremental strategy delete+insert of dbt, with unique_key account_sk and report_date,
    so that a day of an account is deleted whole and its rows of the newest load go in, and with
    pre-hooks in the same transaction, which commits with the write or not at all:
    * the thaw of the freeze, when the write is to replace rows of a frozen month (fact_thaw);
    * for the facts of statistics, the journal of the rows that the write changes or removes, with
      their old values (fact_restatements);
    * the days of the window that the load does not hold, which the strategy alone would leave as
      they were (fact_empty_days).
    No hook runs before the transaction: a hook before_begin makes dbt commit before it, and the
    hooks and the write after it then commit one by one (#191). The partitions of the months of the
    windows come from the task of the pin, outside any model (pin_loads.sql). The rows come from
    staging, of the days of int_window_days, which leaves out the accounts in quarantine and the
    sources that stop (ADR-0035), as integers, numeric(18,6) and char(3), the types of the tables
    of the migrations; staging has left out a row whose measures are all zero (ADR-0036,
    macros/numbers.sql). -#}

{#- The first day that the pinned windows reach, as SQL: the lower bound of the delete of a fact,
    its incremental_predicates, and of the hooks and tests of the facts, so that PostgreSQL reads
    only the partitions of the windows. A subquery on core.loads, which PostgreSQL computes when the
    statement runs: dbt renders a config, and the where of a test, when it parses the project,
    with no database, and its parser of tests refuses a macro of the project in a where, so the
    YAML of the facts writes the same text (#191). -#}
{% macro window_start() -%}
(select min(window_from) from core.loads where status = 'pinned' and outcome = 'loaded')
{%- endmacro %}

{#- The first day of a month that is not frozen, as the trigger freeze_the_months_before_the_window
    of each fact computes it (0005_create_the_facts.sql), in the time zone of the session. -#}
{% macro frozen_before() -%}
date_trunc('month', current_date - 92)::date
{%- endmacro %}

{#- The grain of each fact: the columns of its key between the account and the day, the other
    IDs of a row, its measures, and whether it has a journal of restatements; a row is compared
    with its replacement on the IDs, the measures and the currency. -#}
{% macro fact_grains() -%}
{%- set stats = [
    'impressions', 'clicks', 'spend', 'conversions', 'all_conversions', 'conversion_value',
    'all_conversion_value',
] -%}
{{- return({
    'ad_stats_campaign_daily': (['campaign_id'], [], stats, true),
    'ad_stats_ad_group_daily': (['ad_group_id'], ['campaign_id'], stats, true),
    'ad_stats_ad_daily': (['ad_group_id', 'ad_id'], ['campaign_id'], stats, true),
    'ad_conversions_campaign_daily': (
        ['campaign_id', 'conversion_action_sk'],
        ['attribution_setting'],
        ['conversions', 'all_conversions', 'conversion_value', 'all_conversion_value'],
        false,
    ),
    'web_sessions_daily': (
        ['session_source', 'session_medium', 'session_campaign_name', 'session_campaign_id'],
        ['session_default_channel_group'],
        ['sessions', 'engaged_sessions', 'key_events', 'total_revenue'],
        false,
    ),
}) -}}
{%- endmacro %}

{#- The source, the account and the day of a row as one key, which int_window_days holds as
    day_key: the day goes in as its number of days since 2000, which reads the same in every
    session, as in newest_load_key. -#}
{% macro window_day_key(source, account, day) -%}
{{ source }} || '|' || {{ account }} || '|' || ({{ day }} - date '2000-01-01')::text
{%- endmacro %}

{#- The sources whose loads a fact takes, as core.loads names them: those of pinned_load_facts. -#}
{% macro fact_sources(fact) -%}
    {%- set found = [] -%}
    {%- for source_name, tables in pinned_load_facts().items() -%}
        {%- if fact in tables -%}
            {%- do found.append(source_name[4:]) -%}
        {%- endif -%}
    {%- endfor -%}
    {{- return(found) -}}
{%- endmacro %}

{#- The days of the windows that this run writes into a fact, from days, int_window_days. -#}
{% macro fact_days(fact, days) -%}
(select account_sk, report_date, load_sk
from {{ days }}
where source in ('{{ fact_sources(fact) | join("', '") }}')) as days
{%- endmacro %}

{#- The rows of a fact of statistics of a level, campaign, ad_group or ad, that this run writes:
    those of relations, the staging models of the level of each ad platform, on the days of days,
    with the own metric of conversions of each platform. No connector reads all_conversions at the
    level of the statistics, so it is none here; core.ad_conversions_campaign_daily has it by
    action. -#}
{% macro fact_stats_rows(level, relations, days) -%}
{%- set ids = {
    'campaign': ['campaign_id'],
    'ad_group': ['campaign_id', 'ad_group_id'],
    'ad': ['campaign_id', 'ad_group_id', 'ad_id'],
}[level] -%}
with read as (
    {%- for relation in relations %}
    {%- if not loop.first %}
    union all
    {%- endif %}
    select
        platform,
        account_id,
        report_date,
        {{ ids | join(', ') }},
        impressions,
        clicks,
        spend,
        platform_conversions,
        platform_conversion_value
    from {{ relation }}
    {%- endfor %}
)

select
    cast(d.account_sk as integer) as account_sk,
    {%- for id in ids %}
    cast(r.{{ id }} as bigint) as {{ id }},
    {%- endfor %}
    cast(r.report_date as date) as report_date,
    cast(r.impressions as bigint) as impressions,
    cast(r.clicks as bigint) as clicks,
    cast(r.spend as numeric(18,6)) as spend,
    cast(r.platform_conversions as numeric(18,6)) as conversions,
    cast(null as numeric(18,6)) as all_conversions,
    cast(r.platform_conversion_value as numeric(18,6)) as conversion_value,
    cast(null as numeric(18,6)) as all_conversion_value,
    cast(d.currency as char(3)) as currency,
    cast(d.load_sk as integer) as load_sk
from read as r
join {{ days }} as d
    on d.day_key = {{ window_day_key('r.platform', 'r.account_id', 'r.report_date') }}
{%- endmacro %}

{#- The rows of the fact of the conversions of the campaigns by action that this run writes: those
    of relations, the staging models of the conversions of the campaigns of each ad platform, on the
    days of days, with the surrogate of the action from actions, core.dim_conversion_action. -#}
{% macro fact_conversions_rows(relations, days, actions) -%}
with read as (
    {%- for relation in relations %}
    {%- if not loop.first %}
    union all
    {%- endif %}
    select
        platform,
        account_id,
        report_date,
        campaign_id,
        conversion_action_id,
        attribution_setting,
        platform_conversions,
        all_conversions,
        platform_conversion_value,
        all_conversion_value
    from {{ relation }}
    {%- endfor %}
)

select
    cast(d.account_sk as integer) as account_sk,
    cast(r.campaign_id as bigint) as campaign_id,
    cast(a.conversion_action_sk as integer) as conversion_action_sk,
    cast(r.report_date as date) as report_date,
    cast(r.platform_conversions as numeric(18,6)) as conversions,
    cast(r.all_conversions as numeric(18,6)) as all_conversions,
    cast(r.platform_conversion_value as numeric(18,6)) as conversion_value,
    cast(r.all_conversion_value as numeric(18,6)) as all_conversion_value,
    cast(r.attribution_setting as text) as attribution_setting,
    cast(d.currency as char(3)) as currency,
    cast(d.load_sk as integer) as load_sk
from read as r
join {{ days }} as d
    on d.day_key = {{ window_day_key('r.platform', 'r.account_id', 'r.report_date') }}
join {{ actions }} as a
    on a.platform = r.platform and a.conversion_action_id = r.conversion_action_id
{%- endmacro %}

{#- The rows of the fact of the sessions of web analytics that this run writes: those of relations,
    the staging models of the sessions of each source of web analytics, on the days of days. -#}
{% macro fact_sessions_rows(relations, days) -%}
with read as (
    {%- for relation in relations %}
    {%- if not loop.first %}
    union all
    {%- endif %}
    select
        platform,
        property_id,
        report_date,
        session_source,
        session_medium,
        session_campaign_name,
        session_campaign_id,
        session_default_channel_group,
        sessions,
        engaged_sessions,
        key_events,
        total_revenue
    from {{ relation }}
    {%- endfor %}
)

select
    cast(d.account_sk as integer) as account_sk,
    cast(r.session_source as text) as session_source,
    cast(r.session_medium as text) as session_medium,
    cast(r.session_campaign_name as text) as session_campaign_name,
    cast(r.session_campaign_id as text) as session_campaign_id,
    cast(r.report_date as date) as report_date,
    cast(r.session_default_channel_group as text) as session_default_channel_group,
    cast(r.sessions as bigint) as sessions,
    cast(r.engaged_sessions as bigint) as engaged_sessions,
    cast(r.key_events as numeric(18,6)) as key_events,
    cast(r.total_revenue as numeric(18,6)) as total_revenue,
    cast(d.currency as char(3)) as currency,
    cast(d.load_sk as integer) as load_sk
from read as r
join {{ days }} as d
    on d.day_key = {{ window_day_key('r.platform', 'r.property_id', 'r.report_date') }}
{%- endmacro %}

{#- The first pre-hook of a fact: the freeze (ADR-0035) lifted for the transaction of the model,
    only when the write is to replace rows that the fact holds in a frozen month, as when a load
    of history comes again; the first history of an account inserts, which the freeze lets in. The
    setting holds the ID of the transaction, so it lifts nothing after it. -#}
{% macro fact_thaw(days) -%}
select set_config('agoranomos.thaw', pg_current_xact_id()::text, true)
where exists (
    select
    from {{ this }} as f
    join {{ fact_days(this.identifier, days) }}
        on days.account_sk = f.account_sk and days.report_date = f.report_date
    where f.report_date < {{ frozen_before() }}
        and f.report_date >= {{ window_start() }}
)
{%- endmacro %}

{#- The second pre-hook of a fact of statistics: the journal of restatements (ADR-0036). Each row
    that the fact holds on a day of the window whose replacement, in rows, the rows that the model
    writes, has other values, or has no row of its key, goes into the journal with its old values,
    the load that wrote it and the one that replaces it, the moment of the transaction, and
    whether it vanished. A row that the replacement holds with the same values goes nowhere. -#}
{% macro fact_restatements(rows, days) -%}
{#- dbt renders the hooks when it parses the project too, before this names the table. -#}
{%- if not execute -%}
    {{- return('') -}}
{%- endif -%}
{%- set key, others, measures, _ = fact_grains()[this.identifier] -%}
{%- set kept = ['account_sk'] + others + key + ['report_date'] + measures + ['currency', 'load_sk'] -%}
{%- set compared = others + measures + ['currency'] -%}
insert into {{ this.schema }}.{{ this.identifier }}_restatements (
    {{ kept | join(', ') }}, new_load_sk, replaced_at, is_vanished
)
select
    {%- for column in kept %}
    f.{{ column }},
    {%- endfor %}
    days.load_sk,
    now(),
    n.account_sk is null
from {{ this }} as f
join {{ fact_days(this.identifier, days) }}
    on days.account_sk = f.account_sk and days.report_date = f.report_date
left join ({{ rows }}) as n
    on n.account_sk = f.account_sk
    and n.report_date = f.report_date
    {%- for column in key %}
    and n.{{ column }} = f.{{ column }}
    {%- endfor %}
where f.report_date >= {{ window_start() }}
    and (
        n.account_sk is null
        or (n.{{ compared | join(', n.') }}) is distinct from (f.{{ compared | join(', f.') }})
    )
{%- endmacro %}

{#- The last pre-hook of a fact: the days of the window on which the rows that the model writes,
    rows, hold nothing, as when a platform no longer reports an account on a day, deleted whole,
    as delete+insert deletes only the days that it inserts. The days are found first, from the
    days of the window and the distinct accounts and days of the rows: a delete by not exists over
    the rows hashed every row of the window (#191). -#}
{% macro fact_empty_days(rows, days) -%}
delete from {{ this }} as f
using (
    select days.account_sk, days.report_date
    from {{ fact_days(this.identifier, days) }}
    left join (select distinct account_sk, report_date from ({{ rows }}) as r) as n
        on n.account_sk = days.account_sk and n.report_date = days.report_date
    where n.account_sk is null
) as empty
where f.account_sk = empty.account_sk
    and f.report_date = empty.report_date
    and f.report_date >= {{ window_start() }}
{%- endmacro %}

{#- G2 of ADR-0035, after the write: for each account and day of the window of a fact, the sums of
    the measures of the rows of staging, relations, against those of the fact, fact; a row for each
    account and day where one differs. measures maps each column of staging to the column of the
    fact that takes it, and account names the account in staging. The rows of an account in
    quarantine are on neither side, as days, int_window_days, leaves the account out. The two sides
    go into one aggregate, the fact with its sign turned, rather than into a join, whose estimate
    on two columns the planner can take for one row. -#}
{% macro fact_sums_differ(fact, relations, days, account, measures) -%}
with window_days as (
    select account_sk, report_date, day_key
    from {{ days }}
    where source in ('{{ fact_sources(fact.identifier) | join("', '") }}')
),

staged as (
    {%- for relation in relations %}
    {%- if not loop.first %}
    union all
    {%- endif %}
    select
        platform,
        {{ account }} as account_id,
        report_date,
        {{ measures.keys() | join(', ') }}
    from {{ relation }}
    {%- endfor %}
),

both_sides as (
    select
        d.account_sk,
        d.report_date,
        {%- for column, taken in measures.items() %}
        s.{{ column }} as {{ taken }}{% if not loop.last %},{% endif %}
        {%- endfor %}
    from staged as s
    join window_days as d
        on d.day_key = {{ window_day_key('s.platform', 's.account_id', 's.report_date') }}
    union all
    select
        f.account_sk,
        f.report_date,
        {%- for taken in measures.values() %}
        -f.{{ taken }}{% if not loop.last %},{% endif %}
        {%- endfor %}
    from {{ fact }} as f
    where f.report_date >= {{ window_start() }}
        and exists (
            select
            from window_days as d
            where d.account_sk = f.account_sk and d.report_date = f.report_date
        )
)

select
    account_sk,
    report_date,
    {%- for taken in measures.values() %}
    coalesce(sum({{ taken }}), 0) as {{ taken }}_difference{% if not loop.last %},{% endif %}
    {%- endfor %}
from both_sides
group by account_sk, report_date
having
    {%- for taken in measures.values() %}
    coalesce(sum({{ taken }}), 0) <> 0{% if not loop.last %} or{% endif %}
    {%- endfor %}
{%- endmacro %}
