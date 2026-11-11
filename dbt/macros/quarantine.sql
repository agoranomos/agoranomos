{#- The checks of G0a, G1 and G1a of ADR-0035, which each run of the transform makes on the data of
    each account that it reads, before it writes the core: the model int_g1_failures runs them, one
    statement for each kind of data, and holds a row for each load, account, reason and kind of
    data that failed. An account with a row there stays out of the core in that run: its window
    is not written, its dimensions and its facts keep what they held, and core.account_quarantine
    records why (ADR-0035, "Quarantine"). The same filter keeps it out of the marts of the old path
    until #201 serves the marts from the core. -#}

{#- The reasons of quarantine, as codes, in the order of the checks: the newest pinned load of the
    account failed for it; the loads name an account that no list gives; two rows share a key, or
    a list gives the account twice; a row misses a part of its key; the currency of the account is
    not one of ISO 4217; its time zone is not one of IANA; the source of the rates has no rate of
    its currency into the reporting currency (ADR-0042); a spend has more than six decimal places;
    a measure is missing or below zero; a load holds rows outside its window; the rows of the
    campaigns of a load do not add up to the totals of the account that the platform gives in the
    same load (G0a); a load has no row on the days of its window while the core holds spend on
    them (G1a); the spend of a load fell by more than var('quarantine_spend_fall') against the core
    (G1a); more than half of the pinned accounts of the source are in quarantine, so the source
    stops. -#}
{% macro quarantine_reasons() -%}
{{- return([
    'load_failed', 'unknown_account', 'duplicate_key', 'missing_key', 'unknown_currency',
    'unknown_time_zone', 'missing_rate', 'too_many_decimals', 'missing_measure',
    'negative_measure', 'date_outside_window', 'totals_differ', 'empty_window', 'spend_fall',
    'source_stopped',
]) -}}
{%- endmacro %}

{#- The sources whose accounts the checks read: those of newest_load_sources whose rows name an
    account, the ad platforms and web analytics. The rates have one account each, the base of the
    rates, and an account of an ad platform or a property whose currency they lack goes to
    quarantine instead (quarantine_rate_findings). -#}
{% macro quarantine_sources() -%}
    {%- set found = [] -%}
    {%- for source_name, keys in newest_load_sources().items() -%}
        {%- if keys[0] is not none -%}
            {%- do found.append(source_name[4:]) -%}
        {%- endif -%}
    {%- endfor -%}
    {{- return(found) -}}
{%- endmacro %}

{#- The kinds of facts of staging that G1 checks, as the tests of staging checked them before
    (tests/unit/test_dbt_project.py), with the measures that the facts of the core take, whose
    columns are NOT NULL or zero or more there, so that one account cannot fail the write of a
    fact for all: the column of the account; the columns of the key after the day, the platform
    and the account; the other IDs that a row needs; the measures that a row needs; and the
    measures that are zero or more. -#}
{% macro quarantine_fact_kinds() -%}
{%- set metrics = ['impressions', 'clicks', 'spend'] -%}
{%- set counted = [
    'impressions', 'clicks', 'spend', 'platform_conversions', 'platform_conversion_value',
    'all_conversions', 'all_conversion_value',
] -%}
{%- set converted = ['conversions', 'conversion_value', 'platform_conversions'] -%}
{%- set valued = [
    'conversions', 'conversion_value', 'platform_conversions', 'platform_conversion_value',
    'all_conversions', 'all_conversion_value',
] -%}
{{- return({
    'campaign_stats': ('account_id', ['campaign_id'], [], metrics, counted),
    'ad_group_stats': ('account_id', ['ad_group_id'], ['campaign_id'], metrics, counted),
    'ad_stats': ('account_id', ['ad_group_id', 'ad_id'], ['campaign_id'], metrics, counted),
    'campaign_conversions': (
        'account_id', ['campaign_id', 'conversion_action_id'], [], converted, valued
    ),
    'ad_group_conversions': (
        'account_id', ['ad_group_id', 'conversion_action_id'], ['campaign_id'], converted,
        valued
    ),
    'ad_conversions': (
        'account_id', ['ad_group_id', 'ad_id', 'conversion_action_id'], ['campaign_id'],
        converted, valued
    ),
    'sessions_daily': (
        'property_id',
        ['session_source', 'session_medium', 'session_campaign_name', 'session_campaign_id'],
        [],
        ['sessions', 'engaged_sessions'],
        ['sessions', 'engaged_sessions', 'key_events', 'total_revenue'],
    ),
}) -}}
{%- endmacro %}

{#- The fact tables of raw that each source appends by load, which the checks of the windows and
    of the decimal places read as they are: each table with the column of its spend as text, and
    whether that spend is in micros, as cost_micros of Google is. A table of conversions or of
    sessions has no spend. The account and the day of a row are those of newest_load_sources. The
    connector of Google Ads also loads the totals of each account, which demo_search does not. -#}
{% macro quarantine_raw_tables() -%}
{%- set google = [
    ('campaign_stats', 'metrics__cost_micros', true),
    ('campaign_conversions', none, false),
    ('ad_group_stats', 'metrics__cost_micros', true),
    ('ad_group_conversions', none, false),
    ('ad_stats', 'metrics__cost_micros', true),
    ('ad_conversions', none, false),
] -%}
{{- return({
    'raw_demo_search': google,
    'raw_demo_social': [
        ('insights_campaign', 'spend', false),
        ('insights_adset', 'spend', false),
        ('insights_ad', 'spend', false),
    ],
    'raw_demo_video': [
        ('report_campaign_daily', 'metrics__spend', false),
        ('report_adgroup_daily', 'metrics__spend', false),
        ('report_ad_daily', 'metrics__spend', false),
    ],
    'raw_demo_web': [('report_sessions_daily', none, false)],
    'raw_google_ads': [
        ('campaign_stats', 'metrics__cost_micros', true),
        ('campaign_conversions', none, false),
        ('ad_group_stats', 'metrics__cost_micros', true),
        ('ad_group_conversions', none, false),
        ('ad_stats', 'metrics__cost_micros', true),
        ('ad_conversions', none, false),
        ('customer_stats', 'metrics__cost_micros', true),
    ],
}) -}}
{%- endmacro %}

{#- The columns of the table of the failures, in their order, as each statement inserts them. -#}
{% macro quarantine_columns() -%}
{{- return([
    'source', 'load_id', 'account_id', 'reason', 'checked', 'detail', 'window_from', 'window_to',
]) -}}
{%- endmacro %}

{#- One statement of the checks: the rows of a query of failures into the table of the failures. -#}
{% macro quarantine_insert(failures, query) -%}
insert into {{ failures }} ({{ quarantine_columns() | join(', ') }})
{{ query }}
{%- endmacro %}

{#- The failures that a query of checks found, with the load of each and its window: found gives
    the source, the account, the load where the check knows it, the reason, what it checked and
    what it found. A failure of the account as a whole, such as its currency, takes the newest load
    that this run reads for the account, from days, the table stg_loads__newest_days, or else the
    newest load of the account that a run pinned; a list can name an account that no load names,
    and its failure then has no load. loads is core.loads. -#}
{% macro quarantine_attributed(found, days, loads) -%}
with found as (
    {{ found }}
),

newest_read as (
    select distinct on (source, account_id) source, account_id, load_id
    from {{ days }}
    order by source, account_id, load_id::numeric desc
),

newest_pinned as (
    select distinct on (source, account_id) source, account_id, load_id
    from {{ loads }}
    where status = 'pinned'
    order by source, account_id, load_id::numeric desc
),

attributed as (
    select
        f.source,
        coalesce(f.load_id, r.load_id, p.load_id) as load_id,
        f.account_id,
        f.reason,
        f.checked,
        f.detail
    from found as f
    left join newest_read as r
        on r.source = f.source and r.account_id = f.account_id
    left join newest_pinned as p
        on p.source = f.source and p.account_id = f.account_id
)

select
    a.source::text as source,
    a.load_id::text as load_id,
    a.account_id::text as account_id,
    a.reason::text as reason,
    a.checked::text as checked,
    a.detail::text as detail,
    l.window_from,
    l.window_to
from attributed as a
left join {{ loads }} as l
    on l.source = a.source and l.load_id = a.load_id and l.account_id = a.account_id
{%- endmacro %}

{#- The checks of the accounts as a whole: the newest pinned load of an account that failed for
    it, an account that a list gives twice, a currency outside ISO 4217 or a time zone outside
    IANA, which a cast to char(3) would cut silently in the core (ADR-0038), and an account that
    the pinned loads name and no list gives, which the core cannot convert or place in a day
    (ADR-0035, "a known account for each fact"). lists are the staging models of the accounts and
    the properties, each with the column of its account; currencies and time_zones the seeds. -#}
{% macro quarantine_account_findings(lists, currencies, time_zones, days, loads) -%}
with listed as (
    {%- for relation, account in lists %}
    {%- if not loop.first %}
    union all
    {%- endif %}
    select platform as source, {{ account }} as account_id, currency, time_zone
    from {{ relation }}
    {%- endfor %}
),

accounts as (
    select
        l.source,
        l.account_id,
        count(*) as listings,
        bool_or(c.currency_code is null) as unknown_currency,
        min(coalesce(l.currency, 'none')) filter (where c.currency_code is null) as currency,
        bool_or(z.time_zone is null) as unknown_time_zone,
        min(coalesce(l.time_zone, 'none')) filter (where z.time_zone is null) as time_zone
    from listed as l
    left join {{ currencies }} as c
        on c.currency_code = l.currency
    left join {{ time_zones }} as z
        on z.time_zone = l.time_zone
    group by l.source, l.account_id
),

newest_pinned as (
    select distinct on (source, account_id) source, account_id, load_id, outcome, error
    from {{ loads }}
    where status = 'pinned'
        and source in ('{{ quarantine_sources() | join("', '") }}')
    order by source, account_id, load_id::numeric desc
),

named as (
    select distinct source, account_id
    from {{ days }}
    where source in ('{{ quarantine_sources() | join("', '") }}')
)

select
    source,
    account_id,
    load_id,
    'load_failed' as reason,
    'loads' as checked,
    coalesce(error, 'the load recorded no error') as detail
from newest_pinned
where outcome = 'failed'
union all
select
    n.source,
    n.account_id,
    null,
    'unknown_account',
    'accounts',
    'no list of accounts or properties gives the account'
from named as n
left join accounts as a
    on a.source = n.source and a.account_id = n.account_id
where a.account_id is null
union all
select source, account_id, null, 'duplicate_key', 'accounts', 'listed ' || listings || ' times'
from accounts
where listings > 1
union all
select source, account_id, null, 'unknown_currency', 'accounts', currency
from accounts
where unknown_currency
union all
select source, account_id, null, 'unknown_time_zone', 'accounts', time_zone
from accounts
where unknown_time_zone
{%- endmacro %}

{#- The check of the rates (ADR-0042): an account whose currency has no rate into the reporting
    currency at the source of the rates, neither among the rates of the loads that this run reads
    nor in core.fx_rates_daily as the runs before this one left it, so that the core takes no fact
    that a view could not convert, and no rate of 1 or 0 stands for a missing one. Only the
    accounts that the pinned loads of this run name, as only they write the core; a currency
    outside ISO 4217 has a reason of its own, and the reporting currency needs no rate. A day of a
    currency that has a rate but none on or before that day is a gap, which the test of G3 finds
    after the write of the rates. lists are the staging models of the accounts and the properties,
    each with the column of its account; currencies the seed of ISO 4217; days the table
    stg_loads__newest_days; ecb and demo the staging models of the rates of the ECB and of the
    demo, and rates core.fx_rates_daily (macros/rates.sql). -#}
{% macro quarantine_rate_findings(lists, currencies, days, ecb, demo, rates) -%}
with listed as (
    {%- for relation, account in lists %}
    {%- if not loop.first %}
    union all
    {%- endif %}
    select platform as source, {{ account }} as account_id, currency
    from {{ relation }}
    {%- endfor %}
),

named as (
    select distinct source, account_id
    from {{ days }}
    where source in ('{{ quarantine_sources() | join("', '") }}')
),

published as (
    {{ rates_published(ecb, demo, rates) }}
),

rated as (
    select currency from ({{ rates_into_reporting('published') }}) as read_rates
    union
    select currency::text
    from {{ rates }}
    where reporting_currency = {{ reporting_currency() }}
)

select distinct
    l.source,
    l.account_id,
    cast(null as text) as load_id,
    'missing_rate'::text as reason,
    'rates'::text as checked,
    'no rate of ' || l.currency || ' into ' || {{ reporting_currency() }}
        || ' from the source of the rates, ' || {{ rates_source(ecb, rates) }} as detail
from listed as l
join named as n
    on n.source = l.source and n.account_id = l.account_id
join {{ currencies }} as c
    on c.currency_code = l.currency
where l.currency <> {{ reporting_currency() }}
    and not exists (select from rated as r where r.currency = l.currency)
{%- endmacro %}

{#- The days of an account on which a check failed, as the detail of its failure. -#}
{% macro quarantine_days(day) -%}
case
    when count(*) = 1 then 'on ' || min({{ day }})
    else 'on ' || count(*) || ' days from ' || min({{ day }}) || ' to ' || max({{ day }})
end
{%- endmacro %}

{#- The checks of the rows of one kind of facts, kind of quarantine_fact_kinds, over its staging
    models, relations, which read the newest pinned load of each account and day: two rows with one
    key, a part of the key or another ID that is missing, a measure that is missing, and one below
    zero. One pass over the rows: one aggregate by the key, whose memory PostgreSQL bounds by
    work_mem and spills beyond (#191), then the days of each account with a failure, each with the
    load that this run reads for the day, from days. -#}
{% macro quarantine_fact_findings(kind, relations, days) -%}
{%- set account, key, ids, required, counted = quarantine_fact_kinds()[kind] -%}
{%- set measures = required + (counted | reject('in', required) | list) -%}
with read as (
    {%- for relation in relations %}
    {%- if not loop.first %}
    union all
    {%- endif %}
    select
        platform as source,
        {{ account }} as account_id,
        report_date,
        {{ (key + ids + measures) | join(', ') }}
    from {{ relation }}
    {%- endfor %}
),

keys as (
    select
        source,
        account_id,
        report_date,
        count(*) > 1 as is_duplicate,
        bool_or({{ (key + ids) | join(' is null or ') }} is null) as misses_key,
        bool_or({{ required | join(' is null or ') }} is null) as misses_measure,
        bool_or({{ counted | join(' < 0 or ') }} < 0) as is_negative
    from read
    group by source, account_id, report_date, {{ key | join(', ') }}
),

failed_days as (
    select
        source,
        account_id,
        report_date,
        bool_or(is_duplicate) as duplicate_key,
        bool_or(misses_key) as missing_key,
        bool_or(misses_measure) as missing_measure,
        coalesce(bool_or(is_negative), false) as negative_measure
    from keys
    group by source, account_id, report_date
    having bool_or(is_duplicate or misses_key or misses_measure or coalesce(is_negative, false))
)

select
    f.source,
    f.account_id,
    d.load_id,
    c.reason,
    '{{ kind }}' as checked,
    {{ quarantine_days('f.report_date') }} as detail
from failed_days as f
join {{ days }} as d
    on d.source = f.source and d.account_id = f.account_id and d.day = f.report_date
cross join lateral (
    values
        ('duplicate_key', f.duplicate_key),
        ('missing_key', f.missing_key),
        ('missing_measure', f.missing_measure),
        ('negative_measure', f.negative_measure)
) as c (reason, failed)
where c.failed
group by f.source, f.account_id, d.load_id, c.reason
{%- endmacro %}

{#- The checks of the fact tables of one source in raw as they are, of the loads that this run reads
    for each account: rows dated outside the window that the journal of the load gives the
    account, which staging never reads, and a spend with more than six decimal places, which a
    cast to numeric(18,6) in staging would round silently (ADR-0036). One small aggregate for each
    table, by load and account. A table that no load has written yet is left out. -#}
{% macro quarantine_raw_findings(source_name, days, loads) -%}
{%- set code = source_name[4:] -%}
{%- set account, day = newest_load_keys(source_name) -%}
{%- set parts = [] -%}
{%- for table, spend, micros in quarantine_raw_tables()[source_name] -%}
    {%- if source_name.startswith('raw_demo_') -%}
        {%- set relation = demo_source(source_name, table) -%}
    {%- else -%}
        {%- set columns = {
            'customer__id': 'text', 'segments__date': 'date', '_dlt_load_id': 'text',
        } -%}
        {%- if spend is not none -%}
            {%- do columns.update({spend: 'text'}) -%}
        {%- endif -%}
        {%- set relation = relation_or_empty(source(source_name, table), columns) -%}
    {%- endif -%}
    {%- if spend is none -%}
        {%- set places = 'null::integer' -%}
    {%- else -%}
        {%- set places = 'max(scale(trim_scale(cast(nullif(nullif(trim(cast(r.' ~ spend
            ~ " as text)), ''), '-') as numeric))))" ~ (' + 6' if micros else '') -%}
    {%- endif -%}
    {%- do parts.append(
        "select '" ~ table ~ "' as checked, r._dlt_load_id as load_id, " ~ account
        ~ " as account_id, min(" ~ day ~ ") as first_day, max(" ~ day ~ ") as last_day, "
        ~ places ~ " as places\n    from (select * from " ~ relation ~ ") as r\n    group by "
        ~ "r._dlt_load_id, " ~ account
    ) -%}
{%- endfor %}
with tables as (
    {{ parts | join('\n    union all\n    ') }}
),

read as (
    select distinct account_id, load_id
    from {{ days }}
    where source = '{{ code }}'
)

select
    '{{ code }}' as source,
    t.account_id,
    t.load_id,
    c.reason,
    t.checked,
    c.detail
from tables as t
join read as d
    on d.account_id = t.account_id and d.load_id = t.load_id
join {{ loads }} as l
    on l.source = '{{ code }}' and l.load_id = t.load_id and l.account_id = t.account_id
cross join lateral (
    values
        (
            'date_outside_window',
            t.first_day < l.window_from or t.last_day > l.window_to,
            'rows from ' || t.first_day || ' to ' || t.last_day || ', a window from '
                || l.window_from || ' to ' || l.window_to
        ),
        (
            'too_many_decimals',
            coalesce(t.places > 6, false),
            'a spend with ' || t.places || ' decimal places'
        )
) as c (reason, failed, detail)
where c.failed
{%- endmacro %}

{#- G0a, against the platform (ADR-0035): for each load that this run reads for an account of a
    source whose loads bring the totals of each account by day, as the connector of Google Ads does,
    the impressions, the clicks and the spend of the rows of its campaigns, summed by day, against
    the totals that the platform gives for the account in the same load, on each day of the
    windows that the load gives the account; a day that the load holds no row of on one side
    counts as zero there. A sum that differs from its total by more than the share of the
    platform in var('platform_totals_tolerance'), zero for one that it does not name, is
    totals_differ: a read of the API that lost part of an account, which no check inside the
    warehouse can see. A load that holds no totals at all, as one of a connector before the
    totals came, is not checked. pairs gives, for each such source, its staging model of the
    statistics of campaigns and that of the totals. -#}
{% macro quarantine_totals_findings(pairs, days) -%}
{%- set tolerances = var('platform_totals_tolerance', {}) -%}
with totals as (
    {%- for _, relation in pairs %}
    {%- if not loop.first %}
    union all
    {%- endif %}
    select
        platform as source,
        account_id,
        report_date,
        load_id,
        total_impressions as impressions,
        total_clicks as clicks,
        total_spend as spend
    from {{ relation }}
    {%- endfor %}
),

totalled as (
    select distinct source, load_id
    from totals
),

window_days as (
    select d.source, d.account_id, d.day, d.load_id
    from {{ days }} as d
    join totalled as t
        on t.source = d.source and t.load_id = d.load_id
),

campaigns as (
    select
        source,
        account_id,
        report_date as day,
        sum(impressions) as impressions,
        sum(clicks) as clicks,
        sum(spend) as spend
    from (
        {%- for relation, _ in pairs %}
        {%- if not loop.first %}
        union all
        {%- endif %}
        select platform as source, account_id, report_date, impressions, clicks, spend
        from {{ relation }}
        {%- endfor %}
    ) as r
    group by source, account_id, report_date
),

account_days as (
    select
        source,
        account_id,
        report_date as day,
        sum(impressions) as impressions,
        sum(clicks) as clicks,
        sum(spend) as spend
    from totals
    group by source, account_id, report_date
),

compared as (
    select
        w.source,
        w.account_id,
        w.load_id,
        w.day,
        coalesce(c.impressions, 0) as campaign_impressions,
        coalesce(c.clicks, 0) as campaign_clicks,
        coalesce(c.spend, 0) as campaign_spend,
        coalesce(a.impressions, 0) as total_impressions,
        coalesce(a.clicks, 0) as total_clicks,
        coalesce(a.spend, 0) as total_spend,
        {%- if tolerances %}
        case
            {%- for platform, share in tolerances.items() %}
            when w.source = '{{ platform }}' then {{ share | float }}
            {%- endfor %}
            else 0
        end as share
        {%- else %}
        0 as share
        {%- endif %}
    from window_days as w
    left join campaigns as c
        on c.source = w.source and c.account_id = w.account_id and c.day = w.day
    left join account_days as a
        on a.source = w.source and a.account_id = w.account_id and a.day = w.day
)

select
    source,
    account_id,
    load_id,
    'totals_differ' as reason,
    'account_totals' as checked,
    {{ quarantine_days('day') }} || ': impressions ' || sum(campaign_impressions) || ', clicks '
        || sum(campaign_clicks) || ' and spend ' || sum(campaign_spend)
        || ' in the campaigns, against ' || sum(total_impressions) || ', '
        || sum(total_clicks) || ' and ' || sum(total_spend) || ' in the totals of the account'
        as detail
from compared
where abs(campaign_impressions - total_impressions) > share * abs(total_impressions)
    or abs(campaign_clicks - total_clicks) > share * abs(total_clicks)
    or abs(campaign_spend - total_spend) > share * abs(total_spend)
group by source, account_id, load_id
{%- endmacro %}

{#- G1a, against a collapse (ADR-0035): for each load that this run reads for an account, a measure
    of its rows on the days that the load gives, against what the core holds on the same days. A
    load with no row on its days, as a lost token gives, is empty_window; a measure that fell by
    more than var('quarantine_spend_fall') of what the core holds, spend_fall, where fall is true. A
    window replaces whole days, so either would erase them. relations are the staging models of a
    kind of facts, checked, with the column of their account, account; accounts and facts the
    tables core.dim_account and the fact of the core of that kind as the runs before this one left
    them. The ad platforms compare the spend of the statistics of campaigns, and web analytics,
    which has no spend, the sessions, for an empty window only. The days of the core are bounded by
    those of the windows, so that PostgreSQL reads only the partitions of the windows. -#}
{% macro quarantine_collapse_findings(
    relations, days, accounts, facts, measure='spend', checked='campaign_stats',
    account='account_id', fall=true
) -%}
with window_days as (
    select source, account_id, day, load_id
    from {{ days }}
    where source in ('{{ quarantine_sources() | join("', '") }}')
),

held as (
    select
        a.platform as source,
        a.account_id,
        f.report_date as day,
        sum(f.{{ measure }}) as held
    from {{ facts }} as f
    join {{ accounts }} as a
        on a.account_sk = f.account_sk
    where f.report_date >= (select min(day) from window_days)
        and f.report_date <= (select max(day) from window_days)
    group by a.platform, a.account_id, f.report_date
),

read as (
    {%- for relation in relations %}
    {%- if not loop.first %}
    union all
    {%- endif %}
    select platform as source, {{ account }} as account_id, report_date, {{ measure }}
    from {{ relation }}
    {%- endfor %}
),

read_days as (
    select source, account_id, report_date as day, sum({{ measure }}) as read
    from read
    group by source, account_id, report_date
),

compared as (
    select
        w.source,
        w.account_id,
        w.load_id,
        min(w.day) as first_day,
        max(w.day) as last_day,
        coalesce(sum(h.held), 0) as held,
        coalesce(sum(r.read) filter (where h.day is not null), 0) as read,
        count(r.day) as read_days
    from window_days as w
    left join held as h
        on h.source = w.source and h.account_id = w.account_id and h.day = w.day
    left join read_days as r
        on r.source = w.source and r.account_id = w.account_id and r.day = w.day
    group by w.source, w.account_id, w.load_id
)

select
    source,
    account_id,
    load_id,
    case when read_days = 0 then 'empty_window' else 'spend_fall' end as reason,
    '{{ checked }}' as checked,
    case
        when read_days = 0 then
            'no row from ' || first_day || ' to ' || last_day || ', against {{ measure }} of '
            || held || ' in the core'
        else '{{ measure }} of ' || read || ' against ' || held || ' in the core on the same days'
    end as detail
from compared
where held > 0
    and (
        read_days = 0
        {%- if fall %}
        or read < held * (1 - {{ var('quarantine_spend_fall') }})
        {%- endif %}
    )
{%- endmacro %}

{#- The stop of a source (ADR-0035, ADR-0046): a source with more than half of its accounts in
    quarantine, of the accounts that core.loads holds pinned for it, stops, and each of its pinned
    accounts gets the reason source_stopped, so that the healthy ones stay out of the core too;
    exactly half stops nothing. A filter in SQL, not a test at error, which would stop the healthy
    sources too (#188). It reads the failures that the statements before it wrote. -#}
{% macro quarantine_stop_findings(failures, loads) -%}
with pinned as (
    select distinct source, account_id
    from {{ loads }}
    where status = 'pinned'
        and source in ('{{ quarantine_sources() | join("', '") }}')
),

held as (
    select p.source, count(distinct p.account_id) as accounts
    from pinned as p
    where exists (
        select from {{ failures }} as f
        where f.source = p.source and f.account_id = p.account_id
    )
    group by p.source
),

stopped as (
    select h.source, h.accounts as held, count(*) as accounts
    from held as h
    join pinned as p
        on p.source = h.source
    group by h.source, h.accounts
    having h.accounts * 2 > count(*)
)

select
    p.source,
    p.account_id,
    null as load_id,
    'source_stopped' as reason,
    'source' as checked,
    'in quarantine: ' || s.held || ' of the pinned accounts of the source, ' || s.accounts
        as detail
from stopped as s
join pinned as p
    on p.source = s.source
{%- endmacro %}

{#- Whether a row of an account is kept in this run: no check of int_g1_failures, failures, held
    its account out of the core (ADR-0035). platform and account are the columns of the row. -#}
{% macro not_in_quarantine(failures, platform, account) -%}
not exists (
    select from {{ failures }} as quarantined
    where quarantined.source = {{ platform }} and quarantined.account_id = {{ account }}
)
{%- endmacro %}
