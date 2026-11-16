{#- The rates of the core (ADR-0042, ADR-0037): core.fx_rates_daily holds, for each calendar day,
    each currency of the accounts and the reporting currency, the rate of one unit of the currency
    in the reporting currency, through the base currency of the first source of the rates with no
    rounding on the way, as Regulation (EC) 1103/97 asks, and carries the last published rate over
    a day without one, with a flag. The setting AGORANOMOS_RATES_SOURCE gives the sources and their
    order (rates_chain), and the demo serves only a deployment that holds no rate of them. The
    model core_fx_rates_daily writes the table, and the checks of G1 (quarantine.sql) find an
    account whose currency has no rate at all, both through rates_known. -#}

{#- The sources of the rates, in their order, as the setting AGORANOMOS_RATES_SOURCE of .env gives
    them, which compose.yaml passes to dbt; auto, the default, is nbk for a reporting currency of
    KZT and bank_of_italy for another. ./agoranomos checks the value, and dbt stops on another.
    * bank_of_italy: the Bank of Italy, then the National Bank of Kazakhstan for a currency that the
      first lacks, as RUB and BYN as of November 2026, through the tenge, then the European Central
      Bank;
    * nbk: the National Bank of Kazakhstan, then the Bank of Italy for a currency that the first
      lacks, through the euro, then the European Central Bank;
    * ecb: the European Central Bank alone, for a deployment that keeps its reference rates.
    The rates of a file of the deployment, planned for v0.7, are to come first. -#}
{% macro rates_chain() -%}
    {%- set setting = env_var('AGORANOMOS_RATES_SOURCE', 'auto') or 'auto' -%}
    {%- if setting == 'auto' -%}
        {%- set setting = 'nbk' if env_var('AGORANOMOS_REPORTING_CURRENCY', 'USD') == 'KZT' else 'bank_of_italy' -%}
    {%- endif -%}
    {%- set chains = {
        'bank_of_italy': ['bank_of_italy', 'nbk', 'ecb'],
        'nbk': ['nbk', 'bank_of_italy', 'ecb'],
        'ecb': ['ecb'],
    } -%}
    {%- if setting not in chains -%}
        {{- exceptions.raise_compiler_error(
            'AGORANOMOS_RATES_SOURCE must be auto, bank_of_italy, nbk or ecb, not ' ~ setting
        ) -}}
    {%- endif -%}
    {{- return(chains[setting]) -}}
{%- endmacro %}

{#- The sources of the rates as the check of the rates names them (quarantine.sql), as SQL text: the
    order of AGORANOMOS_RATES_SOURCE, or demo_fx while no source of the order has a rate, neither in
    its staging model nor in core.fx_rates_daily, as in the demo, which then serves (rates_known).
    staged maps the code of each source to its staging model, rates is core.fx_rates_daily. -#}
{% macro rates_named(staged, rates) -%}
    {%- set chain = rates_chain() -%}
    {%- set named = ('the sources of the rates, ' if chain | length > 1 else 'the source of the rates, ') ~ chain | join(', ') -%}
(case
    when
        {%- for code in chain %}
        {% if not loop.first %}or {% endif %}exists (select from {{ staged[code] }})
        {%- endfor %}
        or exists (select from {{ rates }} where rate_source in ('{{ chain | join("', '") }}'))
        then '{{ named }}'
    else 'the source of the rates, demo_fx'
end)
{%- endmacro %}

{#- The base currency of a source of the rates, against which it publishes them: the tenge for the
    National Bank of Kazakhstan, the euro for the others. -#}
{% macro rates_base(code) -%}
    {{- return('KZT' if code == 'nbk' else 'EUR') -}}
{%- endmacro %}

{#- The rate of a source in the convention of the model, the units of a currency for one unit of
    the base, as SQL, from value, the rate as the source published it: the National Bank of
    Kazakhstan publishes the tenge for one unit of the currency, so its rate is the inverse, and the
    others publish the units of the currency for one euro. code is the code of the source as SQL. -#}
{% macro rates_from_published(code, value) -%}
(case when {{ code }} = 'nbk' then 1 / {{ value }} else {{ value }} end)
{%- endmacro %}

{#- The other way round: the rate as the source published it, from the rate of the model. -#}
{% macro rates_to_published(code, value) -%}
(case when {{ code }} = 'nbk' then 1 / {{ value }} else {{ value }} end)
{%- endmacro %}

{#- The rates that the sources give into the reporting currency, as SQL: one row for each day of
    publication of each currency of wanted but the reporting currency, with the rate of one unit of
    it in the reporting currency, the source of its rate, its base currency and its rate as the
    source published it, and the second leg of a rate that goes through the base of another
    source. staged maps the code of each source to its staging model, rates is
    core.fx_rates_daily, days stg_loads__newest_days, loads core.loads, and wanted SQL that gives
    the column currency of the currencies to compute.
    * What each source publishes: on a day that a load of it that this run reads covers, what
      staging holds of the day, so a day without a row has no publication; on another day, what
      core.fx_rates_daily holds as published from it, as the raw layer keeps only the loads that
      the core has not accepted (ADR-0035). Such a row counts when it goes through the base of
      the first source, as all do while the setting keeps its base: after a change between a base
      of EUR and one of KZT, the days whose loads the core accepted have no rate until a load of
      their history (agoranomos_pipelines/rates.py). Each source with its base at 1 on each day of
      publication, and a rate of zero or less, which no source publishes, left out.
    * The rate of a currency against the base of the first source: that of the first source; that
      of another source with the same base; or, for a source with another base, its rate through
      the rate of its base that the first source published on the same day or the last day before
      it, the second leg, with no rounding but that of one division: RUB of the NBK is its price
      in tenge divided by the tenge for one euro of the Bank of Italy.
    * The order of each day and currency (rates_chain): the first source; the second for a
      currency that the first did not publish on its last day of publication on or before the day,
      as RUB, which the Bank of Italy lacks, on a Saturday too, as the NBK publishes on every
      calendar day; then the European Central Bank, and without any rate of a real source the demo,
      for a currency that the first source did not publish on the day, as when its load of the day
      failed. A load of the history fills only the days and currencies that the core holds no rate
      of, or holds from the same source: the rows that the core holds, such as those of the ECB of
      a deployment of v0.5, keep their source.
    * The rate into the reporting currency: through the rate of the reporting currency against the
      base of the first source, of the same day or of the last day before it on which a source of
      the order published it, and that of the currency, in one division; for the base itself as
      the reporting currency, the rate of the currency alone, so that the tenge of a currency of
      the NBK is the price that it published. -#}
{% macro rates_known(staged, rates, days, loads, wanted) -%}
    {%- set chain = rates_chain() -%}
    {%- set first = chain[0] -%}
    {%- set second = chain[1] if chain | length > 2 else none -%}
    {%- set base = rates_base(first) -%}
    {%- set links = chain + ['demo_fx'] -%}
    {%- set reported = env_var('AGORANOMOS_REPORTING_CURRENCY', 'USD') -%}
with read_days as (
    select distinct on (d.source, d.day)
        d.source,
        d.day,
        coalesce(l.kind, 'window') as kind
    from {{ days }} as d
    left join {{ loads }} as l
        on l.source = d.source and l.load_id = d.load_id and l.account_id = d.account_id
    where d.source in ('{{ links | join("', '") }}')
    order by d.source, d.day
),

wanted as (
    select distinct cast(currency as text) as currency
    from ({{ wanted }}) as w
    where currency is not null
    union
    select {{ reporting_currency() }}
),

-- The rows that the table holds as published from a source of the order, of every reporting
-- currency, and one of them for each day and currency, of the reporting currency of the
-- deployment where it has one.
held_all as (
    select
        h.rate_date,
        h.currency::text as currency,
        h.reporting_currency::text as reporting_currency,
        h.rate_into_reporting_currency,
        h.base_currency::text as base_currency,
        h.base_rate,
        h.rate_source,
        h.cross_source,
        h.cross_rate,
        h.cross_published_on,
        h.published_on
    from {{ rates }} as h
    where not h.is_carried_forward
        and h.base_rate is not null
        and h.rate_source in ('{{ links | join("', '") }}')
),

held_rows as (
    select distinct on (rate_date, currency) *
    from held_all
    order by rate_date, currency, reporting_currency <> {{ reporting_currency() }},
        reporting_currency
),

-- What each source publishes, as staging holds it on the days that it reads.
staged as (
    {%- for code in links %}
    {%- if not loop.first %}
    union all
    {%- endif %}
    select
        s.rate_date,
        '{{ code }}' as source,
        s.base_currency::text as base_currency,
        s.quote_currency::text as currency,
        {%- if code == 'nbk' %}
        {#- From the price as the NBK publishes it, as a row that the table holds gives it. #}
        {{ rates_from_published("'nbk'", 's.base_rate') }} as rate,
        cast(s.base_rate as numeric) as base_rate,
        {%- else %}
        cast(s.rate as numeric) as rate,
        cast(s.rate as numeric) as base_rate,
        {%- endif %}
        r.kind
    from {{ staged[code] }} as s
    join read_days as r on r.source = '{{ code }}' and r.day = s.rate_date
    where s.rate > 0
        and s.quote_currency <> s.base_currency
    {%- endfor %}
),

-- What each source published as the table holds it, through the base of the first source: a rate
-- against that base; a rate against the base of another source, with its second leg, which is a
-- publication of the first source of that base; and the rate of a reporting currency of before
-- that a row of that base keeps.
held as (
    select
        h.published_on as rate_date,
        h.rate_source as source,
        h.base_currency,
        h.currency,
        {{ rates_from_published('h.rate_source', 'h.base_rate') }} as rate,
        h.base_rate
    from held_rows as h
    where (h.cross_source is null and h.base_currency = '{{ base }}')
        or h.cross_source = '{{ first }}'
    union all
    select
        h.cross_published_on,
        h.cross_source,
        '{{ base }}',
        h.base_currency,
        {{ rates_from_published('h.cross_source', 'h.cross_rate') }},
        h.cross_rate
    from held_rows as h
    where h.cross_source = '{{ first }}'
    union all
    -- The rate into the reporting currency of before comes out of a division with more decimal
    -- places than its value has: trim_scale gives it those of the value, which changes no digit,
    -- so a division through it has the digits of one through the rate as the source published it.
    select
        h.published_on,
        h.rate_source,
        h.base_currency,
        h.reporting_currency,
        trim_scale(h.rate_into_reporting_currency),
        {{ rates_to_published('h.rate_source', 'trim_scale(h.rate_into_reporting_currency)') }}
    from held_all as h
    where h.cross_source is null
        and h.base_currency = '{{ base }}'
        and h.currency = h.base_currency
        and h.reporting_currency <> h.currency
),

published as (
    select rate_date, source, base_currency, currency, rate, base_rate, kind
    from staged
    union all
    select rate_date, source, base_currency, currency, rate, base_rate, 'held'
    from (
        select distinct on (h.rate_date, h.source, h.currency) h.*
        from held as h
        where h.rate > 0
            and h.currency <> h.base_currency
            and not exists (
                select from read_days as d where d.source = h.source and d.day = h.rate_date
            )
        order by h.rate_date, h.source, h.currency
    ) as h
),

-- Each source at 1 on each of its days of publication. Each rate with the decimal places of its
-- value (trim_scale, which changes no digit), as staging, the table and a division give the same
-- value with different places: a division then has the same digits whether its rates come from
-- staging or from the table, so a later run computes a row that the table holds the same again.
publications as (
    select rate_date, source, base_currency, currency, trim_scale(rate) as rate,
        trim_scale(base_rate) as base_rate, kind
    from published
    union all
    select rate_date, source, base_currency, base_currency, cast(1 as numeric), cast(1 as numeric), kind
    from (
        select distinct on (rate_date, source) rate_date, source, base_currency, kind
        from published
        order by rate_date, source, kind = 'held', kind
    ) as days_of_publication
),

-- Whether a source of the order holds or reads a rate: the demo serves only without one.
real as (
    select exists (select from publications where source <> 'demo_fx') as found
),

calendar as (
    select cast(day as date) as day
    from (select min(rate_date) as first_day, max(rate_date) as last_day from publications) as b
    cross join lateral generate_series(
        cast(b.first_day as timestamp), cast(b.last_day as timestamp), interval '1 day'
    ) as day
),

-- For each calendar day, the last day on or before it on which the first source published.
first_days as (
    select c.day, max(p.rate_date) over (order by c.day) as last_published
    from calendar as c
    left join (select distinct rate_date from publications where source = '{{ first }}') as p
        on p.rate_date = c.day
),

-- The rate of the base of another source that the first source published, on each calendar day
-- the last one on or before it: the second leg.
bridge_days as (
    select
        c.day,
        b.currency,
        max(p.rate_date) over (partition by b.currency order by c.day) as published_on
    from calendar as c
    cross join (
        select distinct base_currency as currency
        from publications
        where base_currency <> '{{ base }}'
    ) as b
    left join (
        select distinct rate_date, currency from publications where source = '{{ first }}'
    ) as p
        on p.rate_date = c.day and p.currency = b.currency
),

bridges as (
    select d.day, d.currency, d.published_on, p.rate, p.base_rate
    from bridge_days as d
    join publications as p
        on p.source = '{{ first }}' and p.rate_date = d.published_on and p.currency = d.currency
),

-- Every rate of a wanted currency against the base of the first source, with its role in the
-- order and its place. amount is in the convention of the first source, so that its own rate is
-- never divided twice: for a base of EUR the units of the currency for one euro, and for a base of
-- KZT the tenge for one unit of the currency. A rate of a source with another base goes through
-- the rate of that base that the first source published, in one division.
candidates as (
    {%- for code in links %}
    {%- if not loop.first %}
    union all
    {%- endif %}
    select
        p.rate_date,
        p.currency,
        '{{ code }}' as source,
        '{{ "first" if code == first else ("second" if code == second else "fallback") }}' as role,
        {{ loop.index }} as place,
        p.kind,
        p.base_currency,
        p.base_rate,
        {%- if rates_base(code) == base and base == 'EUR' %}
        p.rate as amount,
        {%- elif rates_base(code) == base %}
        p.base_rate as amount,
        {%- elif base == 'EUR' %}
        b.rate / p.base_rate as amount,
        {%- else %}
        b.base_rate / p.rate as amount,
        {%- endif %}
        {%- if rates_base(code) == base %}
        cast(null as text) as cross_source,
        cast(null as numeric) as cross_rate,
        cast(null as date) as cross_published_on
        {%- else %}
        '{{ first }}' as cross_source,
        b.base_rate as cross_rate,
        b.published_on as cross_published_on
        {%- endif %}
    from publications as p
    join wanted as w on w.currency = p.currency
    {%- if rates_base(code) != base %}
    join bridges as b on b.day = p.rate_date and b.currency = p.base_currency
    {%- endif %}
    where p.source = '{{ code }}'
    {%- if code == 'demo_fx' %}
        and not (select found from real)
    {%- endif %}
    {%- endfor %}
),

-- Whether a rate is the fill of a load of the history over a row that the table holds from
-- another source.
ranked as (
    select
        c.*,
        case
            when c.kind = 'history' and h.rate_source is not null and h.rate_source <> c.source
                then 1
            else 0
        end as tier
    from candidates as c
    left join held_rows as h on h.rate_date = c.rate_date and h.currency = c.currency
),

-- The first source always; the second for a currency that the first did not publish on its last
-- day of publication on or before the day; a fallback for a currency of which the first source
-- has no rate on the day but the fill of a load of the history.
eligible as (
    select r.*
    from ranked as r
    join first_days as f on f.day = r.rate_date
    where r.role = 'first'
        or (
            r.role = 'second'
            and not exists (
                select
                from publications as p
                where p.source = '{{ first }}'
                    and p.rate_date = f.last_published
                    and p.currency = r.currency
            )
        )
        or (
            r.role = 'fallback'
            and not exists (
                select
                from ranked as m
                where m.role = 'first'
                    and m.rate_date = r.rate_date
                    and m.currency = r.currency
                    and m.tier = 0
            )
        )
),

chosen as (
    select distinct on (rate_date, currency) *
    from eligible
    order by rate_date, currency, tier, place
)
{%- if base != reported %},

-- The rate of the reporting currency against the base of the first source on each calendar day,
-- that of its last day of publication on or before it.
reporting_days as (
    select c.day, max(r.rate_date) over (order by c.day) as published_on
    from calendar as c
    left join chosen as r
        on r.rate_date = c.day and r.currency = {{ reporting_currency() }}
),

reporting_rates as (
    select d.day, r.amount
    from reporting_days as d
    join chosen as r
        on r.rate_date = d.published_on and r.currency = {{ reporting_currency() }}
)
{%- endif %}

select
    c.rate_date as published_on,
    c.currency,
    c.base_currency,
    c.base_rate,
    {%- if base == reported and base == 'EUR' %}
    1 / c.amount as rate_into_reporting_currency,
    {%- elif base == reported %}
    c.amount as rate_into_reporting_currency,
    {%- elif base == 'EUR' %}
    r.amount / c.amount as rate_into_reporting_currency,
    {%- else %}
    c.amount / r.amount as rate_into_reporting_currency,
    {%- endif %}
    c.source as rate_source,
    c.cross_source,
    c.cross_rate,
    c.cross_published_on
from chosen as c
{%- if base != reported %}
join reporting_rates as r on r.day = c.rate_date
{%- endif %}
where c.currency <> {{ reporting_currency() }}
{%- endmacro %}
