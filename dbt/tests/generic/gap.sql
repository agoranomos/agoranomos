{#- Finds every day, between the first and the last day of each account, on which the account
    has no row: a day that a load missed. A day on which every campaign of an account paused has
    no row either, so its YAML gives this test the severity of a warning. -#}
{% test gap(model, account_columns, date_column) %}
with bounds as (
    select {{ account_columns | join(', ') }}, min({{ date_column }}) as first_day, max({{ date_column }}) as last_day
    from {{ model }}
    group by {{ account_columns | join(', ') }}
),
expected as (
    select {{ account_columns | join(', ') }}, cast(generate_series(first_day, last_day, interval '1 day') as date) as missing_day
    from bounds
)
select e.*
from expected as e
where not exists (
    select 1
    from {{ model }} as m
    where {% for column in account_columns %}m.{{ column }} = e.{{ column }} and {% endfor %}m.{{ date_column }} = e.missing_day
)
{% endtest %}
