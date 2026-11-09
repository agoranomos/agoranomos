{#- G2 of ADR-0035: fails when more than the share dimensionless_share of the vars of
    dbt_project.yml of the rows of the model, the rows of a fact of the core in the window that the
    where of its config gives, have values of the columns that no row of `to`, a dimension of the
    core, has as to_columns: rows of a fact whose entity has no row in its dimension. Each entity
    that the statistics name gets one, as a stub if no list gives it (ADR-0036). -#}
{% test few_without_a_dimension(model, columns, to, to_columns) %}
select
    count(*) filter (where t.{{ to_columns[0] }} is null) as without_a_dimension,
    count(*) as checked
{#- With a where, dbt gives the model as a subquery with an alias of its own. #}
from (select * from {{ model }}) as m
left join {{ to }} as t
    on {% for column in columns %}t.{{ to_columns[loop.index0] }} = m.{{ column }}{% if not loop.last %} and {% endif %}{% endfor %}
having count(*) filter (where t.{{ to_columns[0] }} is null) > {{ var('dimensionless_share') }} * count(*)
{% endtest %}
