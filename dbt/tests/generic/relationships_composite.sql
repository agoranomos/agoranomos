{#- Fails for every row of the model whose values of the columns appear in no row of `to` as the
    values of to_columns, in that order. A row with a NULL in the columns is left to not_null. -#}
{% test relationships_composite(model, columns, to, to_columns) %}
select {% for column in columns %}m.{{ column }}{% if not loop.last %}, {% endif %}{% endfor %}
from {{ model }} as m
where {% for column in columns %}m.{{ column }} is not null and {% endfor %}not exists (
    select 1
    from {{ to }} as t
    where {% for column in columns %}t.{{ to_columns[loop.index0] }} = m.{{ column }}{% if not loop.last %} and {% endif %}{% endfor %}
)
{% endtest %}
