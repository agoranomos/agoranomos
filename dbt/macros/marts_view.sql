{#- The views of the marts (ADR-0037, ADR-0039): the project overrides the materialization view of
    dbt, so that a run of the transform leaves a view of marts that exists as it is, with its oid,
    its grants and its comments. dbt would recreate it on every run, under a rename and a drop
    that take ACCESS EXCLUSIVE on its name: that lock waits for a long query of Metabase, and every
    reader waits behind it. The model then does nothing, and its tests and its task in Airflow
    still run. A view that does not exist, or a relation of another kind under its name, such as a
    table of a mart of v0.4, is built as dbt builds a view, and so is every view of marts when a
    build passes the var rebuild_views: dbt build --select marts --vars '{rebuild_views: true}'.
    The views of the other schemas, staging and ops, are built on every run as before: a view of
    staging reads a table of raw through relation_or_empty, which only the next build finds. A
    view of marts reads the core alone, which no run drops, as a view over a view of staging would
    go with it on every run (#187). -#}
{% materialization view, default %}
    {%- set existing = load_cached_relation(this) -%}
    {%- if model.schema == 'marts' and existing is not none and existing.is_view
        and not var('rebuild_views', false) -%}
        {%- call noop_statement('main', 'KEPT') -%}{%- endcall -%}
        {{- return({'relations': [this.incorporate(type='view')]}) -}}
    {%- endif -%}
    {{- return(dbt.materialization_view_default()) -}}
{% endmaterialization %}
