{#- A name of a platform as staging needs it: of a campaign, an account or a source of sessions.
    dlt loads an empty string as NULL (csv, ADR-0038), and a row of an earlier release can keep
    one: either way the platform gave no name, so a coalesce goes on to the next name, and the
    not_null test of a name that a platform must give stops the model. A case and not nullif,
    which gives a text where the column is a character varying, and so another type in the marts
    (PostgreSQL 18, 9.18.3). -#}
{% macro to_name(expression) -%}
    case when {{ expression }} <> '' then {{ expression }} end
{%- endmacro %}
