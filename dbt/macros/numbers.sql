{#- The numbers of the APIs as staging needs them (ADR-0025). The platforms send numbers as
    strings, and TikTok writes "-" for a number that means nothing on a day: each becomes NULL. -#}

{#- A whole number, such as impressions. -#}
{% macro to_count(expression) -%}
    cast(nullif(nullif(trim(cast({{ expression }} as text)), ''), '-') as bigint)
{%- endmacro %}

{#- Any other number, such as money in major units, to six decimal places. -#}
{% macro to_number(expression) -%}
    cast(nullif(nullif(trim(cast({{ expression }} as text)), ''), '-') as numeric(18,6))
{%- endmacro %}

{#- Money in millionths of a unit, as Google writes cost_micros, in units. -#}
{% macro micros_to_number(expression) -%}
    cast(cast(nullif(trim(cast({{ expression }} as text)), '') as numeric(20,0)) / 1000000 as numeric(18,6))
{%- endmacro %}

{#- The ID of an entity that the API writes as a string of digits and the core keeps as bigint
    (ADR-0036), such as an ID of TikTok (ADR-0053), as text in the form that the core gives it, or
    NULL for an empty one and for one that is not a whole number that fits bigint (id_fits), never
    cut. G1 reads the IDs in raw as they are and holds the account of such a value in quarantine
    with invalid_id (quarantine_raw_ids), and a NULL in a key of a fact with missing_key, so that
    one account with a bad ID stops neither the run nor the others (ADR-0035). -#}
{% macro to_id(expression) -%}
    case when {{ id_fits(expression) }}
        then cast(cast(trim(cast({{ expression }} as text)) as bigint) as text)
    end
{%- endmacro %}

{#- Whether a value is a whole number of digits alone that fits bigint, at most
    9223372036854775807: false for NULL, an empty value, a sign, a space inside, a decimal point,
    or more. The CASE keeps the cast to numeric from a value that is not digits. -#}
{% macro id_fits(expression) -%}
    case
        when trim(cast({{ expression }} as text)) ~ '^[0-9]{1,19}$'
            then cast(trim(cast({{ expression }} as text)) as numeric) <= 9223372036854775807
        else false
    end
{%- endmacro %}

{#- The rows of a report of facts, the SQL of the call block, with any of their measures other
    than zero: a row whose measures are all zero, or none, is left out of staging, whatever the API
    returned (ADR-0036), and a row with zero impressions and a conversion stays. The statistics of
    campaigns also pass conversions, the staging model of the conversions of the campaigns of the
    same platform, whose rows have a conversion or a value other than zero: a row of statistics
    whose measures are all zero then stays when its campaign has a conversion on its day. The
    measures of a row alone would lose such a day, and conversions_primary of the view of campaigns
    with it: Meta counts its actions in a child table and none in the statistics, the conversions
    of TikTok and of demo_search need not be in the own metric of the platform, and Google Ads
    gives its conversions by action in a report of their own. The join is to the distinct campaign
    days of the conversions, not a subquery for each row. The levels of ad groups and ads keep a
    row by its measures alone, as the core holds the conversions by action of campaigns only. -#}
{% macro nonzero(measures, conversions=none) -%}
select reported.* from (
{{ caller() }}
) as reported
{%- if conversions is not none %}
left join (
    select distinct platform, account_id, campaign_id, report_date
    from {{ conversions }}
) as converted
    on converted.platform = reported.platform
    and converted.account_id = reported.account_id
    and converted.campaign_id = reported.campaign_id
    and converted.report_date = reported.report_date
{%- endif %}
where {% for measure in measures %}reported.{{ measure }} <> 0{% if not loop.last %} or {% endif %}{% endfor %}
{%- if conversions is not none %} or converted.report_date is not null{% endif %}
{%- endmacro %}
