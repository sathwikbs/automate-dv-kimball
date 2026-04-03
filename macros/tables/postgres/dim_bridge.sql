/*
 * Copyright (c) 2026 automate-dv-kimball contributors
 * Licensed under the Apache License, Version 2.0
 */

{%- macro postgres__dim_bridge(src_pk, bridge_fks, source_model, src_ldts, weighting_factor, src_eff, src_exp, src_extra_columns) -%}

{%- set max_datetime = automate_dv.max_datetime() -%}
{%- set bridge_fk_list = automate_dv.expand_column_list(columns=[bridge_fks]) -%}
{%- set pk_list = automate_dv.expand_column_list(columns=[src_pk]) -%}
{%- set time_varying = src_eff is not none and src_exp is not none -%}

WITH bridge_source AS (
    SELECT
        {{ automate_dv.prefix(pk_list, 'f') }},
        {{ automate_dv.prefix(bridge_fk_list, 'f') }},
        {{ automate_dv.prefix([src_ldts], 'f') }},
        {%- if automate_dv.is_something(weighting_factor) %}
        f.{{ weighting_factor }}{{ ',' if time_varying or automate_dv.is_something(src_extra_columns) }}
        {%- endif %}
        {%- if time_varying %}
        {{ automate_dv.prefix([src_eff], 'f') }},
        {{ automate_dv.prefix([src_exp], 'f') }}{{ ',' if automate_dv.is_something(src_extra_columns) }}
        {%- endif %}
        {%- if automate_dv.is_something(src_extra_columns) %}
        {{ automate_dv.prefix(automate_dv.expand_column_list(columns=[src_extra_columns]), 'f') }}
        {%- endif %}
    FROM {{ ref(source_model) }} AS f
    WHERE {{ automate_dv.multikey(src_pk, prefix='f', condition='IS NOT NULL') }}
),

{%- if time_varying %}

bridge_current AS (
    SELECT *
    FROM bridge_source AS f
    WHERE f.{{ src_exp }} = {{ automate_dv.cast_date(max_datetime, as_string=true, datetime=true) }}
        OR f.{{ src_exp }} > CURRENT_DATE
),

{%- endif %}

bridge_latest AS (
    SELECT DISTINCT ON ({{ automate_dv.prefix(pk_list, 'f') }})
        {{ automate_dv.prefix(pk_list, 'f') }},
        {{ automate_dv.prefix(bridge_fk_list, 'f') }},
        {{ automate_dv.prefix([src_ldts], 'f') }}{{ ',' if automate_dv.is_something(weighting_factor) or time_varying or automate_dv.is_something(src_extra_columns) }}
        {%- if automate_dv.is_something(weighting_factor) %}
        f.{{ weighting_factor }}{{ ',' if time_varying or automate_dv.is_something(src_extra_columns) }}
        {%- endif %}
        {%- if time_varying %}
        {{ automate_dv.prefix([src_eff], 'f') }},
        {{ automate_dv.prefix([src_exp], 'f') }}{{ ',' if automate_dv.is_something(src_extra_columns) }}
        {%- endif %}
        {%- if automate_dv.is_something(src_extra_columns) %}
        {{ automate_dv.prefix(automate_dv.expand_column_list(columns=[src_extra_columns]), 'f') }}
        {%- endif %}
    FROM {% if time_varying %}bridge_current{% else %}bridge_source{% endif %} AS f
    ORDER BY {{ automate_dv.prefix(pk_list, 'f') }}, {{ automate_dv.prefix([src_ldts], 'f') }} DESC
),

bridge_output AS (
    SELECT
        {{ pk_list | join(',\n        ') }},
        {{ bridge_fk_list | join(',\n        ') }},
        {{ src_ldts }}
        {%- set opt_after_ldts = automate_dv.is_something(weighting_factor) or time_varying or automate_dv.is_something(src_extra_columns) -%}
        {%- if opt_after_ldts %},{%- endif %}
        {%- if automate_dv.is_something(weighting_factor) %}
        {{ weighting_factor }}
        {%- if time_varying or automate_dv.is_something(src_extra_columns) %},{%- endif %}
        {%- endif %}
        {%- if time_varying %}
        {{ src_eff }},
        {{ src_exp }}
        {%- if automate_dv.is_something(src_extra_columns) %},{%- endif %}
        {%- endif %}
        {%- if automate_dv.is_something(src_extra_columns) %}
        {{ automate_dv.expand_column_list(columns=[src_extra_columns]) | join(',\n        ') }}
        {%- endif %}
    FROM bridge_latest
)

{%- if automate_dv.is_any_incremental() %}
SELECT bo.*
FROM bridge_output AS bo
LEFT JOIN {{ this }} AS d
    ON {{ automate_dv.multikey(src_pk, prefix=['bo', 'd'], condition='=') }}
WHERE {{ automate_dv.multikey(src_pk, prefix='d', condition='IS NULL') }}
{%- else %}
SELECT * FROM bridge_output
{%- endif %}

{%- endmacro -%}
