/*
 * Copyright (c) 2026 automate-dv-kimball contributors
 * Licensed under the Apache License, Version 2.0
 */

{%- macro sqlserver__fact(src_pk, src_fk, src_ldts, source_model, dimensions, satellites, degenerate_dimensions, src_extra_columns) -%}

{%- set fk_list = automate_dv.expand_column_list(columns=[src_fk]) -%}
{%- set degen_list = degenerate_dimensions if degenerate_dimensions is iterable and degenerate_dimensions is not string and degenerate_dimensions is not none else [] -%}

WITH src AS (
    SELECT
        {{ automate_dv.prefix([src_pk], 's') }},
        {{ automate_dv.prefix([src_ldts], 's') }},
        {%- for fk in fk_list %}
        {{ automate_dv.prefix([fk], 's') }}{{ ', ' if not loop.last or degen_list | length > 0 }}
        {%- endfor %}
        {%- for dd in degen_list %}
        {{ automate_dv.prefix([dd], 's') }}{{ ', ' if not loop.last }}
        {%- endfor %}
    FROM {{ ref(source_model) }} AS s
    WHERE {{ automate_dv.multikey(src_pk, prefix='s', condition='IS NOT NULL') }}
),

{%- if satellites is mapping %}
{%- for sat_name, sat_config in satellites.items() %}
{%- set sat_pk = sat_config['pk'] %}
{%- set sat_ldts = sat_config['ldts'] %}
{%- set sat_measures = sat_config.get('measures', []) %}

{{ sat_name | lower }}_latest AS (
    SELECT * FROM (
        SELECT
            {{ automate_dv.prefix([sat_pk], 'sat') }},
            {%- for col in sat_measures %}
            sat.{{ col }},
            {%- endfor %}
            ROW_NUMBER() OVER (
                PARTITION BY {{ automate_dv.prefix([sat_pk], 'sat') }}
                ORDER BY {{ automate_dv.prefix([sat_ldts], 'sat') }} DESC
            ) AS row_num
        FROM {{ ref(sat_name) }} AS sat
        WHERE {{ automate_dv.multikey(sat_pk, prefix='sat', condition='IS NOT NULL') }}
    ) AS ranked
    WHERE row_num = 1
),

{%- endfor %}
{%- endif %}

fact_output AS (
    SELECT
        {{ automate_dv.prefix([src_pk], 'src') }},
        {{ automate_dv.prefix([src_ldts], 'src') }},
        {%- for fk in fk_list %}
        {{ automate_dv.prefix([fk], 'src') }},
        {%- endfor %}
        {%- for dd in degen_list %}
        {{ automate_dv.prefix([dd], 'src') }}{{ ', ' if not loop.last }}
        {%- endfor %}
        {%- if satellites is mapping %}
        {%- for sat_name, sat_config in satellites.items() %}
        {%- set sat_measures = sat_config.get('measures', []) %}
        {%- for col in sat_measures %},
        {{ sat_name | lower }}_latest.{{ col }}
        {%- endfor %}
        {%- endfor %}
        {%- endif %}
    FROM src
    {%- if satellites is mapping %}
    {%- for sat_name, sat_config in satellites.items() %}
    {%- set sat_pk = sat_config['pk'] %}
    LEFT JOIN {{ sat_name | lower }}_latest
        ON {{ automate_dv.prefix([src_pk], 'src') }} = {{ sat_name | lower }}_latest.{{ sat_pk }}
    {%- endfor %}
    {%- endif %}
)

SELECT * FROM fact_output
{%- if automate_dv.is_any_incremental() %}
WHERE {{ src_pk }} NOT IN (SELECT {{ src_pk }} FROM {{ this }})
{%- endif %}

{%- endmacro -%}
