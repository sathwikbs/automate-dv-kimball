/*
 * Copyright (c) 2026 automate-dv-kimball contributors
 * Licensed under the Apache License, Version 2.0
 */

{%- macro postgres__dim(src_pk, src_nk, src_ldts, source_model, satellites, scd_type, src_extra_columns, integer_surrogate) -%}

{%- set max_datetime = automate_dv.max_datetime() -%}

WITH src AS (
    SELECT
        {{ automate_dv.prefix([src_pk], 's') }},
        {{ automate_dv.prefix([src_nk], 's') }}
        {%- if automate_dv.is_something(src_extra_columns) %},
        {{ automate_dv.prefix(automate_dv.expand_column_list(columns=[src_extra_columns]), 's') }}
        {%- endif %}
    FROM {{ ref(source_model) }} AS s
    WHERE {{ automate_dv.multikey(src_pk, prefix='s', condition='IS NOT NULL') }}
),

{%- if satellites is mapping %}
{%- for sat_name, sat_config in satellites.items() %}
{%- set sat_pk = sat_config['pk'] %}
{%- set sat_ldts = sat_config['ldts'] %}
{%- set sat_payload = sat_config.get('payload', []) %}

{{ sat_name | lower }}_ranked AS (
    SELECT DISTINCT ON ({{ automate_dv.prefix([sat_pk], 'sat') }})
        {{ automate_dv.prefix([sat_pk], 'sat') }},
        {{ automate_dv.prefix([sat_ldts], 'sat') }},
        {%- for col in sat_payload %}
        sat.{{ col }}{{ ', ' if not loop.last }}
        {%- endfor %}
    FROM {{ ref(sat_name) }} AS sat
    WHERE {{ automate_dv.multikey(sat_pk, prefix='sat', condition='IS NOT NULL') }}
    {%- if scd_type == 1 %}
    ORDER BY {{ automate_dv.prefix([sat_pk], 'sat') }}, {{ automate_dv.prefix([sat_ldts], 'sat') }} DESC
    {%- else %}
    ORDER BY {{ automate_dv.prefix([sat_pk], 'sat') }}, {{ automate_dv.prefix([sat_ldts], 'sat') }} ASC
    {%- endif %}
),

{%- endfor %}
{%- endif %}

{%- if scd_type == 1 %}

dim_output AS (
    SELECT
        {%- if integer_surrogate %}
        DENSE_RANK() OVER (ORDER BY {{ automate_dv.prefix([src_pk], 'src') }}) AS DIM_SK,
        {%- endif %}
        {{ automate_dv.prefix([src_pk], 'src') }},
        {{ automate_dv.prefix([src_nk], 'src') }}
        {%- if automate_dv.is_something(src_extra_columns) %},
        {{ automate_dv.prefix(automate_dv.expand_column_list(columns=[src_extra_columns]), 'src') }}
        {%- endif %}
        {%- if satellites is mapping %}
        {%- for sat_name, sat_config in satellites.items() %}
        {%- set sat_payload = sat_config.get('payload', []) %}
        {%- for col in sat_payload %},
        {{ sat_name | lower }}_ranked.{{ col }}
        {%- endfor %}
        {%- endfor %}
        {%- endif %}
    FROM src
    {%- if satellites is mapping %}
    {%- for sat_name, sat_config in satellites.items() %}
    {%- set sat_pk = sat_config['pk'] %}
    LEFT JOIN {{ sat_name | lower }}_ranked
        ON {{ automate_dv.prefix([src_pk], 'src') }} = {{ sat_name | lower }}_ranked.{{ sat_pk }}
    {%- endfor %}
    {%- endif %}
)

{%- elif scd_type == 2 %}

sat_timeline AS (
    {%- if satellites is mapping %}
    {%- set sat_list = satellites.keys() | list %}
    {%- set first_sat = sat_list[0] %}
    {%- set first_config = satellites[first_sat] %}
    SELECT
        sat.{{ first_config['pk'] }} AS {{ src_pk }},
        sat.{{ first_config['ldts'] }} AS EFFECTIVE_FROM
    FROM {{ ref(first_sat) }} AS sat
    {%- for sat_name in sat_list[1:] %}
    {%- set sat_config = satellites[sat_name] %}
    UNION
    SELECT
        sat.{{ sat_config['pk'] }} AS {{ src_pk }},
        sat.{{ sat_config['ldts'] }} AS EFFECTIVE_FROM
    FROM {{ ref(sat_name) }} AS sat
    {%- endfor %}
    {%- endif %}
),

scd2_windows AS (
    SELECT
        {{ src_pk }},
        EFFECTIVE_FROM,
        LEAD(EFFECTIVE_FROM) OVER (
            PARTITION BY {{ src_pk }}
            ORDER BY EFFECTIVE_FROM
        ) AS NEXT_EFFECTIVE_FROM
    FROM sat_timeline
),

scd2_versioned AS (
    SELECT
        w.{{ src_pk }},
        w.EFFECTIVE_FROM,
        COALESCE(
            {{ automate_dv.dateadd('microsecond', -1, 'w.NEXT_EFFECTIVE_FROM') }},
            {{ automate_dv.cast_date(max_datetime, as_string=true, datetime=true) }}
        ) AS EFFECTIVE_TO,
        CASE
            WHEN w.NEXT_EFFECTIVE_FROM IS NULL THEN TRUE
            ELSE FALSE
        END AS IS_CURRENT
    FROM scd2_windows AS w
),

{%- if satellites is mapping %}
{%- for sat_name, sat_config in satellites.items() %}
{%- set sat_pk = sat_config['pk'] %}
{%- set sat_ldts = sat_config['ldts'] %}
{%- set sat_payload = sat_config.get('payload', []) %}

{{ sat_name | lower }}_as_of AS (
    SELECT DISTINCT ON (v.{{ src_pk }}, v.EFFECTIVE_FROM)
        v.{{ src_pk }},
        v.EFFECTIVE_FROM,
        {%- for col in sat_payload %}
        {{ sat_name | lower }}_ranked.{{ col }}{{ ', ' if not loop.last }}
        {%- endfor %}
    FROM scd2_versioned AS v
    LEFT JOIN {{ sat_name | lower }}_ranked
        ON v.{{ src_pk }} = {{ sat_name | lower }}_ranked.{{ sat_pk }}
        AND {{ sat_name | lower }}_ranked.{{ sat_ldts }} <= v.EFFECTIVE_FROM
    ORDER BY v.{{ src_pk }}, v.EFFECTIVE_FROM, {{ sat_name | lower }}_ranked.{{ sat_ldts }} DESC
),

{%- endfor %}
{%- endif %}

dim_output AS (
    SELECT
        {%- if integer_surrogate %}
        DENSE_RANK() OVER (ORDER BY {{ automate_dv.prefix([src_pk], 'src') }}, v.EFFECTIVE_FROM) AS DIM_SK,
        {%- endif %}
        {{ automate_dv.prefix([src_pk], 'src') }},
        {{ automate_dv.prefix([src_nk], 'src') }}
        {%- if automate_dv.is_something(src_extra_columns) %},
        {{ automate_dv.prefix(automate_dv.expand_column_list(columns=[src_extra_columns]), 'src') }}
        {%- endif %},
        v.EFFECTIVE_FROM,
        v.EFFECTIVE_TO,
        v.IS_CURRENT
        {%- if satellites is mapping %}
        {%- for sat_name, sat_config in satellites.items() %}
        {%- set sat_payload = sat_config.get('payload', []) %}
        {%- for col in sat_payload %},
        {{ sat_name | lower }}_as_of.{{ col }}
        {%- endfor %}
        {%- endfor %}
        {%- endif %}
    FROM src
    INNER JOIN scd2_versioned AS v
        ON {{ automate_dv.prefix([src_pk], 'src') }} = v.{{ src_pk }}
    {%- if satellites is mapping %}
    {%- for sat_name, sat_config in satellites.items() %}
    LEFT JOIN {{ sat_name | lower }}_as_of
        ON v.{{ src_pk }} = {{ sat_name | lower }}_as_of.{{ src_pk }}
        AND v.EFFECTIVE_FROM = {{ sat_name | lower }}_as_of.EFFECTIVE_FROM
    {%- endfor %}
    {%- endif %}
)

{%- endif %}

SELECT * FROM dim_output

{%- endmacro -%}
