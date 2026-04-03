/*
 * Copyright (c) 2026 automate-dv-kimball contributors
 * Licensed under the Apache License, Version 2.0
 */

{%- macro sqlserver__fact_periodic_snapshot(src_pk, src_fk, src_ldts, source_model, dimensions, satellites, snapshot_period, src_extra_columns) -%}

{%- set fk_list = automate_dv.expand_column_list(columns=[src_fk]) -%}
{%- set src_pk_list = automate_dv.expand_column_list(columns=[src_pk]) -%}
{%- set src_extra_list = automate_dv.expand_column_list(columns=[src_extra_columns]) if automate_dv.is_something(src_extra_columns) else [] -%}
{%- set sat_entries = satellites.items() | list -%}
{%- set sp_lower = snapshot_period | lower -%}

{%- if satellites is not mapping or sat_entries | length == 0 -%}
    {%- if execute -%}
        {{- exceptions.raise_compiler_error("satellites must be a non-empty mapping for fact_periodic_snapshot in '{}'".format(this)) -}}
    {%- endif -%}
{%- endif -%}

{%- if satellites is mapping %}
{%- for sat_name, sat_config in sat_entries %}
{%- set sat_pk = sat_config['pk'] %}
{%- set sat_pk_list = automate_dv.expand_column_list(columns=[sat_pk]) %}
{%- set sat_ldts = sat_config['ldts'] %}
{%- set sat_measures = sat_config.get('measures', []) %}

{{ sat_name | lower }}_periodic AS (
    SELECT * FROM (
        SELECT
            {%- for col in sat_pk_list %}
            {{ automate_dv.prefix([col], 'sat') }},
            {%- endfor %}
            DATETRUNC({{ sp_lower }}, {{ automate_dv.prefix([sat_ldts], 'sat') }}) AS snapshot_date_key,
            {%- for col in sat_measures %}
            sat.{{ col }},
            {%- endfor %}
            ROW_NUMBER() OVER (
                PARTITION BY
                {%- for col in sat_pk_list %}
                    {{ automate_dv.prefix([col], 'sat') }}{% if not loop.last %}, {% endif %}
                {%- endfor %},
                    DATETRUNC({{ sp_lower }}, {{ automate_dv.prefix([sat_ldts], 'sat') }})
                ORDER BY {{ automate_dv.prefix([sat_ldts], 'sat') }} DESC
            ) AS row_num
        FROM {{ ref(sat_name) }} AS sat
        WHERE {{ automate_dv.multikey(sat_pk, prefix='sat', condition='IS NOT NULL') }}
    ) AS ranked
    WHERE row_num = 1
),

{%- endfor %}
{%- endif %}

src AS (
    SELECT
        {{ automate_dv.prefix(src_pk_list, 's') }},
        {{ automate_dv.prefix([src_ldts], 's') }},
        {%- for fk in fk_list %}
        {{ automate_dv.prefix([fk], 's') }}{{ ', ' if not loop.last or src_extra_list | length > 0 }}
        {%- endfor %}
        {%- if src_extra_list | length > 0 %}
        {{ automate_dv.prefix(src_extra_list, 's') }}
        {%- endif %}
    FROM {{ ref(source_model) }} AS s
    WHERE {{ automate_dv.multikey(src_pk, prefix='s', condition='IS NOT NULL') }}
),

joined_link_satellite AS (
    {%- set select_cols = [] -%}
    {%- do select_cols.append('sp0.snapshot_date_key') -%}
    {%- for col in src_pk_list %}
        {%- do select_cols.append('src.' ~ col) -%}
    {%- endfor %}
    {%- for fk in fk_list %}
        {%- do select_cols.append('src.' ~ fk) -%}
    {%- endfor %}
    {%- for ex in src_extra_list %}
        {%- do select_cols.append('src.' ~ ex) -%}
    {%- endfor %}
    {%- for sat_name_inner, sat_config_inner in sat_entries %}
    {%- set alias_inner = 'sp' ~ loop.index0 %}
    {%- for col in sat_config_inner.get('measures', []) %}
        {%- do select_cols.append(alias_inner ~ '.' ~ col) -%}
    {%- endfor %}
    {%- endfor %}
    SELECT
        {{ select_cols | join(',\n        ') }}
    FROM {{ sat_entries[0][0] | lower }}_periodic AS sp0
    INNER JOIN src AS src
        ON {% for col in src_pk_list %}src.{{ col }} = sp0.{{ col }}{% if not loop.last %} AND {% endif %}{% endfor %}
    {%- for sat_name, sat_config in sat_entries %}
    {%- if not loop.first %}
    {%- set sp_alias = 'sp' ~ loop.index0 %}
    LEFT JOIN {{ sat_name | lower }}_periodic AS {{ sp_alias }}
        ON {% for col in src_pk_list %}src.{{ col }} = {{ sp_alias }}.{{ col }}{% if not loop.last %} AND {% endif %}{% endfor %}
        AND sp0.snapshot_date_key = {{ sp_alias }}.snapshot_date_key
    {%- endif %}
    {%- endfor %}
),

fact_output AS (
{%- for sat_name, sat_config in sat_entries %}
{%- for col in sat_config.get('semi_additive', []) %}
    -- Semi-additive: {{ col }} (do not SUM across date periods; use AVG or latest)
{%- endfor %}
{%- endfor %}
    SELECT * FROM joined_link_satellite
)

SELECT * FROM fact_output
{%- if automate_dv.is_any_incremental() %}
WHERE snapshot_date_key > (SELECT MAX(snapshot_date_key) FROM {{ this }})
{%- endif %}

{%- endmacro -%}
