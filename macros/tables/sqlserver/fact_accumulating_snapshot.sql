/*
 * Copyright (c) 2026 automate-dv-kimball contributors
 * Licensed under the Apache License, Version 2.0
 */

{%- macro sqlserver__fact_accumulating_snapshot(src_pk, src_fk, source_model, dimensions, milestones, lag_facts, satellites, timespan, src_extra_columns) -%}

{%- set fk_list = automate_dv.expand_column_list(columns=[src_fk]) -%}
{%- set pk_expand = automate_dv.expand_column_list(columns=[src_pk]) -%}
{%- set max_datetime = automate_dv.max_datetime() -%}

WITH src AS (
    SELECT
        {{ automate_dv.prefix([src_pk], 's') }},
        {%- for fk in fk_list %}
        {{ automate_dv.prefix([fk], 's') }}{{ ', ' if not loop.last or automate_dv.is_something(src_extra_columns) }}
        {%- endfor %}
        {%- if automate_dv.is_something(src_extra_columns) %}
        {{ automate_dv.prefix(automate_dv.expand_column_list(columns=[src_extra_columns]), 's') }}
        {%- endif %}
    FROM {{ ref(source_model) }} AS s
    WHERE {{ automate_dv.multikey(src_pk, prefix='s', condition='IS NOT NULL') }}
),

{%- if milestones is mapping %}
{%- for mkey, mcfg in milestones.items() %}
{%- set m_pk_expand = automate_dv.expand_column_list(columns=[mcfg['pk']]) %}
{%- set m_date = mcfg['date_col'] %}
{%- set m_model = mcfg['model'] %}

milestone__{{ mkey }} AS (
    SELECT
        {%- for col in m_pk_expand %}
        ranked.{{ col }},
        {%- endfor %}
        ranked.{{ mkey }}
    FROM (
        SELECT
            {%- for col in m_pk_expand %}
            m.{{ col }},
            {%- endfor %}
            m.{{ m_date }} AS {{ mkey }},
            ROW_NUMBER() OVER (
                PARTITION BY {% for col in m_pk_expand %}m.{{ col }}{% if not loop.last %}, {% endif %}{% endfor %}
                ORDER BY m.{{ m_date }} DESC
            ) AS row_num
        FROM {{ ref(m_model) }} AS m
        WHERE {{ automate_dv.multikey(m_pk_expand, prefix='m', condition='IS NOT NULL') }}
    ) AS ranked
    WHERE ranked.row_num = 1
),

{%- endfor %}
{%- endif %}

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

fact_enriched AS (
    SELECT
        {{ automate_dv.prefix([src_pk], 'src') }},
        {%- for fk in fk_list %}
        {{ automate_dv.prefix([fk], 'src') }},
        {%- endfor %}
        {%- if automate_dv.is_something(src_extra_columns) %}
        {{ automate_dv.prefix(automate_dv.expand_column_list(columns=[src_extra_columns]), 'src') }},
        {%- endif %}
        {%- if milestones is mapping %}
        {%- for mkey, mcfg in milestones.items() %}
        ms_{{ mkey }}.{{ mkey }}{{ ',' if not loop.last }}
        {%- endfor %}
        {%- endif %}
        {%- if satellites is mapping %}
        {%- set sat_items = satellites.items() | list %}
        {%- for sat_name, sat_config in sat_items %}
        {%- set sat_measures = sat_config.get('measures', []) %}
        {%- for col in sat_measures %},
        {{ sat_name | lower }}_latest.{{ col }}
        {%- endfor %}
        {%- endfor %}
        {%- endif %}
    FROM src
    {%- if milestones is mapping %}
    {%- for mkey, mcfg in milestones.items() %}
    {%- set m_pk_expand = automate_dv.expand_column_list(columns=[mcfg['pk']]) %}
    LEFT JOIN milestone__{{ mkey }} AS ms_{{ mkey }}
        ON {{ automate_dv.multikey(m_pk_expand, prefix=['src', 'ms_' ~ mkey], condition='=') }}
    {%- endfor %}
    {%- endif %}
    {%- if satellites is mapping %}
    {%- for sat_name, sat_config in satellites.items() %}
    {%- set sat_pk = sat_config['pk'] %}
    LEFT JOIN {{ sat_name | lower }}_latest
        ON {{ automate_dv.multikey(src_pk, prefix=['src', sat_name | lower ~ '_latest'], condition='=') }}
    {%- endfor %}
    {%- endif %}
),

{%- set sel_ns = namespace(parts=[]) %}
{%- for col in pk_expand %}{% do sel_ns.parts.append('fe.' ~ col) %}{%- endfor %}
{%- for fk in fk_list %}{% do sel_ns.parts.append('fe.' ~ fk) %}{%- endfor %}
{%- if automate_dv.is_something(src_extra_columns) %}
{%- for col in automate_dv.expand_column_list(columns=[src_extra_columns]) %}{% do sel_ns.parts.append('fe.' ~ col) %}{%- endfor %}
{%- endif %}
{%- if milestones is mapping %}
{%- for mkey in milestones.keys() %}{% do sel_ns.parts.append('fe.' ~ mkey) %}{%- endfor %}
{%- endif %}
{%- if lag_facts is mapping %}
{%- for lname, lcfg in lag_facts.items() %}
{%- do sel_ns.parts.append("DATEDIFF(day, fe." ~ lcfg['from'] ~ ", fe." ~ lcfg['to'] ~ ") AS " ~ lname) %}
{%- endfor %}
{%- endif %}
{%- if satellites is mapping %}
{%- for sat_name, sat_config in satellites.items() %}
{%- for col in sat_config.get('measures', []) %}
{%- do sel_ns.parts.append('fe.' ~ col) %}
{%- endfor %}
{%- endfor %}
{%- endif %}

{%- if timespan %}

{#- Timespan mode: Type 2-like fact history with EFFECTIVE_FROM / EFFECTIVE_TO / IS_CURRENT -#}


fact_snapshot_base AS (
    SELECT
        {{ sel_ns.parts | join(',\n        ') }}
    FROM fact_enriched fe
),

{%- set milestone_keys = milestones.keys() | list if milestones is mapping else [] %}

fact_with_timespan AS (
    SELECT
        fsb.*,
        COALESCE(
            {%- for mkey in milestone_keys %}
            fsb.{{ mkey }}{{ ', ' if not loop.last }}
            {%- endfor %}
            {%- if not milestone_keys %}
            CAST(NULL AS DATETIME2)
            {%- endif %}
        ) AS EFFECTIVE_FROM,
        CAST('{{ max_datetime }}' AS DATETIME2) AS EFFECTIVE_TO,
        CAST(1 AS BIT) AS IS_CURRENT
    FROM fact_snapshot_base fsb
)

{%- if automate_dv.is_any_incremental() %}
,

closed_prior AS (
    SELECT
        {%- for col in pk_expand %}
        t.{{ col }},
        {%- endfor %}
        {%- for fk in fk_list %}
        t.{{ fk }},
        {%- endfor %}
        {%- if automate_dv.is_something(src_extra_columns) %}
        {%- for col in automate_dv.expand_column_list(columns=[src_extra_columns]) %}
        t.{{ col }},
        {%- endfor %}
        {%- endif %}
        {%- for mkey in milestone_keys %}
        t.{{ mkey }},
        {%- endfor %}
        {%- if lag_facts is mapping %}
        {%- for lname in lag_facts.keys() %}
        t.{{ lname }},
        {%- endfor %}
        {%- endif %}
        {%- if satellites is mapping %}
        {%- for sat_name, sat_config in satellites.items() %}
        {%- for col in sat_config.get('measures', []) %}
        t.{{ col }},
        {%- endfor %}
        {%- endfor %}
        {%- endif %}
        t.EFFECTIVE_FROM,
        DATEADD(microsecond, -1, nv.EFFECTIVE_FROM) AS EFFECTIVE_TO,
        CAST(0 AS BIT) AS IS_CURRENT
    FROM {{ this }} t
    INNER JOIN fact_with_timespan nv
        ON {{ automate_dv.multikey(pk_expand, prefix=['t', 'nv'], condition='=', operator='AND') }}
    WHERE t.IS_CURRENT = CAST(1 AS BIT)
    AND (
        {%- for mkey in milestone_keys %}
        NOT (t.{{ mkey }} = nv.{{ mkey }} OR (t.{{ mkey }} IS NULL AND nv.{{ mkey }} IS NULL))
        {%- if not loop.last %} OR {% endif %}
        {%- endfor %}
        {%- if not milestone_keys %}
        1 = 0
        {%- endif %}
    )
)

SELECT * FROM fact_with_timespan fwt
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} t
    WHERE {{ automate_dv.multikey(pk_expand, prefix=['t', 'fwt'], condition='=', operator='AND') }}
    AND t.IS_CURRENT = CAST(1 AS BIT)
    {%- for mkey in milestone_keys %}
    AND (t.{{ mkey }} = fwt.{{ mkey }} OR (t.{{ mkey }} IS NULL AND fwt.{{ mkey }} IS NULL))
    {%- endfor %}
)

UNION ALL

SELECT * FROM closed_prior

{%- else %}

SELECT * FROM fact_with_timespan

{%- endif %}

{%- else %}

fact_snapshot_output AS (
    SELECT
        {{ sel_ns.parts | join(',\n        ') }}
    FROM fact_enriched fe
)

SELECT * FROM fact_snapshot_output fs
{%- if automate_dv.is_any_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} t
    WHERE {{ automate_dv.multikey(pk_expand, prefix=['t', 'fs'], condition='=', operator='AND') }}
    {%- if milestones is mapping %}
    {%- for mkey in milestones.keys() %}
    AND (t.{{ mkey }} = fs.{{ mkey }} OR (t.{{ mkey }} IS NULL AND fs.{{ mkey }} IS NULL))
    {%- endfor %}
    {%- endif %}
)
{%- endif %}

{%- endif %}

{%- endmacro -%}
