/*
 * Copyright (c) 2026 automate-dv-kimball contributors
 * Licensed under the Apache License, Version 2.0
 */

{%- macro fact_accumulating_snapshot(src_pk, src_fk, source_model, dimensions, milestones, lag_facts, satellites=none, timespan=false, src_extra_columns=none) -%}

    {{- automate_dv.check_required_parameters(src_pk=src_pk, src_fk=src_fk,
                                              source_model=source_model,
                                              dimensions=dimensions,
                                              milestones=milestones,
                                              lag_facts=lag_facts) -}}

    {{- automate_dv.prepend_generated_by() }}

    {#- Emit ref() for each unique dimension to establish DAG edges -#}
    {%- set emitted_dims = [] -%}
    {%- if dimensions is mapping -%}
        {%- for fk, dim_ref in dimensions.items() -%}
            {%- if dim_ref is mapping -%}
                {%- set dim_name = dim_ref['dim'] -%}
            {%- else -%}
                {%- set dim_name = dim_ref -%}
            {%- endif -%}
            {%- if dim_name not in emitted_dims -%}
                {%- do emitted_dims.append(dim_name) %}
-- depends_on: {{ ref(dim_name) }}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}
    {%- if milestones is mapping -%}
        {%- for mname, mcfg in milestones.items() -%}
            {%- set dim_name = mcfg['dim'] -%}
            {%- if dim_name not in emitted_dims -%}
                {%- do emitted_dims.append(dim_name) %}
-- depends_on: {{ ref(dim_name) }}
            {%- endif -%}
        {%- endfor -%}
    {%- endif %}

    {{ adapter.dispatch('fact_accumulating_snapshot', 'automate_dv_kimball')(src_pk=src_pk, src_fk=src_fk,
                                                                     source_model=source_model,
                                                                     dimensions=dimensions,
                                                                     milestones=milestones,
                                                                     lag_facts=lag_facts,
                                                                     satellites=satellites,
                                                                     timespan=timespan,
                                                                     src_extra_columns=src_extra_columns) -}}

{%- endmacro -%}


{%- macro default__fact_accumulating_snapshot(src_pk, src_fk, source_model, dimensions, milestones, lag_facts, satellites, timespan, src_extra_columns) -%}

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
        m.{{ col }},
        {%- endfor %}
        m.{{ m_date }} AS {{ mkey }}
    FROM {{ ref(m_model) }} AS m
    WHERE {{ automate_dv.multikey(m_pk_expand, prefix='m', condition='IS NOT NULL') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY {% for col in m_pk_expand %}m.{{ col }}{% if not loop.last %}, {% endif %}{% endfor %}
        ORDER BY m.{{ m_date }} DESC
    ) = 1
),

{%- endfor %}
{%- endif %}

{%- if satellites is mapping %}
{%- for sat_name, sat_config in satellites.items() %}
{%- set sat_pk = sat_config['pk'] %}
{%- set sat_ldts = sat_config['ldts'] %}
{%- set sat_measures = sat_config.get('measures', []) %}

{{ sat_name | lower }}_latest AS (
    SELECT
        {{ automate_dv.prefix([sat_pk], 'sat') }},
        {%- for col in sat_measures %}
        sat.{{ col }}{{ ', ' if not loop.last }}
        {%- endfor %}
    FROM {{ ref(sat_name) }} AS sat
    WHERE {{ automate_dv.multikey(sat_pk, prefix='sat', condition='IS NOT NULL') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY {{ automate_dv.prefix([sat_pk], 'sat') }}
        ORDER BY {{ automate_dv.prefix([sat_ldts], 'sat') }} DESC
    ) = 1
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
{%- if target.type in ['databricks', 'spark'] %}
{%- do sel_ns.parts.append("DATEDIFF(DAY, fe." ~ lcfg['from'] ~ ", fe." ~ lcfg['to'] ~ ") AS " ~ lname) %}
{%- elif target.type == 'bigquery' %}
{%- do sel_ns.parts.append("DATE_DIFF(fe." ~ lcfg['to'] ~ ", fe." ~ lcfg['from'] ~ ", DAY) AS " ~ lname) %}
{%- elif target.type == 'sqlserver' %}
{%- do sel_ns.parts.append("DATEDIFF(DAY, fe." ~ lcfg['from'] ~ ", fe." ~ lcfg['to'] ~ ") AS " ~ lname) %}
{%- else %}
{%- do sel_ns.parts.append("DATEDIFF('day', fe." ~ lcfg['from'] ~ ", fe." ~ lcfg['to'] ~ ") AS " ~ lname) %}
{%- endif %}
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

{#- Timespan mode: Type 2-like fact history with EFFECTIVE_FROM / EFFECTIVE_TO / IS_CURRENT.
    On initial load: all rows get EFFECTIVE_FROM, EFFECTIVE_TO=max_datetime, IS_CURRENT=true.
    On incremental: new rows that differ from the current version are inserted as current;
    prior current rows for the same PK are closed via UNION ALL. -#}


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
            CAST(NULL AS TIMESTAMP)
            {%- endif %}
        ) AS EFFECTIVE_FROM,
        {{ automate_dv.cast_date(max_datetime, as_string=true, datetime=true) }} AS EFFECTIVE_TO,
        TRUE AS IS_CURRENT
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
        {%- if target.type in ['databricks', 'spark'] %}
        {{ automate_dv.dateadd('microsecond', -1, 'nv.EFFECTIVE_FROM') }} AS EFFECTIVE_TO,
        {%- elif target.type == 'bigquery' %}
        TIMESTAMP_SUB(nv.EFFECTIVE_FROM, INTERVAL 1 MICROSECOND) AS EFFECTIVE_TO,
        {%- else %}
        {{ automate_dv.dateadd('microsecond', -1, 'nv.EFFECTIVE_FROM') }} AS EFFECTIVE_TO,
        {%- endif %}
        FALSE AS IS_CURRENT
    FROM {{ this }} t
    INNER JOIN fact_with_timespan nv
        ON {{ automate_dv.multikey(pk_expand, prefix=['t', 'nv'], condition='=', operator='AND') }}
    WHERE t.IS_CURRENT = TRUE
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
    AND t.IS_CURRENT = TRUE
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
