/*
 * Copyright (c) 2026 automate-dv-kimball contributors
 * Licensed under the Apache License, Version 2.0
 */

{%- macro fact_factless(src_pk, src_fk, src_ldts, source_model, dimensions, include_count=true, src_extra_columns=none) -%}

    {{- automate_dv.check_required_parameters(src_pk=src_pk, src_fk=src_fk,
                                              src_ldts=src_ldts,
                                              source_model=source_model) -}}

    {{- automate_dv.prepend_generated_by() }}

    {#- Emit ref() for each dimension to establish DAG edges -#}
    {%- if dimensions is mapping -%}
        {%- set emitted_dims = [] -%}
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
    {%- endif %}

    {{ adapter.dispatch('fact_factless', 'automate_dv_kimball')(src_pk=src_pk, src_fk=src_fk,
                                                         src_ldts=src_ldts,
                                                         source_model=source_model,
                                                         dimensions=dimensions,
                                                         include_count=include_count,
                                                         src_extra_columns=src_extra_columns) -}}

{%- endmacro -%}


{%- macro default__fact_factless(src_pk, src_fk, src_ldts, source_model, dimensions, include_count, src_extra_columns) -%}

{%- set fk_list = automate_dv.expand_column_list(columns=[src_fk]) -%}

WITH src AS (
    SELECT
        {{ automate_dv.prefix([src_pk], 's') }},
        {%- for fk in fk_list %}
        {{ automate_dv.prefix([fk], 's') }},
        {%- endfor %}
        {{ automate_dv.prefix([src_ldts], 's') }}
        {%- if automate_dv.is_something(src_extra_columns) %},
        {{ automate_dv.prefix(automate_dv.expand_column_list(columns=[src_extra_columns]), 's') }}
        {%- endif %}
        {%- if include_count %},
        1 AS RECORD_COUNT
        {%- endif %}
    FROM {{ ref(source_model) }} AS s
    WHERE {{ automate_dv.multikey(src_pk, prefix='s', condition='IS NOT NULL') }}
)

SELECT * FROM src
{%- if automate_dv.is_any_incremental() %}
WHERE {{ src_pk }} NOT IN (SELECT {{ src_pk }} FROM {{ this }})
{%- endif %}

{%- endmacro -%}
