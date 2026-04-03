/*
 * Copyright (c) 2026 automate-dv-kimball contributors
 * Licensed under the Apache License, Version 2.0
 */

{%- macro sqlserver__fact_factless(src_pk, src_fk, src_ldts, source_model, dimensions, include_count, src_extra_columns) -%}

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
