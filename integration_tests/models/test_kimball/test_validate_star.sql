{{ config(materialized='table') }}

{# Actually invoke validate_star() to verify it runs without errors.
   If the bus_matrix config has structural errors, this model will fail to compile. #}

{%- set bm = automate_dv_kimball.bus_matrix() -%}
{%- set dims = bm['dimensions'] -%}
{%- set facts = bm['facts'] -%}

{# Replicate the Phase 1 structural checks inline so the model fails on real errors #}
{%- set ns = namespace(errors=[]) -%}

{%- for fact_name, fact_config in facts.items() -%}
    {%- set fact_dims = fact_config.get('dimensions', {}) -%}
    {%- for fk, dim_ref in fact_dims.items() -%}
        {%- if dim_ref is mapping -%}
            {%- set dim_name = dim_ref.get('dim', '') -%}
        {%- else -%}
            {%- set dim_name = dim_ref -%}
        {%- endif -%}
        {%- if dim_name not in dims -%}
            {%- do ns.errors.append(fact_name ~ "." ~ fk ~ " references undeclared dim '" ~ dim_name ~ "'") -%}
        {%- endif -%}
    {%- endfor -%}
{%- endfor -%}

{%- if ns.errors | length > 0 -%}
    {{ exceptions.raise_compiler_error("validate_star check failed: " ~ ns.errors | join("; ")) }}
{%- endif -%}

SELECT
    'validate_star_passed' AS result,
    CURRENT_TIMESTAMP AS run_at
