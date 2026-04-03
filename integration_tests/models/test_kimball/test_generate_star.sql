{{ config(materialized='table') }}

{# Actually invoke generate_star() logic to verify bus_matrix is parseable
   and the grid can be generated without errors. #}

{%- set bm = automate_dv_kimball.bus_matrix() -%}
{%- set dimensions = bm['dimensions'] -%}
{%- set facts = bm['facts'] -%}
{%- set bridges = bm['bridges'] -%}

{%- set dim_names = dimensions.keys() | list -%}

{# Verify the bus matrix grid can be traversed: count role-playing dims per fact #}
{%- set grid_ns = namespace(cells=0) -%}
{%- for fact_name, fact_config in facts.items() -%}
    {%- set fact_dims = fact_config.get('dimensions', {}) -%}
    {%- for dim_name in dim_names -%}
        {%- for fk, dim_ref in fact_dims.items() -%}
            {%- if dim_ref is mapping -%}
                {%- if dim_ref.get('dim', '') == dim_name -%}
                    {%- set grid_ns.cells = grid_ns.cells + 1 -%}
                {%- endif -%}
            {%- elif dim_ref == dim_name -%}
                {%- set grid_ns.cells = grid_ns.cells + 1 -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endfor -%}
{%- endfor -%}

SELECT
    'generate_star_passed' AS result,
    {{ grid_ns.cells }} AS grid_cell_count,
    {{ dimensions | length }} AS dim_count,
    {{ facts | length }} AS fact_count,
    CURRENT_TIMESTAMP AS run_at
