{{ config(materialized='table') }}

{%- set bm = automate_dv_kimball.bus_matrix() -%}
{%- set dims = bm['dimensions'] -%}
{%- set facts = bm['facts'] -%}
{%- set bridges = bm['bridges'] -%}

SELECT
    '{{ dims | length }}' AS dimension_count,
    '{{ facts | length }}' AS fact_count,
    '{{ bridges | length }}' AS bridge_count,
    '{{ dims.keys() | list | join(",") }}' AS dimension_names,
    '{{ facts.keys() | list | join(",") }}' AS fact_names,
    '{{ bridges.keys() | list | join(",") }}' AS bridge_names
