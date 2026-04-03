{{ config(materialized='table') }}

{%- set src_pk = 'ORDER_HK' -%}
{%- set src_nk = 'ORDER_ID' -%}
{%- set src_ldts = 'LOAD_DATETIME' -%}
{%- set src_source = 'SOURCE' -%}
{%- set source_model = 'stg_orders' -%}

{{ automate_dv.hub(src_pk=src_pk,
                   src_nk=src_nk,
                   src_ldts=src_ldts,
                   src_source=src_source,
                   source_model=source_model) }}
