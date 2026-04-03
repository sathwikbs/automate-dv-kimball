{{ config(materialized='table') }}

{%- set src_pk = 'PRODUCT_HK' -%}
{%- set src_hashdiff = {'source_column': 'PRODUCT_HASHDIFF', 'alias': 'HASHDIFF'} -%}
{%- set src_payload = ['PRODUCT_NAME', 'UNIT_PRICE'] -%}
{%- set src_ldts = 'LOAD_DATETIME' -%}
{%- set src_source = 'SOURCE' -%}
{%- set source_model = 'stg_products' -%}

{{ automate_dv.sat(src_pk=src_pk,
                   src_hashdiff=src_hashdiff,
                   src_payload=src_payload,
                   src_ldts=src_ldts,
                   src_source=src_source,
                   source_model=source_model) }}
