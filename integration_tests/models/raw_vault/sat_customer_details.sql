{{ config(materialized='table') }}

{%- set src_pk = 'CUSTOMER_HK' -%}
{%- set src_hashdiff = {'source_column': 'CUSTOMER_HASHDIFF', 'alias': 'HASHDIFF'} -%}
{%- set src_payload = ['CUSTOMER_NAME', 'CUSTOMER_PHONE'] -%}
{%- set src_ldts = 'LOAD_DATETIME' -%}
{%- set src_source = 'SOURCE' -%}
{%- set source_model = 'stg_customers' -%}

{{ automate_dv.sat(src_pk=src_pk,
                   src_hashdiff=src_hashdiff,
                   src_payload=src_payload,
                   src_ldts=src_ldts,
                   src_source=src_source,
                   source_model=source_model) }}
