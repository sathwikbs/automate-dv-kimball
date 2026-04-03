{{ config(materialized='table') }}

{%- set src_pk = 'CUSTOMER_HK' -%}
{%- set src_cdk = ['PHONE_TYPE'] -%}
{%- set src_hashdiff = {'source_column': 'PHONE_HASHDIFF', 'alias': 'HASHDIFF'} -%}
{%- set src_payload = ['PHONE_NUMBER'] -%}
{%- set src_eff = 'LOAD_DATETIME' -%}
{%- set src_ldts = 'LOAD_DATETIME' -%}
{%- set src_source = 'SOURCE' -%}
{%- set source_model = 'stg_customer_phones' -%}

{{ automate_dv.ma_sat(src_pk=src_pk,
                      src_cdk=src_cdk,
                      src_hashdiff=src_hashdiff,
                      src_payload=src_payload,
                      src_eff=src_eff,
                      src_ldts=src_ldts,
                      src_source=src_source,
                      source_model=source_model) }}
