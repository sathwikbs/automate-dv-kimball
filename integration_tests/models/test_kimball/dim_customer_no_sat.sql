{{ config(materialized='table') }}

{%- set src_pk = 'CUSTOMER_HK' -%}
{%- set src_nk = 'CUSTOMER_ID' -%}
{%- set src_ldts = 'LOAD_DATETIME' -%}
{%- set source_model = 'hub_customer' -%}

{{ automate_dv_kimball.dim(src_pk=src_pk,
                   src_nk=src_nk,
                   src_ldts=src_ldts,
                   source_model=source_model,
                   scd_type=1) }}
