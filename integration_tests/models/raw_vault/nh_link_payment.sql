{{ config(materialized='table') }}

{%- set src_pk = 'PAYMENT_HK' -%}
{%- set src_fk = ['ORDER_HK', 'CUSTOMER_HK'] -%}
{%- set src_payload = ['PAYMENT_AMOUNT', 'PAYMENT_METHOD'] -%}
{%- set src_eff = 'LOAD_DATETIME' -%}
{%- set src_ldts = 'LOAD_DATETIME' -%}
{%- set src_source = 'SOURCE' -%}
{%- set source_model = 'stg_payments' -%}

{{ automate_dv.nh_link(src_pk=src_pk,
                       src_fk=src_fk,
                       src_payload=src_payload,
                       src_eff=src_eff,
                       src_ldts=src_ldts,
                       src_source=src_source,
                       source_model=source_model) }}
