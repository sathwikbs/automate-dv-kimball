{{ config(materialized='table') }}

{%- set src_pk = 'PROMO_COV_HK' -%}
{%- set src_fk = ['PRODUCT_HK', 'CUSTOMER_HK'] -%}
{%- set src_ldts = 'LOAD_DATETIME' -%}
{%- set src_source = 'SOURCE' -%}
{%- set source_model = 'stg_promotions' -%}

{{ automate_dv.link(src_pk=src_pk,
                    src_fk=src_fk,
                    src_ldts=src_ldts,
                    src_source=src_source,
                    source_model=source_model) }}
