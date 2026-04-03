{{ config(materialized='table') }}

{%- set src_pk = 'PROMO_COV_HK' -%}
{%- set src_fk = ['PRODUCT_HK', 'CUSTOMER_HK'] -%}
{%- set src_ldts = 'LOAD_DATETIME' -%}
{%- set source_model = 'link_promotion_coverage' -%}

{%- set dimensions = {
    'PRODUCT_HK': 'dim_product',
    'CUSTOMER_HK': 'dim_customer'
} -%}

{{ automate_dv_kimball.fact_factless(src_pk=src_pk,
                              src_fk=src_fk,
                              src_ldts=src_ldts,
                              source_model=source_model,
                              dimensions=dimensions,
                              include_count=true) }}
