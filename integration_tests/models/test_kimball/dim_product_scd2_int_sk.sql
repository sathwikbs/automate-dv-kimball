{{ config(materialized='table') }}

{# Tests dim() with SCD2 + integer_surrogate combined.
   These were tested separately but never together. #}

{%- set src_pk = 'PRODUCT_HK' -%}
{%- set src_nk = 'PRODUCT_ID' -%}
{%- set src_ldts = 'LOAD_DATETIME' -%}
{%- set source_model = 'hub_product' -%}

{%- set satellites = {
    'sat_product_details': {
        'pk': 'PRODUCT_HK',
        'ldts': 'LOAD_DATETIME',
        'payload': ['PRODUCT_NAME', 'UNIT_PRICE'],
        'hashdiff': 'HASHDIFF'
    }
} -%}

{{ automate_dv_kimball.dim(src_pk=src_pk,
                   src_nk=src_nk,
                   src_ldts=src_ldts,
                   source_model=source_model,
                   satellites=satellites,
                   scd_type=2,
                   integer_surrogate=true) }}
