{{ config(materialized='table') }}

{# Tests dim() with a BV same_as_link as source_model.
   Proves dim() does not care whether the hub is raw or BV-deduplicated. #}

{%- set src_pk = 'CUSTOMER_HK' -%}
{%- set src_nk = 'CUSTOMER_ID' -%}
{%- set src_ldts = 'LOAD_DATETIME' -%}
{%- set source_model = 'bv_customer_deduped' -%}

{%- set satellites = {
    'sat_customer_details': {
        'pk': 'CUSTOMER_HK',
        'ldts': 'LOAD_DATETIME',
        'payload': ['CUSTOMER_NAME', 'CUSTOMER_PHONE']
    }
} -%}

{{ automate_dv_kimball.dim(src_pk=src_pk,
                   src_nk=src_nk,
                   src_ldts=src_ldts,
                   source_model=source_model,
                   satellites=satellites,
                   scd_type=1) }}
