{{ config(materialized='table') }}

{# Tests dim() with a BV computed_sat as satellite source.
   Proves the column-contract philosophy: dim() does not care
   whether the satellite is raw or BV-derived. #}

{%- set src_pk = 'CUSTOMER_HK' -%}
{%- set src_nk = 'CUSTOMER_ID' -%}
{%- set src_ldts = 'LOAD_DATETIME' -%}
{%- set source_model = 'hub_customer' -%}

{%- set satellites = {
    'sat_customer_details': {
        'pk': 'CUSTOMER_HK',
        'ldts': 'LOAD_DATETIME',
        'payload': ['CUSTOMER_NAME', 'CUSTOMER_PHONE']
    },
    'bv_customer_segment': {
        'pk': 'CUSTOMER_HK',
        'ldts': 'LOAD_DATETIME',
        'payload': ['CUSTOMER_SEGMENT', 'LIFETIME_VALUE']
    }
} -%}

{{ automate_dv_kimball.dim(src_pk=src_pk,
                   src_nk=src_nk,
                   src_ldts=src_ldts,
                   source_model=source_model,
                   satellites=satellites,
                   scd_type=1) }}
