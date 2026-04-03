{{ config(materialized='table') }}

{%- set src_pk = 'PRODUCT_HK' -%}
{%- set src_fk = [] -%}
{%- set src_ldts = 'LOAD_DATETIME' -%}
{%- set source_model = 'hub_product' -%}

{%- set dimensions = {
    'PRODUCT_HK': 'dim_product'
} -%}

{%- set satellites = {
    'sat_inventory_level': {
        'pk': 'PRODUCT_HK',
        'ldts': 'LOAD_DATETIME',
        'measures': ['QUANTITY_ON_HAND'],
        'semi_additive': ['QUANTITY_ON_HAND']
    }
} -%}

{{ automate_dv_kimball.fact_periodic_snapshot(src_pk=src_pk,
                                      src_fk=src_fk,
                                      src_ldts=src_ldts,
                                      source_model=source_model,
                                      dimensions=dimensions,
                                      satellites=satellites,
                                      snapshot_period='month') }}
