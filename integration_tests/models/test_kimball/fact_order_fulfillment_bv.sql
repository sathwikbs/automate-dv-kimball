{{ config(materialized='table') }}

{# Tests fact_accumulating_snapshot() with BV pre-resolved milestones.
   Proves the macro accepts any model with the right columns as milestone source. #}

{%- set src_pk = 'ORDER_HK' -%}
{%- set src_fk = ['CUSTOMER_HK'] -%}
{%- set source_model = 'link_order' -%}

{%- set dimensions = {
    'CUSTOMER_HK': 'dim_customer'
} -%}

{%- set milestones = {
    'ORDER_DATE': {
        'model': 'bv_order_milestones',
        'pk': 'ORDER_HK',
        'date_col': 'ORDER_PLACED_DATE',
        'dim': 'dim_date'
    },
    'SHIP_DATE': {
        'model': 'bv_order_milestones',
        'pk': 'ORDER_HK',
        'date_col': 'ORDER_SHIPPED_DATE',
        'dim': 'dim_date'
    }
} -%}

{%- set lag_facts = {
    'ORDER_TO_SHIP_DAYS': {
        'from': 'ORDER_DATE',
        'to': 'SHIP_DATE'
    }
} -%}

{{ automate_dv_kimball.fact_accumulating_snapshot(src_pk=src_pk,
                                          src_fk=src_fk,
                                          source_model=source_model,
                                          dimensions=dimensions,
                                          milestones=milestones,
                                          lag_facts=lag_facts) }}
