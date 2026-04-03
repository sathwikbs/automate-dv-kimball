{{ config(materialized='table') }}

{%- set src_pk = 'ORDER_HK' -%}
{%- set src_fk = ['CUSTOMER_HK'] -%}
{%- set source_model = 'link_order' -%}

{%- set dimensions = {
    'CUSTOMER_HK': 'dim_customer'
} -%}

{%- set milestones = {
    'ORDER_DATE': {
        'model': 'sat_order_placed',
        'pk': 'ORDER_HK',
        'date_col': 'LOAD_DATETIME',
        'dim': 'dim_date'
    },
    'SHIP_DATE': {
        'model': 'sat_order_shipped',
        'pk': 'ORDER_HK',
        'date_col': 'LOAD_DATETIME',
        'dim': 'dim_date'
    }
} -%}

{%- set lag_facts = {
    'ORDER_TO_SHIP_DAYS': {
        'from': 'ORDER_DATE',
        'to': 'SHIP_DATE'
    }
} -%}

{%- set satellites = {
    'sat_order_details': {
        'pk': 'ORDER_HK',
        'ldts': 'LOAD_DATETIME',
        'measures': ['ORDER_AMOUNT', 'ORDER_QUANTITY']
    }
} -%}

{{ automate_dv_kimball.fact_accumulating_snapshot(src_pk=src_pk,
                                          src_fk=src_fk,
                                          source_model=source_model,
                                          dimensions=dimensions,
                                          milestones=milestones,
                                          lag_facts=lag_facts,
                                          satellites=satellites) }}
