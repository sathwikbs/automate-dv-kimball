{{ config(materialized='table') }}

{# Tests fact_accumulating_snapshot() with 3 milestones (order, ship, delivery).
   Current tests only have 2 milestones. The plan shows 3 as the reference pattern. #}

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
    },
    'DELIVERY_DATE': {
        'model': 'sat_order_delivered',
        'pk': 'ORDER_HK',
        'date_col': 'LOAD_DATETIME',
        'dim': 'dim_date'
    }
} -%}

{%- set lag_facts = {
    'ORDER_TO_SHIP_DAYS': {
        'from': 'ORDER_DATE',
        'to': 'SHIP_DATE'
    },
    'SHIP_TO_DELIVERY_DAYS': {
        'from': 'SHIP_DATE',
        'to': 'DELIVERY_DATE'
    },
    'ORDER_TO_DELIVERY_DAYS': {
        'from': 'ORDER_DATE',
        'to': 'DELIVERY_DATE'
    }
} -%}

{%- set satellites = {
    'sat_order_details': {
        'pk': 'ORDER_HK',
        'ldts': 'LOAD_DATETIME',
        'measures': ['ORDER_AMOUNT']
    }
} -%}

{{ automate_dv_kimball.fact_accumulating_snapshot(src_pk=src_pk,
                                          src_fk=src_fk,
                                          source_model=source_model,
                                          dimensions=dimensions,
                                          milestones=milestones,
                                          lag_facts=lag_facts,
                                          satellites=satellites) }}
