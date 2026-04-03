{{ config(materialized='table') }}

{%- set src_pk = 'ORDER_HK' -%}
{%- set src_fk = ['CUSTOMER_HK', 'PRODUCT_HK'] -%}
{%- set src_ldts = 'LOAD_DATETIME' -%}
{%- set source_model = 'link_order' -%}

{%- set dimensions = {
    'CUSTOMER_HK': 'dim_customer',
    'PRODUCT_HK': 'dim_product',
    'ORDER_DATE_KEY': {'dim': 'dim_date', 'role': 'order_date'}
} -%}

{%- set satellites = {
    'sat_order_details': {
        'pk': 'ORDER_HK',
        'ldts': 'LOAD_DATETIME',
        'measures': ['ORDER_AMOUNT', 'ORDER_QUANTITY']
    },
    'sat_order_context': {
        'pk': 'ORDER_HK',
        'ldts': 'LOAD_DATETIME',
        'measures': ['ORDER_DATE_KEY', 'ORDER_NUMBER']
    }
} -%}

{%- set degenerate_dimensions = [] -%}

{{ automate_dv_kimball.fact(src_pk=src_pk,
                    src_fk=src_fk,
                    src_ldts=src_ldts,
                    source_model=source_model,
                    dimensions=dimensions,
                    satellites=satellites,
                    degenerate_dimensions=degenerate_dimensions) }}
