{{ config(materialized='table') }}

{%- set src_pk = 'CUSTOMER_HK' -%}
{%- set as_of_dates_table = 'as_of_dates' -%}
{%- set src_ldts = 'LOAD_DATETIME' -%}
{%- set source_model = 'hub_customer' -%}

{%- set satellites = {
    'sat_customer_details': {
        'pk': {'CUSTOMER_HK': 'CUSTOMER_HK'},
        'ldts': {'LOAD_DATETIME': 'LOAD_DATETIME'}
    },
    'sat_customer_address': {
        'pk': {'CUSTOMER_HK': 'CUSTOMER_HK'},
        'ldts': {'LOAD_DATETIME': 'LOAD_DATETIME'}
    }
} -%}

{%- set stage_tables_ldts = {
    'stg_customers': 'LOAD_DATETIME',
    'stg_customer_addresses': 'LOAD_DATETIME'
} -%}

{{ automate_dv.pit(src_pk=src_pk,
                   src_extra_columns=none,
                   as_of_dates_table=as_of_dates_table,
                   satellites=satellites,
                   stage_tables_ldts=stage_tables_ldts,
                   src_ldts=src_ldts,
                   source_model=source_model) }}
