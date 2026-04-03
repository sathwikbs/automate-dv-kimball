{{ config(materialized='table') }}

{%- set src_pk = 'CUSTOMER_HK' -%}
{%- set src_ldts = 'LOAD_DATETIME' -%}
{%- set src_source = 'SOURCE' -%}

{%- set src_satellite = {
    'SAT_CUSTOMER_DETAILS': {
        'sat_name': {
            'SATELLITE_NAME': 'SATELLITE_NAME_1'
        },
        'hashdiff': {
            'HASHDIFF': 'CUSTOMER_HASHDIFF'
        }
    },
    'SAT_CUSTOMER_ADDRESS': {
        'sat_name': {
            'SATELLITE_NAME': 'SATELLITE_NAME_2'
        },
        'hashdiff': {
            'HASHDIFF': 'CUSTOMER_ADDRESS_HASHDIFF'
        }
    }
} -%}

{%- set source_model = ['stg_customers_xts', 'stg_customer_addresses_xts'] -%}

{{ automate_dv.xts(src_pk=src_pk,
                   src_satellite=src_satellite,
                   src_extra_columns=none,
                   src_ldts=src_ldts,
                   src_source=src_source,
                   source_model=source_model) }}
