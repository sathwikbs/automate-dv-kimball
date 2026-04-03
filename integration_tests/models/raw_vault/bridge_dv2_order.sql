{{ config(materialized='table') }}

{%- set src_pk = 'CUSTOMER_HK' -%}
{%- set as_of_dates_table = 'as_of_dates' -%}
{%- set src_ldts = 'LOAD_DATETIME' -%}
{%- set source_model = 'hub_customer' -%}

{%- set bridge_walk = {
    'CUSTOMER_ACCOUNT': {
        'bridge_link_pk': 'CUST_ACCT_HK',
        'bridge_end_date': 'EFF_SAT_END_DATE',
        'bridge_load_date': 'EFF_SAT_LOAD_DATETIME',
        'link_table': 'link_customer_account',
        'link_pk': 'CUST_ACCT_HK',
        'link_fk1': 'CUSTOMER_HK',
        'link_fk2': 'ACCOUNT_HK',
        'eff_sat_table': 'eff_sat_customer_account',
        'eff_sat_pk': 'CUST_ACCT_HK',
        'eff_sat_end_date': 'END_DATE',
        'eff_sat_load_date': 'LOAD_DATETIME'
    }
} -%}

{%- set stage_tables_ldts = {
    'stg_account_effectivity': 'LOAD_DATETIME'
} -%}

{{ automate_dv.bridge(src_pk=src_pk,
                      src_extra_columns=none,
                      as_of_dates_table=as_of_dates_table,
                      bridge_walk=bridge_walk,
                      stage_tables_ldts=stage_tables_ldts,
                      src_ldts=src_ldts,
                      source_model=source_model) }}
