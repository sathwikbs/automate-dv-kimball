{{ config(materialized='table') }}

{%- set src_pk = 'CUST_ACCT_HK' -%}
{%- set src_dfk = 'CUSTOMER_HK' -%}
{%- set src_sfk = 'ACCOUNT_HK' -%}
{%- set src_start_date = 'START_DATE' -%}
{%- set src_end_date = 'END_DATE' -%}
{%- set src_eff = 'EFFECTIVE_FROM' -%}
{%- set src_ldts = 'LOAD_DATETIME' -%}
{%- set src_source = 'SOURCE' -%}
{%- set source_model = 'stg_account_effectivity' -%}

{{ automate_dv.eff_sat(src_pk=src_pk,
                       src_dfk=src_dfk,
                       src_sfk=src_sfk,
                       src_start_date=src_start_date,
                       src_end_date=src_end_date,
                       src_eff=src_eff,
                       src_ldts=src_ldts,
                       src_source=src_source,
                       source_model=source_model) }}
