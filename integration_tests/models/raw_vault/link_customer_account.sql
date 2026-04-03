{{ config(materialized='table') }}

{%- set src_pk = 'CUST_ACCT_HK' -%}
{%- set src_fk = ['CUSTOMER_HK', 'ACCOUNT_HK'] -%}
{%- set src_ldts = 'LOAD_DATETIME' -%}
{%- set src_source = 'SOURCE' -%}
{%- set source_model = 'stg_account_effectivity' -%}

{{ automate_dv.link(src_pk=src_pk,
                    src_fk=src_fk,
                    src_ldts=src_ldts,
                    src_source=src_source,
                    source_model=source_model) }}
