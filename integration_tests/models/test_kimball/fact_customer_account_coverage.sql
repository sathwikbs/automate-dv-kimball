{{ config(materialized='table') }}

{# Proves link -> fact_factless() pattern for a different link.
   The customer-account link records which customers hold which accounts.
   This is a classic coverage/factless fact — no measures, just the intersection. #}

-- depends_on: {{ ref('dim_customer') }}

{%- set src_pk = 'CUST_ACCT_HK' -%}
{%- set src_fk = ['CUSTOMER_HK', 'ACCOUNT_HK'] -%}
{%- set src_ldts = 'LOAD_DATETIME' -%}
{%- set source_model = 'link_customer_account' -%}

{%- set dimensions = {
    'CUSTOMER_HK': 'dim_customer'
} -%}

{{ automate_dv_kimball.fact_factless(src_pk=src_pk,
                              src_fk=src_fk,
                              src_ldts=src_ldts,
                              source_model=source_model,
                              dimensions=dimensions,
                              include_count=true) }}
