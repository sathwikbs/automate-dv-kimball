{{ config(materialized='table') }}

{# Tests dim_bridge() with src_eff / src_exp for time-varying bridges.
   The macro supports these parameters but they were never tested. #}

{%- set src_pk = 'CUST_ACCT_HK' -%}
{%- set bridge_fks = ['CUSTOMER_HK', 'ACCOUNT_HK'] -%}
{%- set source_model = 'bridge_cust_acct' -%}
{%- set src_ldts = 'LOAD_DATETIME' -%}

{{ automate_dv_kimball.dim_bridge(src_pk=src_pk,
                           bridge_fks=bridge_fks,
                           source_model=source_model,
                           src_ldts=src_ldts,
                           weighting_factor='OWNERSHIP_PCT',
                           src_eff='EFFECTIVE_FROM',
                           src_exp='EXPIRY_DATE') }}
