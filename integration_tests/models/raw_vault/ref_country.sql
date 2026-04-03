{{ config(materialized='table') }}

{%- set src_pk = 'COUNTRY_HK' -%}
{%- set src_extra_columns = ['COUNTRY_CODE', 'COUNTRY_NAME'] -%}
{%- set src_ldts = 'LOAD_DATETIME' -%}
{%- set src_source = 'SOURCE' -%}
{%- set source_model = 'stg_country_codes' -%}

{{ automate_dv.ref_table(src_pk=src_pk,
                         src_extra_columns=src_extra_columns,
                         src_ldts=src_ldts,
                         src_source=src_source,
                         source_model=source_model) }}
