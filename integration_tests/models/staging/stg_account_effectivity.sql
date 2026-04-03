{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: "raw_account_effectivity"
derived_columns:
  SOURCE: "!ACCOUNT_SYS"
hashed_columns:
  CUST_ACCT_HK:
    - "CUSTOMER_ID"
    - "ACCOUNT_ID"
  CUSTOMER_HK: "CUSTOMER_ID"
  ACCOUNT_HK: "ACCOUNT_ID"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
