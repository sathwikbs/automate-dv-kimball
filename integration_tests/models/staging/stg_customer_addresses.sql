{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: "raw_customer_addresses"
derived_columns:
  SOURCE: "!ADDRESS_SYS"
hashed_columns:
  CUSTOMER_HK: "CUSTOMER_ID"
  CUSTOMER_ADDRESS_HASHDIFF:
    is_hashdiff: true
    columns:
      - "ADDRESS_LINE"
      - "CITY"
      - "STATE_CODE"
      - "ZIP_CODE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
