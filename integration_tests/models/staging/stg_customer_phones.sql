{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: "raw_customer_phones"
derived_columns:
  SOURCE: "!PHONE_SYS"
hashed_columns:
  CUSTOMER_HK: "CUSTOMER_ID"
  PHONE_HASHDIFF:
    is_hashdiff: true
    columns:
      - "PHONE_NUMBER"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
