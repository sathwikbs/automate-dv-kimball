{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: "raw_deliveries"
derived_columns:
  SOURCE: "!DELIVERY_SYS"
hashed_columns:
  ORDER_HK: "ORDER_ID"
  DELIVERY_HASHDIFF:
    is_hashdiff: true
    columns:
      - "DELIVERY_DATETIME"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
