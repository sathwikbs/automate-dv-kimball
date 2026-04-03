{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: "raw_products"
derived_columns:
  SOURCE: "!PRODUCT_SYS"
hashed_columns:
  PRODUCT_HK: "PRODUCT_ID"
  PRODUCT_HASHDIFF:
    is_hashdiff: true
    columns:
      - "PRODUCT_NAME"
      - "UNIT_PRICE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
