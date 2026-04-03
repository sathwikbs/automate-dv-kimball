{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: "raw_inventory"
derived_columns:
  SOURCE: "!INVENTORY_SYS"
hashed_columns:
  PRODUCT_HK: "PRODUCT_ID"
  INVENTORY_HASHDIFF:
    is_hashdiff: true
    columns:
      - "QUANTITY_ON_HAND"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
