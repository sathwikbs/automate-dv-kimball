{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: "raw_promotions"
derived_columns:
  SOURCE: "!PROMO_SYS"
hashed_columns:
  PROMO_COV_HK:
    - "PRODUCT_ID"
    - "CUSTOMER_ID"
  PRODUCT_HK: "PRODUCT_ID"
  CUSTOMER_HK: "CUSTOMER_ID"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
