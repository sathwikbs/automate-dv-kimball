{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: "raw_orders"
derived_columns:
  SOURCE: "!ORDERS_SYS"
  ORDER_DATE_KEY: "ORDER_DATE"
hashed_columns:
  ORDER_HK: "ORDER_ID"
  CUSTOMER_HK: "CUSTOMER_ID"
  PRODUCT_HK: "PRODUCT_ID"
  ORDER_HASHDIFF:
    is_hashdiff: true
    columns:
      - "ORDER_AMOUNT"
      - "ORDER_QUANTITY"
  ORDER_AMOUNT_HASHDIFF:
    is_hashdiff: true
    columns:
      - "ORDER_AMOUNT"
  ORDER_CONTEXT_HASHDIFF:
    is_hashdiff: true
    columns:
      - "ORDER_DATE"
      - "ORDER_NUMBER"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
