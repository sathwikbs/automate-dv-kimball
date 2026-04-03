{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: "raw_country_codes"
derived_columns:
  SOURCE: "!REF_SYS"
hashed_columns:
  COUNTRY_HK: "COUNTRY_CODE"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     hashed_columns=metadata_dict['hashed_columns'],
                     ranked_columns=none) }}
