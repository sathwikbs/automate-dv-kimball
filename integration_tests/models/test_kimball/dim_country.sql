{{ config(materialized='table') }}

{# Proves ref_table -> dim() pattern.
   Reference tables in DV2 are natural static/conformed dimensions in Kimball.
   No SCD logic needed — reference data is treated as slowly changing at most. #}

SELECT
    COUNTRY_HK,
    COUNTRY_CODE,
    COUNTRY_NAME,
    LOAD_DATETIME
FROM {{ ref('ref_country') }}
