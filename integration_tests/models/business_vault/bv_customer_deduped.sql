{{ config(materialized='table') }}

{# Business Vault same_as_link pattern: deduplicates hub_customer to simulate
   multi-source hub resolution. This proves dim() works with a BV-resolved
   source_model, not just a raw hub. #}

SELECT
    h.CUSTOMER_HK,
    h.CUSTOMER_ID,
    h.LOAD_DATETIME,
    h.SOURCE
FROM {{ ref('hub_customer') }} h
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY h.CUSTOMER_ID
    ORDER BY h.LOAD_DATETIME ASC
) = 1
