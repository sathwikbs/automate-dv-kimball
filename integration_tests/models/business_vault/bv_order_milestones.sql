{{ config(materialized='table') }}

{# Business Vault pre-resolved milestones: pivots order/ship dates from separate
   satellites into a single wide row per order. This proves fact_accumulating_snapshot()
   works with BV-derived milestone sources, not just raw sats. #}

WITH latest_placed AS (
    SELECT *
    FROM {{ ref('sat_order_placed') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ORDER_HK ORDER BY LOAD_DATETIME DESC
    ) = 1
),

latest_shipped AS (
    SELECT *
    FROM {{ ref('sat_order_shipped') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ORDER_HK ORDER BY LOAD_DATETIME DESC
    ) = 1
)

SELECT
    op.ORDER_HK,
    op.LOAD_DATETIME AS ORDER_PLACED_DATE,
    os.LOAD_DATETIME AS ORDER_SHIPPED_DATE,
    'SYSTEM' AS SOURCE
FROM latest_placed op
LEFT JOIN latest_shipped os
    ON op.ORDER_HK = os.ORDER_HK
