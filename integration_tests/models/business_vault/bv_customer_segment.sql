{{ config(materialized='table') }}

{# Business Vault computed satellite: derives CUSTOMER_SEGMENT and LIFETIME_VALUE
   from raw vault assets. This proves dim() works with BV-derived satellites,
   not just raw sats. #}

WITH latest_customer AS (
    SELECT *
    FROM {{ ref('sat_customer_details') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY CUSTOMER_HK ORDER BY LOAD_DATETIME DESC
    ) = 1
),

order_agg AS (
    SELECT
        l.CUSTOMER_HK,
        COUNT(*) AS total_orders,
        SUM(sd.ORDER_AMOUNT) AS lifetime_value
    FROM {{ ref('link_order') }} l
    INNER JOIN {{ ref('sat_order_details') }} sd
        ON l.ORDER_HK = sd.ORDER_HK
    GROUP BY l.CUSTOMER_HK
)

SELECT
    s.CUSTOMER_HK,
    s.LOAD_DATETIME,
    CASE
        WHEN o.total_orders >= 3 THEN 'PLATINUM'
        WHEN o.total_orders >= 2 THEN 'GOLD'
        WHEN o.total_orders >= 1 THEN 'SILVER'
        ELSE 'BRONZE'
    END AS CUSTOMER_SEGMENT,
    COALESCE(o.lifetime_value, CAST(0 AS DOUBLE)) AS LIFETIME_VALUE,
    'SYSTEM' AS SOURCE
FROM latest_customer s
LEFT JOIN order_agg o ON s.CUSTOMER_HK = o.CUSTOMER_HK
