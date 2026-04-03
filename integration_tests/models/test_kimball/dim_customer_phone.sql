{{ config(materialized='table') }}

{# Proves ma_sat -> dim() pattern.
   Multi-active satellite has multiple phone records per customer (one per PHONE_TYPE).
   This dim pivots to the latest phone number per type, flattening into one row per customer. #}

WITH latest_phones AS (
    SELECT
        CUSTOMER_HK,
        PHONE_TYPE,
        PHONE_NUMBER,
        LOAD_DATETIME
    FROM {{ ref('ma_sat_customer_phones') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY CUSTOMER_HK, PHONE_TYPE
        ORDER BY LOAD_DATETIME DESC
    ) = 1
),

pivoted AS (
    SELECT
        CUSTOMER_HK,
        MAX(CASE WHEN PHONE_TYPE = 'HOME' THEN PHONE_NUMBER END) AS HOME_PHONE,
        MAX(CASE WHEN PHONE_TYPE = 'MOBILE' THEN PHONE_NUMBER END) AS MOBILE_PHONE,
        MAX(CASE WHEN PHONE_TYPE = 'WORK' THEN PHONE_NUMBER END) AS WORK_PHONE,
        MAX(LOAD_DATETIME) AS LOAD_DATETIME
    FROM latest_phones
    GROUP BY CUSTOMER_HK
),

hub AS (
    SELECT CUSTOMER_HK, CUSTOMER_ID
    FROM {{ ref('hub_customer') }}
)

SELECT
    h.CUSTOMER_HK,
    h.CUSTOMER_ID,
    p.HOME_PHONE,
    p.MOBILE_PHONE,
    p.WORK_PHONE,
    p.LOAD_DATETIME
FROM hub h
INNER JOIN pivoted p ON h.CUSTOMER_HK = p.CUSTOMER_HK
