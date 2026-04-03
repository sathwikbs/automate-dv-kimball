{{ config(materialized='table') }}

{# Proves xts() -> audit dim pattern.
   XTS tracks which satellites have data for each customer at each load timestamp.
   This becomes an audit dimension showing satellite coverage per entity over time. #}

WITH xts AS (
    SELECT
        CUSTOMER_HK,
        HASHDIFF,
        SATELLITE_NAME,
        LOAD_DATETIME,
        SOURCE
    FROM {{ ref('xts_customer') }}
    WHERE SATELLITE_NAME IS NOT NULL
),

hub AS (
    SELECT CUSTOMER_HK, CUSTOMER_ID
    FROM {{ ref('hub_customer') }}
)

SELECT
    x.CUSTOMER_HK,
    h.CUSTOMER_ID,
    x.SATELLITE_NAME,
    x.LOAD_DATETIME,
    x.SOURCE
FROM xts x
INNER JOIN hub h ON x.CUSTOMER_HK = h.CUSTOMER_HK
