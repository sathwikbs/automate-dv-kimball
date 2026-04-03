{{ config(materialized='table') }}

{# Proves eff_sat -> dim() SCD2 pattern.
   Effectivity satellite tracks when customer-account relationships are active.
   The dim surfaces this as a slowly changing dimension with validity windows. #}

WITH eff AS (
    SELECT
        CUST_ACCT_HK,
        CUSTOMER_HK,
        ACCOUNT_HK,
        START_DATE,
        END_DATE,
        LOAD_DATETIME
    FROM {{ ref('eff_sat_customer_account') }}
),

hub AS (
    SELECT CUSTOMER_HK, CUSTOMER_ID
    FROM {{ ref('hub_customer') }}
)

SELECT
    e.CUST_ACCT_HK,
    h.CUSTOMER_ID,
    e.CUSTOMER_HK,
    e.ACCOUNT_HK,
    e.START_DATE AS EFFECTIVE_FROM,
    e.END_DATE AS EFFECTIVE_TO,
    CASE
        WHEN e.END_DATE = CAST('9999-12-31 23:59:59' AS TIMESTAMP) THEN true
        ELSE false
    END AS IS_CURRENT,
    e.LOAD_DATETIME
FROM eff e
INNER JOIN hub h ON e.CUSTOMER_HK = h.CUSTOMER_HK
