{{ config(materialized='table') }}

SELECT
    CUST_ACCT_HK,
    CUSTOMER_HK,
    ACCOUNT_HK,
    OWNERSHIP_PCT,
    LOAD_DATETIME,
    LOAD_DATETIME AS EFFECTIVE_FROM,
    CAST('9999-12-31 23:59:59' AS TIMESTAMP) AS EXPIRY_DATE
FROM {{ ref('stg_customer_accounts') }}
