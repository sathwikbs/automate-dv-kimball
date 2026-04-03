{{ config(materialized='table') }}

{# Proves nh_link -> fact() pattern.
   Non-historized link carries payment events with payload (amount, method).
   This is a natural transaction fact — nh_links are insert-only, no SCD history. #}

-- depends_on: {{ ref('dim_customer') }}

WITH nh AS (
    SELECT
        PAYMENT_HK,
        ORDER_HK,
        CUSTOMER_HK,
        PAYMENT_AMOUNT,
        PAYMENT_METHOD,
        LOAD_DATETIME
    FROM {{ ref('nh_link_payment') }}
)

SELECT
    nh.PAYMENT_HK,
    nh.ORDER_HK,
    nh.CUSTOMER_HK,
    nh.PAYMENT_AMOUNT,
    nh.PAYMENT_METHOD,
    nh.LOAD_DATETIME
FROM nh
WHERE nh.PAYMENT_HK IS NOT NULL
