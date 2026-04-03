{{ config(materialized='table') }}

{# Proves pit() -> dim() pattern.
   PIT table resolves which satellite records were active per customer at each as-of date.
   Joining PIT back to satellites gives a fully denormalized point-in-time dimension. #}

WITH pit AS (
    SELECT
        CUSTOMER_HK,
        AS_OF_DATE,
        sat_customer_details_LOAD_DATETIME,
        sat_customer_address_LOAD_DATETIME
    FROM {{ ref('pit_customer') }}
),

hub AS (
    SELECT CUSTOMER_HK, CUSTOMER_ID
    FROM {{ ref('hub_customer') }}
),

details AS (
    SELECT CUSTOMER_HK, LOAD_DATETIME, CUSTOMER_NAME, CUSTOMER_PHONE
    FROM {{ ref('sat_customer_details') }}
),

address AS (
    SELECT CUSTOMER_HK, LOAD_DATETIME, ADDRESS_LINE, CITY, STATE_CODE, ZIP_CODE
    FROM {{ ref('sat_customer_address') }}
)

SELECT
    p.CUSTOMER_HK,
    h.CUSTOMER_ID,
    p.AS_OF_DATE,
    d.CUSTOMER_NAME,
    d.CUSTOMER_PHONE,
    a.ADDRESS_LINE,
    a.CITY,
    a.STATE_CODE,
    a.ZIP_CODE
FROM pit p
INNER JOIN hub h
    ON p.CUSTOMER_HK = h.CUSTOMER_HK
LEFT JOIN details d
    ON p.CUSTOMER_HK = d.CUSTOMER_HK
    AND p.sat_customer_details_LOAD_DATETIME = d.LOAD_DATETIME
LEFT JOIN address a
    ON p.CUSTOMER_HK = a.CUSTOMER_HK
    AND p.sat_customer_address_LOAD_DATETIME = a.LOAD_DATETIME
