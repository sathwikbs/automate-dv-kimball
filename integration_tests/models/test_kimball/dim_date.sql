{{ config(materialized='table') }}

SELECT
    DATE_KEY,
    FULL_DATE,
    DAY_NAME,
    MONTH_NAME,
    YEAR
FROM {{ ref('seed_date_dim') }}
