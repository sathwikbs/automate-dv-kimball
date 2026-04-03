{{ config(materialized='table') }}

SELECT FULL_DATE AS date_day
FROM {{ ref('seed_date_dim') }}
