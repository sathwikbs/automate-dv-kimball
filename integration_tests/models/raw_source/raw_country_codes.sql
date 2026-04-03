{{ config(materialized='table') }}

SELECT * FROM (VALUES
    ('US', 'United States',  CAST('2024-01-01 00:00:00' AS TIMESTAMP), 'REF_SYS'),
    ('GB', 'United Kingdom', CAST('2024-01-01 00:00:00' AS TIMESTAMP), 'REF_SYS'),
    ('DE', 'Germany',        CAST('2024-01-01 00:00:00' AS TIMESTAMP), 'REF_SYS'),
    ('FR', 'France',         CAST('2024-01-01 00:00:00' AS TIMESTAMP), 'REF_SYS'),
    ('JP', 'Japan',          CAST('2024-01-01 00:00:00' AS TIMESTAMP), 'REF_SYS')
) AS t(COUNTRY_CODE, COUNTRY_NAME, LOAD_DATETIME, SOURCE)
