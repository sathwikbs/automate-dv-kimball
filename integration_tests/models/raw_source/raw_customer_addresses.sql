{{ config(materialized='table') }}

SELECT * FROM (VALUES
    ('C001', '123 Main St',   'Springfield', 'IL', '62701', CAST('2024-01-01 00:00:00' AS TIMESTAMP)),
    ('C002', '456 Oak Ave',   'Shelbyville', 'IL', '62565', CAST('2024-01-01 00:00:00' AS TIMESTAMP)),
    ('C003', '789 Elm Blvd',  'Capital City','IL', '62702', CAST('2024-01-02 00:00:00' AS TIMESTAMP)),
    ('C004', '321 Pine Rd',   'Ogdenville',  'IL', '62550', CAST('2024-01-02 00:00:00' AS TIMESTAMP)),
    ('C005', '654 Maple Dr',  'North Haverbrook', 'IL', '62551', CAST('2024-01-03 00:00:00' AS TIMESTAMP)),
    ('C001', '123 Main St #2','Springfield', 'IL', '62701', CAST('2024-01-15 00:00:00' AS TIMESTAMP))
) AS t(CUSTOMER_ID, ADDRESS_LINE, CITY, STATE_CODE, ZIP_CODE, LOAD_DATETIME)
