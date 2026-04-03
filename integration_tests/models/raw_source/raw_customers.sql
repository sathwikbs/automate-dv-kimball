{{ config(materialized='table') }}

SELECT * FROM (VALUES
    ('C001', 'Alice Smith',       '555-0101', CAST('2024-01-01 00:00:00' AS TIMESTAMP)),
    ('C002', 'Bob Brown',         '555-0201', CAST('2024-01-01 00:00:00' AS TIMESTAMP)),
    ('C003', 'Charlie Davis',     '555-0301', CAST('2024-01-02 00:00:00' AS TIMESTAMP)),
    ('C004', 'Diana Evans',       '555-0401', CAST('2024-01-02 00:00:00' AS TIMESTAMP)),
    ('C005', 'Eve Foster',        '555-0501', CAST('2024-01-03 00:00:00' AS TIMESTAMP)),
    ('C001', 'Alice Smith-Jones', '555-0102', CAST('2024-01-15 00:00:00' AS TIMESTAMP)),
    ('C003', 'Charlie D.',        '555-0302', CAST('2024-01-20 00:00:00' AS TIMESTAMP))
) AS t(CUSTOMER_ID, CUSTOMER_NAME, CUSTOMER_PHONE, LOAD_DATETIME)
