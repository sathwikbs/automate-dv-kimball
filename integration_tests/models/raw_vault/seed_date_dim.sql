{{ config(materialized='table') }}

SELECT * FROM (VALUES
    ('20240115', CAST('2024-01-15' AS DATE), 'Monday', 'January', 2024),
    ('20240116', CAST('2024-01-16' AS DATE), 'Tuesday', 'January', 2024),
    ('20240117', CAST('2024-01-17' AS DATE), 'Wednesday', 'January', 2024),
    ('20240118', CAST('2024-01-18' AS DATE), 'Thursday', 'January', 2024),
    ('20240119', CAST('2024-01-19' AS DATE), 'Friday', 'January', 2024),
    ('20240120', CAST('2024-01-20' AS DATE), 'Saturday', 'January', 2024),
    ('20240125', CAST('2024-01-25' AS DATE), 'Thursday', 'January', 2024),
    ('20240202', CAST('2024-02-02' AS DATE), 'Friday', 'February', 2024),
    ('20240208', CAST('2024-02-08' AS DATE), 'Thursday', 'February', 2024),
    ('20240210', CAST('2024-02-10' AS DATE), 'Saturday', 'February', 2024),
    ('20240301', CAST('2024-03-01' AS DATE), 'Friday', 'March', 2024)
) AS t(DATE_KEY, FULL_DATE, DAY_NAME, MONTH_NAME, YEAR)
