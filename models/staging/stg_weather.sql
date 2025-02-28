{{ config(materialized='view') }}
SELECT
    date,
    temperature,
    precipitation,
    wind_speed,
    CASE
        WHEN precipitation > 0.2 THEN 'High'
        WHEN precipitation > 0 THEN 'Low'
        ELSE 'None'
    END AS precipitation_level
FROM {{ source('taxi_raw', 'weather') }}