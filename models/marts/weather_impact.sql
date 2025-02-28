{{ config(materialized='table', description='Impact of weather on trip volume and duration') }}
SELECT
    d.trip_date,
    w.temperature,
    w.precipitation,
    w.precipitation_level,
    SUM(d.num_trips) AS total_trips,
    AVG(d.avg_duration) AS avg_trip_duration,
    SUM(d.total_distance) AS total_distance,
    SUM(d.total_congestion_charge) AS total_congestion
FROM {{ ref('int_daily_trips') }} d
LEFT JOIN {{ ref('stg_weather') }} w ON d.trip_date = w.date
GROUP BY 1, 2, 3, 4
HAVING total_trips > 0