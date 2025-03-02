{{ config(materialized='table', description='Trip metrics aggregated by pickup zone') }}
SELECT
    z.borough,
    z.zone_name,
    COUNT(DISTINCT t.trip_rank) AS num_trips,
    SUM(t.trip_distance) AS total_distance,
    AVG(t.fare_amount) AS avg_fare,
    SUM(t.tip_amount) / NULLIF(SUM(t.total_amount), 0) AS tip_percentage,
    MAX(w.temperature) AS temperature,
    MAX(w.precipitation) AS precipitation,
    MAX(w.wind_speed) AS wind_speed,
    MAX(w.precipitation_level) AS precipitation_level
FROM {{ ref('int_trip_details') }} t
JOIN {{ ref('stg_zones') }} z ON t.pickup_location_id = z.location_id
LEFT JOIN {{ ref('stg_weather') }} w ON DATE(t.pickup_datetime) = w.date
GROUP BY 1, 2