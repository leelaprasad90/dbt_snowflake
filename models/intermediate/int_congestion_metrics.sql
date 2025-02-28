{{ config(materialized='view') }}
SELECT
    trip_date,
    pickup_borough,
    AVG(congestion_surcharge) AS avg_congestion_surcharge,
    COUNT(*) FILTER (WHERE congestion_surcharge > 0) AS congested_trips,
    SUM(trip_duration_minutes) / NULLIF(SUM(trip_distance), 0) AS avg_duration_per_mile
FROM {{ ref('int_trip_details') }}
WHERE trip_distance > 0
GROUP BY 1, 2