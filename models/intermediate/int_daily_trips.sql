{{ config(materialized='view') }}
SELECT
    DATE_TRUNC('day', pickup_datetime) AS trip_date,
    pickup_location_id,
    dropoff_location_id,
    COUNT(*) AS num_trips,
    SUM(trip_distance) AS total_distance,
    AVG(trip_duration_minutes) AS avg_duration,
    SUM(congestion_surcharge) AS total_congestion_charge
FROM {{ ref('int_trip_details') }}
GROUP BY 1, 2, 3