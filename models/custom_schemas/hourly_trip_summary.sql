{{ config(materialized='table', schema=generate_custom_schema()) }}
SELECT
    DATE_TRUNC('hour', pickup_datetime) AS trip_hour,
    pickup_borough,
    COUNT(*) AS hourly_trips,
    SUM(total_amount) AS hourly_revenue
FROM {{ ref('int_trip_details') }}
GROUP BY 1, 2