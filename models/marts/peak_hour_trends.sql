{{ config(
    materialized='incremental',
    unique_key='trip_hour || pickup_borough',
    incremental_strategy='merge',
    partition_by={'field': 'trip_hour', 'data_type': 'timestamp', 'granularity': 'hour'}
) }}
SELECT
    DATE_TRUNC('hour', pickup_datetime) AS trip_hour,
    pickup_borough,
    COUNT(*) AS hourly_trips,
    AVG(trip_duration_minutes) AS avg_duration,
    SUM(total_amount) AS hourly_revenue,
    MAX(w.precipitation) AS max_precipitation
FROM {{ ref('int_trip_details') }} t
LEFT JOIN {{ ref('stg_weather') }} w ON DATE(t.pickup_datetime) = w.date
{% if is_incremental() %}
WHERE {{ dynamic_partition_filter('pickup_datetime', 'trip_hour') }}
{% endif %}
GROUP BY 1, 2