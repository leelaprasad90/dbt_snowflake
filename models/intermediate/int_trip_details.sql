{{ config(materialized='view') }}
SELECT
    t.*,
    {{ calculate_trip_duration('pickup_datetime', 'dropoff_datetime') }} AS trip_duration_minutes,
    pz.borough AS pickup_borough,
    pz.zone_name AS pickup_zone,
    dz.borough AS dropoff_borough,
    dz.zone_name AS dropoff_zone,
    ROW_NUMBER() OVER (PARTITION BY t.pickup_datetime ORDER BY t.total_amount) AS trip_rank,
    LAG(trip_distance) OVER (PARTITION BY pickup_location_id ORDER BY pickup_datetime) AS prev_trip_distance
FROM {{ ref('stg_trips') }} t
LEFT JOIN {{ ref('stg_zones') }} pz ON t.pickup_location_id = pz.location_id
LEFT JOIN {{ ref('stg_zones') }} dz ON t.dropoff_location_id = dz.location_id