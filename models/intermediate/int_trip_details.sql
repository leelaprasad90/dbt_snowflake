{{ config(materialized='view') }}
SELECT
    t.vendor_id,
    t.pickup_datetime,
    t.dropoff_datetime,
    t.passenger_count,
    t.trip_distance,
    t.pickup_location_id,
    t.dropoff_location_id,
    t.payment_type,
    t.fare_amount,
    t.tip_amount,
    t.total_amount,
    t.congestion_surcharge,
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