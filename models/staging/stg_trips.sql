{{ config(materialized='view') }}
SELECT
    VendorID AS vendor_id,
    tpep_pickup_datetime AS pickup_datetime,
    tpep_dropoff_datetime AS dropoff_datetime,
    passenger_count,
    trip_distance,
    PULocationID AS pickup_location_id,
    DOLocationID AS dropoff_location_id,
    payment_type,
    fare_amount,
    tip_amount,
    total_amount,
    congestion_surcharge
FROM {{ source('taxi_raw', 'yellow_trips') }}
WHERE trip_distance > 0 AND fare_amount >= 0