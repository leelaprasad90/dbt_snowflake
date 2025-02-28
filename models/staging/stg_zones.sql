{{ config(materialized='view') }}
SELECT
    LocationID AS location_id,
    Borough AS borough,
    Zone AS zone_name,
    service_zone
FROM {{ source('taxi_raw', 'taxi_zones') }}