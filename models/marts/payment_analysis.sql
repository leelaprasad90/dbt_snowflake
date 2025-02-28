{{ config(
    materialized='incremental',
    unique_key='trip_id',
    incremental_strategy='merge',
    description='Payment trends with credit card tip analysis'
) }}
WITH trip_ranks AS (
    SELECT
        pickup_datetime || '-' || dropoff_datetime || '-' || pickup_location_id AS trip_id,
        payment_type,
        total_amount,
        tip_amount
    FROM {{ ref('int_trip_details') }}
    {% if is_incremental() %}
    WHERE pickup_datetime >= (SELECT MAX(pickup_datetime) FROM {{ this }})
    {% endif %}
)
SELECT
    t.trip_id,
    p.payment_type_name,
    t.total_amount,
    t.tip_amount,
    CASE WHEN p.payment_type_name = 'Credit card' THEN t.tip_amount ELSE 0 END AS credit_card_tips
FROM trip_ranks t
JOIN {{ ref('payment_types') }} p ON t.payment_type = p.payment_type_id