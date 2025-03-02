{{ config(
    materialized='incremental',
    unique_key='trip_id',
    incremental_strategy='merge',
    description='Payment trends with credit card tip analysis'
) }}

{% if is_incremental() %}
    {% set max_date_query %}
        SELECT MAX(pickup_datetime) AS max_date FROM {{ this }}
        WHERE pickup_datetime BETWEEN '1970-01-01' AND '9999-12-31'
    {% endset %}
    {% set max_date_result = run_query(max_date_query) %}
    {% if max_date_result %}
        {% set max_date = max_date_result.columns[0][0] %}
    {% else %}
        {% set max_date = '1970-01-01' %}  -- Fallback for first run or invalid data
    {% endif %}
{% endif %}

WITH trip_ranks AS (
    SELECT
        pickup_datetime || '-' || dropoff_datetime || '-' || pickup_location_id AS trip_id,
        pickup_datetime,
        payment_type,
        total_amount,
        tip_amount
    FROM {{ ref('int_trip_details') }}
    {% if is_incremental() %}
    WHERE pickup_datetime >= '{{ max_date }}'
    {% endif %}
)
SELECT
    t.trip_id,
    p.payment_type_name,
    t.pickup_datetime,
    t.total_amount,
    t.tip_amount,
    CASE WHEN p.payment_type_name = 'Credit card' THEN t.tip_amount ELSE 0 END AS credit_card_tips
FROM trip_ranks t
JOIN {{ ref('payment_types') }} p ON t.payment_type = p.payment_type_id