{% snapshot zone_changes_snapshot %}
{{
    config(
        target_schema='ANALYTICS',
        unique_key='location_id',
        strategy='check',
        check_cols=['borough', 'zone_name']
    )
}}
SELECT
    location_id,
    borough,
    zone_name,
    CURRENT_TIMESTAMP AS updated_at
FROM {{ ref('stg_zones') }}
{% endsnapshot %}