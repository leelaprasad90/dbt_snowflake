{% macro calculate_trip_duration(start_time, end_time) %}
    DATEDIFF('minute', {{ start_time }}, {{ end_time }})
{% endmacro %}   