{% macro dynamic_partition_filter(datetime_column, partition_column) %}
    {{ datetime_column }} >= DATEADD('hour', -24, (SELECT MAX({{ partition_column }}) FROM {{ this }}))
{% endmacro %}