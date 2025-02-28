{% macro generate_custom_schema() %}
    {% if target.name == 'prod' %}
        'HOURLY_ANALYTICS'
    {% else %}
        'DEV_HOURLY_ANALYTICS'
    {% endif %}
{% endmacro %}