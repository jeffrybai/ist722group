{% macro pad_publisher_key(pub_id) %}
    {% if target.type == 'bigquery' %}
        format('%04d', {{ pub_id }})
    {% elif target.type == 'postgres' or target.type == 'snowflake' %}
        lpad(cast({{ pub_id }} as text), 4, '0')
    {% elif target.type == 'sqlserver' %}
        right('0000' + cast({{ pub_id }} as varchar), 4)
    {% else %}
        {{ exceptions.raise_compiler_error("Unsupported database for pad_publisher_key macro.") }}
    {% endif %}
{% endmacro %}
