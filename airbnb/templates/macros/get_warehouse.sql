{% macro get_warehouse(size) %}
    {% set available_sizes = ['xsmall', 'small', 'medium', 'large', 'xlarge', '2xlarge'] %}
    {% if size not in available_sizes %}
        {{ exceptions.raise_compiler_error("Warehouse size not one of " ~ valid_warehouse_sizes) }}
    {% endif %}
    {% if target.name in ('production', 'prod') %}
        {% do return('dbt_production_' ~ size) %}
    {% elif target.name in ('ci') %}
        {% do return('dbt_ci_' ~ size) %}
    {% else %}
        {% do return(None) %}
    {% endif %}
{% endmacro %}