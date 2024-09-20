{% macro run_sql_query(stmt) %}

{% set results = run_query(stmt) %}

{% if execute %}

{# Return the first column #}

{% set results_list = results %}
{% else %}
{% set results_list = [] %}
{% endif %}

{{ return(results_list) }}

{% endmacro %}