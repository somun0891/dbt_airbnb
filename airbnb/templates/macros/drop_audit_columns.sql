{% macro drop_audit_columns(model) %}

    {% if 'dev' in target.name %}
        
        {{ log('Dropping audit columns for '~ model.identifier, info=True) }}
        {% set audit_columns_list = audit_columns().split(',') %}
        
        {% set query %}
            {% for column in audit_columns_list %}
              ALTER TABLE {{ model }}  DROP COLUMN IF EXISTS {{ column }};
            {% endfor %}
        {% endset %}

        {% if var('debug',true) %}
            {{  log(query , info=True)}}       
        {% else %}
            {% do run_query(query) %} 
        {% endif %}
       
    {% endif %}

{% endmacro %}