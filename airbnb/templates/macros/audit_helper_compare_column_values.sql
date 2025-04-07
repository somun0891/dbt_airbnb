{% macro audit_helper_compare_column_values() %} -- to use this macro, replace the table references and primary key and dbt run-operation audit_helper_compare_column_values

{%- set columns_to_compare=adapter.get_columns_in_relation(ref('sales_deprecated'))  -%}

{%  set old_etl_relation_query  %}
        select * from {{ ref('base_tickit_sales') }}
{% endset %}


{%- set new_etl_relation_query  %}
        select * from AIRBNB.RAW.sales_deprecated
{% endset %}

    {% if execute%}

        {% for column in columns_to_compare %}

             {{ log("The column being compared is:" ~ column.name , info=True) }}

             {% set audit_query = audit_helper.compare_column_values(
                       a_query=old_etl_relation_query,
                       b_query=new_etl_relation_query,
                       primary_key="salesid",
                       column_to_compare=column.name
               ) %}

               {% set audit_results =  run_query(audit_query) %}
                  {% do audit_results.print_table() %}

        {% endfor %}

    {% endif %}

{% endmacro %}

