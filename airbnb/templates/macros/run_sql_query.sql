{% macro run_sql_query(stmt) %}

{% set results = run_query(stmt) %}

{% if execute %}

{# Return the first column , all values
 results.columns[0].values()  
 #}

{# Return first column first value 
   results.columns[0][0]
  #}

{# Return first row , first column value and second column value and so on-
r = results.rows
{% set first_col_val = r[0]["first_column"] %}
{% set second_col_val = r[0]["Second_column"] %}
  #}

{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

{{ return( results_list[0]) }}
 

{% endmacro %}