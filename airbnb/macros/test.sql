{% macro get_column_query(table) %}

{% set database = 'AIRBNB' %}
{% set schema = 'RAW' %}
{% set table = table %}



{% set columns_list = [] %}

{%- set column_data_type_query -%}
SELECT
  t.table_catalog,
  t.table_schema,
  t.table_name,
  CASE t.table_type
    WHEN 'BASE TABLE' THEN 'TABLE'
    ELSE t.table_type
  END AS table_type,
  c.column_name,
  c.data_type
FROM "{{ database }}".information_schema.tables t
INNER JOIN "{{ database }}".information_schema.columns c
  ON c.table_schema = t.table_schema
  AND c.table_name = t.table_name
WHERE t.table_catalog =  '{{ database.upper() }}' 
  AND t.table_type IN ('BASE TABLE', 'VIEW')
  AND t.table_schema = '{{ schema.upper() }}' 
  AND t.table_name = '{{ table.upper() }}'
;
{%- endset -%}

{% if execute  %}
{% set results = run_query(column_data_type_query)   %}

{% for row in results.rows  %}

    {% if row['TABLE_NAME'] == table | upper %}
     {{ columns_list.append(row['COLUMN_NAME']) }}   
    {% endif %} 

{% endfor %}

{{ return(columns_list) }}
{% endif %}


{% endmacro %}






{% macro teamlist() %}
  
{% set teams = [] %}
{% set team = {} %}
{% set group = {} %}

   {% set query %}
     SELECT DISTINCT Team, State as City FROM Teams;
   {% endset %}

 {%  if execute  %}
   {% set results = run_query(query) %}

    {% for row in results.rows %}

      {% set team = ({ row['TEAM']:row['CITY'] })   %}
      {% do teams.append(team)  %}    
      {% do group.update(team) %}

       {# {% do log(team , info=True)     %} #}
  
    {% endfor %}

 {{ return(group) }}
 {%   endif %}
  
{% endmacro %}

{% macro log_results(results) %}

{% if execute %}

  {{ log("==================Begin Summary=================" ,info=True) }}
    {% for res in results %}
    {%set line %}
     nodeid:  {{res.node.unique_id  }}  , status: {{res.status}}  , (message: {{res.message}})
    {% endset %}

{{ log(line ,True) }} 

{% endfor %}
{{ log("==================End Summary=================" ,info=True)   }}
  
{% endif %}
  
{% endmacro %}


{% macro generate_case_stmt(mktunit) %}

{% for val in mktunit %}
  CASE WHEN  '{{val}}' THEN amt END AS "{{val}}_amt"
  {%- if not loop.last -%},{%- endif -%}

{%- endfor %}
{%endmacro %}



{% macro get_col_values(relation , field) %}
  
{% if field is not none and relation is not none and field is string and relation is string  %}
 
    {% set sql_query %}
       select distinct {{field}} from {{relation}}
    {% endset %}
    
    {%  set results = run_query(sql_query)  %}
    
    {%   if execute   %}   
         {% if results is not none %}
             {%  set res = results.columns[0].values() %}
           {% else %} 
             set res = []
         {% endif %}
    {%  endif %}

{% else %}
 {{ exceptions.raise_compiler_error("Invalid Parameters, Please pass correct ones! It must be a valid string. ") }}

{% endif %}
{{ return(res) }}
{% endmacro %}



