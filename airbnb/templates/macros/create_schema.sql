{% macro create_schema_custom(schema_name) %}

{% set schema_exists_sql %}

select COUNT(1) from information_schema.schemata where SCHEMA_NAME = '{{schema_name}}'
AND CATALOG_NAME = '{{target.database | upper}}';  {# information_Schema is case senstive #}

{% endset %}

 {% set words = 'DBT is Cool'.split(' ')    %}  

{# create schema SQL #}
 {% set query    %}
    CREATE SCHEMA IF NOT EXISTS
 {% if schema_name is none or schema_name|length < 1 %}    {# Don't wrap schema_name in expressions tag as already covered by {%%}%} #}
       testing                                                           {# Don't wrap in expressions tag #}
 {%  else     %}
       {{ schema_name }}              {# wrap in expressions tag as it is dynamic and not already part of statement tag #}
 {%   endif   %}
 {% endset %}


{{ log(" Check for " ~ schema_name  ~ " schema existence." ,info=True) }}
{% set results = run_query(schema_exists_sql) %}



{# fetch values from the results object and stored in a cnt list #}
{% if execute   %}
{% set cnt = results.columns[0].values() %}
            {# {{ log(cnt , info=True) }}  #}
{% else  %}
{% set cnt = 0 %}
{% endif %}



{%  if cnt[0] == 1 and var('str')|int == 20 %}   
  {{ log(schema_name ~ " schema already exists..." ,info=True) }}    
  {{ log("test! "* 3 , info=True)}}
  {# {{print('hi')}}   #}

{% else %}
      {# {{log(target.database ,info=True)}} #}
      {# {{log(schema_name ,info=True)}}  #}
{# run schema creation logic #}
{% do run_query(query) %}
  {{ log(schema_name ~ "  Schema created successfully..." ,info=True) }}


{% endif %}

{% endmacro %}

