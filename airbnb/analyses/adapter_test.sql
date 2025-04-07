
{%-set relation = adapter.get_relation(
        database = 'TESTDB',
        schema = 'PUBLIC',
        identifier = 'cars'
      ) -%}

{% set relation_exists  = relation is not none %}

{{ relation_exists }}

{# new/backup relation has to exist #}
{%-set new_relation = adapter.get_relation(
        database = 'TESTDB',
        schema = 'PUBLIC',
        identifier = 'cars_bkp'  
      )
-%}

{{ relation }}
{{new_relation}}

{% if relation_exists and new_relation is not none %}

  {% do adapter.rename_relation(relation,new_relation) %}

{% endif %} 

{# if relation DOESNOT exists then no-op , destructive , does a cascade drop #}
{# {% do adapter.drop_relation(relation) %} #}

{# if schema exists then no-op #}
{% do adapter.create_schema(api.Relation.create(database=target.database, schema='demo')) %} 


select brand as {{ adapter.quote('brand') }} from cars


{%    set columns  = adapter.get_columns_in_relation(relation) %}

{% set colslist = [] %}
{% set exclude = ['make','ID'] %}

{% set exclude =  exclude | map('upper') | list %}

{{exclude}}

{% for col in columns %}
   {% if col.column | upper not in exclude %}
      {{ colslist.append(col.column) }}
   {% endif %}
{% endfor %}

  insert into cars ({{ colslist | join(',') }})
  SELECT 
    {{ colslist | join('\n ,') }}
  from 
   source_ref
    
{# exclude columns from main column list #}
{% set get_quoted_columns  = columns | map(attribute="quoted") | list %}
{% set dest_columns  = columns | map("lower") | list %} {# doesn't work , it has to from a adapter.get_columns_in_relation #}
{% set exclude_cols = ['make'] %}

{{ get_quoted_columns }}
The dest columns are: {{ dest_columns }}
{{exclude_cols | map("lower") | list}}

{%- set update_columns = [] -%}

    {# {%- for col in dest_columns -%} #} {# doesn't work #}
     {%- for col in columns -%}
      {% if col.column | lower not in exclude_cols | map('lower') | list %} 
        {%- do update_columns.append(col.column) -%} 
       {% endif %}
    {% endfor %}

{{ update_columns }} 


{% macro get_merge_update_columns(merge_update_columns, merge_exclude_columns, dest_columns) %}
  {{ return(adapter.dispatch('get_merge_update_columns', 'dbt')(merge_update_columns, merge_exclude_columns, dest_columns)) }}
{% endmacro %}

{% macro default__get_merge_update_columns(merge_update_columns, merge_exclude_columns, dest_columns) %}
  {%- set default_cols = dest_columns | map(attribute="quoted") | list -%}

  {%- if merge_update_columns and merge_exclude_columns -%}
    {{ exceptions.raise_compiler_error(
        'Model cannot specify merge_update_columns and merge_exclude_columns. Please update model to use only one config'
    )}}
  {%- elif merge_update_columns -%}
    {%- set update_columns = merge_update_columns -%}
  {%- elif merge_exclude_columns -%}
    {%- set update_columns = [] -%}
    {%- for column in dest_columns -%}
      {% if column.column | lower not in merge_exclude_columns | map("lower") | list %}
        {%- do update_columns.append(column.quoted) -%}
      {% endif %}
    {%- endfor -%}
  {%- else -%}
    {%- set update_columns = default_cols -%}
  {%- endif -%}

  {{ return(update_columns) }}

{% endmacro %}