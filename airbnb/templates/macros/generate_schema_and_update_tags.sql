{% macro generate_schema_name(custom_schema_name , node) %}

{% if custom_schema_name is none %}
    {{ target.schema }}
{% else %}
    {{ custom_schema_name|trim }}
    {# {{ target.schema  || '_' || custom_schema_name|trim  }} #}
{% endif %}

{% endmacro %}


{% macro set_query_tag() -%}
{%- set new_query_tag = "dbt_" ~ model.name  -%}
{%- if new_query_tag -%}
{%-  set orig_query_tag = get_current_query_tag() -%}  {# Always use model name #}
{{ log("Setting query_tag to '" ~ new_query_tag ~ "'. Will reset to query tag '" ~orig_query_tag~ "' after materialization. ") }}
{%-  do run_query("alter session set query_tag = {}".format(new_query_tag)) -%}
  {{ return(orig_query_tag) }}
{%- endif -%}
{{ return(none) }}
{%- endmacro -%}
  
  
  
  
 {% macro generate_database_name (custom_database_name=none , node=none)   -%}
 {%-	set default_database = target.database  -%}
	
 {%-	if custom_database_name is none -%}
		{{default_database}} 
 {%-	else  -%}
		{{ custom_database_name | trim }} 
 {%-   endif  -%}
 {%- endmacro -%}