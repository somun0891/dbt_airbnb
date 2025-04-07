{% macro simple_cte(relation_list) %}
with 
{% for cte_ref in relation_list -%}
{{- cte_ref[0] }} as (
    select 
        *
    from 
        {{cte_ref[1]}}
){%- if not loop.last -%},{%- endif -%}

{% endfor %}
{% endmacro %}