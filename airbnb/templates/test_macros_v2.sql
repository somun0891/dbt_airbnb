{% set colstr = "col1 , col2 , col3" %}

{% set collist = (colstr | replace(" ","")).split(",") %}
{% set separator = "-" %}


{% for col in collist -%}
   
   coalesce( {{ col }} , ''  )

  {%- if not loop.last %} || {{ separator }} || {% endif %}

{%- endfor %}

{{ print(dbt.hash(dbt.concat(collist))) }}

{% set cols = collist | join(", ") %}

The returned cols are : {{ cols }}


 {# incremental_predicates  = "DBT_INTERNAL_DEST.session_start > {{ get_timestamp(current_timestamp()) }} " #}

 {{ log( get_timestamp(current_timestamp()) , info=True) }}