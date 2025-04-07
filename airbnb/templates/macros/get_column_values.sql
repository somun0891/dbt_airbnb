 {% macro get_column_values_custom(column , relation)  %}

 {% set sql  %}

select distinct {{ column }} 
from {{ relation }}
order by 1

{% endset %}

results  = run_query(sql)

if execute 
set result_list = results.columns[0].values()
else 
set result_list = []
endif

{{ return(result_list) }}

{% endmacro %}

{#  call the above reusable macro from inside another macro #}
 {% macro get_symbol()  -%}
 {{ return(get_column_values_custom(symbol , ref('base_ESCM__TRADE')))  }}

{% endmacro %}
