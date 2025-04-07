{% macro convert_list_to_str(val, separator=',') %}

{% if val is not string and val is sequence %}
   {% set final_str = val | join('{{separator}}') %}
{% else %}
  {{ exceptions.raise_compiler_error("The variable is expected to be either  list or tuple , got " ~ val~ " instead.") }}
{% endif %}

{{ return(final_str) }}
   
{% endmacro %}