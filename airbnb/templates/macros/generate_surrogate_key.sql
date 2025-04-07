
{% macro generate_surrogate_key(t1_cols) %}

md5(
{% for col in t1_cols -%}
		COALESCE(CAST( {{col}} as varchar) , '') 	
			{%- if not loop.last -%}
			|| '|' ||
			{%- endif -%}
{%- endfor %}
)

{% endmacro %}
