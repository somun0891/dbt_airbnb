{% macro dim_type_1_cols( t1_cols , dim_name , src_alias , snap_alias , inc_alias )%}
{%- for col in t1_cols  %}
    {% if is_incremental() -%}
             case when {{snap_alias}}.dbt_valid_to IS NULL and {{snap_alias}}.t1_key <> {{src_alias}}.t1_key then {{src_alias}}.{{col}}  
                   when {{inc_alias}}.{{dim_name}}_key is not null then {{inc_alias}}.{{col}} 
                   else  {{snap_alias}}.{{col}} END AS {{col}}   
                   {%- if not loop.last -%},{%- endif %}        
        {%- else -%}
             case when {{snap_alias}}.dbt_valid_to IS NULL and {{snap_alias}}.t1_key <> {{src_alias}}.t1_key then {{src_alias}}.{{col}} 
                  else {{snap_alias}}.{{col}} END AS {{col}}
                   {%- if not loop.last -%},{%- endif %}                      
        {%- endif -%}
{%- endfor -%}
{% endmacro %}