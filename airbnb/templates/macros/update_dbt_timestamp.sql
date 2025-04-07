{% macro update_dbt_timestamp( dim_name , src_alias , snap_alias , inc_alias )%}

        {%- if is_incremental() -%}
               case when  {{snap_alias}}.dbt_valid_to IS NULL and {{snap_alias}}.t1_key <> {{src_alias}}.t1_key then convert_timezone('UTC',CURRENT_TIMESTAMP)
                when {{inc_alias}}.{{dim_name}}_key is not null then inc.DBT_UPDATED_AT
                else {{snap_alias}}.DBT_UPDATED_AT END AS DBT_UPDATED_AT
        {%- else -%} 
               case when {{snap_alias}}.dbt_valid_to IS NULL and {{snap_alias}}.t1_key <> {{src_alias}}.t1_key then convert_timezone('UTC',CURRENT_TIMESTAMP)
                else {{snap_alias}}.DBT_UPDATED_AT END AS DBT_UPDATED_AT
        {%- endif -%}

{% endmacro %}
