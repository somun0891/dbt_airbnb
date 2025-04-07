{% macro create_dbt_stage(my_file_format='JSON') %}

{% set create_stage %}
  CREATE stage if not exists dbt_load 
     copy_options = (ON_ERROR='SKIP_FILE')
	 file_format='{{my_file_format}}'
{% endset %}

{% do run_query(create_stage) %}

{% endmacro %}

{% macro load_file(stage_name='dbt_load') %}

    {% set put_query %}

        put file:/target/run_results.json @{{target.database}}.{{target.schema}}.{{stage_name}}
        auto_compress = true overwrite = true

    {% endset %}

    {{ log("file_path:" ~ put_query , info=true)}}

    {% if execute %}
      
     {%- do run_query(put_query) -%}

    {% endif %}

{% set get_current_invocation_id %}
    select 
      trim($1:metadata.invocation_id , '"') as invocation_id
    from 
      @{{target.database}}.{{target.schema}}.{{stage_name}}
{% endset %}


    {% if execute %}
     {%- set current_invocation_id =  run_query(get_current_invocation_id).columns[0][0] -%}
    {% endif %}

{{ log("current invocation id from run results: " ~  current_invocation_id , true)}}
{{ return(current_invocation_id) }}

{% endmacro %}

{% macro copy_file(stage_name='dbt_load')%}

    {% set result_size %}
        select array_size($1:results) as res_length
     from 
        @{{target.database}}.{{target.schema}}.{{stage_name}}
    {% endset %}


    {% if execute %}
     {%- set rsize =  run_query(result_size).columns[0][0] -%}
     {{ log('results_size:' ~ rsize , info=true) }}
  
     {% if rsize > 0 %}
     {% if rsize > 0 %}
     {% if rsize > 0 %}
     {% if rsize > 0 %}
     {% if rsize > 0
    
    BEGIN;
     {% set copy_command %}
       
        copy into dbt_run_results(invocation_id , copied_result_time , execution_detail)
            FROM 
            ( --COPY TRANSFORM
                SELECT 
                trim($1:metadata:invocation_id  , '"')::varchar(256) as invocation_id
                CURRENT_TIMESTAMP::timestamp_ltz(6) as copied_result_time
                parse_json($1)::text as execution_detail                
                FROM
                  @{{target.database}}.{{target.schema}}.{{stage_name}}
            )
            file_format(type='JSON')
            on_error='skip_file'
        {% endset %}

        {{ log(copy_command , info=true)}}

        {% do run_query(copy_command) %}

        {% set check_file_loaded %}
           SELECT COUNT(1) FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
           WHERE status='LOADED'
        {% endset %}

        {% set row_count = run_query(check_file_loaded).columns[0][0] %}

        {{ log('copied file into dbt_results, row_count: ' ~ row_count , info=true)}}

    COMMIT;

    {% endif %}

  {% endif %}
{% endmacro %}


{% macro remove_stg_files(stage_name='dbt_load') %}

  {% set remove_stg %}
    remove @{{target.database}}.{{target.schema}}.{{stage_name}}
  {% endset %}

  {%log("cleaning up stage - "~remove_stg , info=true)%}

  {% do run_query(remove_stg) %}
 
{% endmacro %}