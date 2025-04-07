
{{  run_sql_query("select 1")  }}

{{ 'IAS_DEV_WH' if target.name == 'prod' else run_sql_query("select 1") }}