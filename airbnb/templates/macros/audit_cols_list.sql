{% macro audit_cols_list() %}
  
{% set audit_cols_string %}

  dbt_valid_from, dbt_valid_to, dbt_updated_at, create_ts, update_ts   
  , dbt_scd_id, update_timestamp

{% endset %}   

{% set fields = audit_cols_string | replace("\n", "") | replace(" ", "") %}

{% set audit_cols  = fields.split(",") %}

{{ return(audit_cols) }}

{% endmacro %}