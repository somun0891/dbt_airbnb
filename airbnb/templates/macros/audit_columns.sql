{% macro audit_columns() %}
  
  dbt_valid_from,
  dbt_valid_to,
  dbt_updated_at,
  create_ts,
  update_ts,
  dbt_scd_id,
  update_timestamp

{% endmacro %}