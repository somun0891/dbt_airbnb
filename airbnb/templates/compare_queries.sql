 
 {%- set old_relation = adapter.get_relation(
      database= target.database,
      schema= target.schema,
      identifier="sales_deprecated") -%} 

 {% set dbt_relation = ref('base_tickit_sales') %}

{% set audit_query =
 audit_helper.compare_relations(
    a_relation=dbt_relation,
    b_relation=old_relation,
    primary_key="salesid"
)
%}
{% if execute %}
    {% set run_results = run_query(audit_query) %}
    {% do run_results.print_table() %}
{% endif %}

