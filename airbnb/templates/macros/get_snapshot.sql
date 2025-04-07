{% macro get_snapshot(snapshot_name) %}

{% set non_prod_snapshot_relation = ref(snapshot_name) %}

{% set prod_snapshot_relation = adapter.get_relation(
									database= non_prod_snapshot_relation.database  
									 ,schema= 'snapshots'
									,identifier= non_prod_snapshot_relation.identifier 
								) %}
								
{# schema= 'snapshots' -- hardcoded for prod #}
{% if target.name != 'prod' and prod_snapshot_relation is not none %}

	{% set preferred_snapshot_relation = prod_snapshot_relation if target.name == 'prod'
	 and prod_snapshot_relation is not none else non_prod_snapshot_relation %}

{% endif %}

    {{ return(preferred_snapshot_relation) }}

{% endmacro %}