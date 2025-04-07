{% macro warehouse_resize(prod_size, stage_size) %}

  {% if target.name == "prod" or target.name == "prod_backfill" %}
  ALTER WAREHOUSE {{ target.warehouse }} SET WAREHOUSE_SIZE = {{ prod_size }};

  {% else %}
  ALTER WAREHOUSE {{ target.warehouse }} SET WAREHOUSE_SIZE = {{ stage_size }};

  {% endif %}

{% endmacro %}