{{
    config(
        materialized= 'table',
        transient=true
    )
}}

with source as (
      select * from {{ source('tickit', 'sales') }}
),
renamed as (
    select
        *
    from source
)
select * from renamed

{# {{ this.database }} 
{{ this.schema  }}
{{ this.identifier  }}
{{ config.get('materialized')  }}
{{ config.get('transient')  }}


{% set schema = "my_schema" + "_" + target.user + "_" + run_started_at.strftime("%Y%m%d_%H%M")     %}
{{ schema }}
{% do adapter.create_schema(api.Relation.create(database=target.database, schema= schema)) %} #}