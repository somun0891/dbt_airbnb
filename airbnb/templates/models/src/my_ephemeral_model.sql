{{
  config(
    materialized = 'ephemeral',
    )
}}


{# with __dbt__cte__my_ephemeral_model as ( #}

select 1 as fun

)
,cte2 as (
     select 2 as play
     UNION ALL 
     SELECT fun from __dbt__cte__my_ephemeral_model

{# ) #}

{# select * from __dbt__cte__my_ephemeral_model #}