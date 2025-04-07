{{
  config(
    materialized = 'ephemeral',
    )
}}


{# with __dbt__cte__my_ephemeral_model_2 as (   #}

select 22 as fun

)
,cte22 as (
     select 33 as play
     UNION ALL 
     SELECT fun from __dbt__cte__my_ephemeral_model_2

{# ) #}