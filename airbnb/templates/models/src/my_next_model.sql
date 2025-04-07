{{
  config(
    materialized = 'ephemeral'
    )
}}
with next_model as (
 select '999' as interesting_number
)
,my_ephemeral_model_ref as (

    select * from {{ref('my_ephemeral_model')}}

),my_ephemeral_model_2_ref as (

    select * from {{ref('my_ephemeral_model_2')}}

),something_else as (

    select 123 from dual
)
select * from something_else