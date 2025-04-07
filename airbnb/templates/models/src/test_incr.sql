{% set filter =  get_timestamp(current_timestamp())  %}


{{
  config(
    materialized = 'incremental',
    unique_key = 'id',
    incremental_strategy = 'merge',
    incremental_predicates = filter
   
  )

}}



  select 1 as id , 'sachi' as name , current_timestamp() as audit_ts
  union all 
  select 2 , 'somun' ,  dateadd(hour , -1 , current_timestamp())

