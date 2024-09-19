{% snapshot listing_snapshot %}

{{
   config(
       target_database='TESTDB',
       target_schema='SNAPSHOT',
       unique_key='LISTID',
       strategy='check',
       check_cols = ['SELLERID', 'EVENTID', 'DATEID']  
   )
}}

{#
     business key(s)/unique key - used to join with the source , uniquely identifier a record
     SK hash = hash of business key(s) + current_timestamp() in YYYYMMDDH24MISS
     type- 2 cols - columns listed in check_cols config
     type- 1 cols - hashed and stored for comparison while creating incremental dim with both T1 & t2

  #}
{% set t1_cols  = ['NUMTICKETS' , 'PRICEPERTICKET' , 'TOTALPRICE' , 'LISTTIME'] %}
select 
    * ,
    CAST(LISTID AS VARCHAR) ||'-'|| TO_VARCHAR(CONVERT_TIMEZONE('UTC' , CURRENT_TIMESTAMP::TIMESTAMP_NTZ(3)) , 'YYYYMMDDH24MISS') as listing_key --surrogate key = list_id + current ts
    ,{{ dbt_utils.generate_surrogate_key(t1_cols) }}  as t1_key --compute hash for type 1 fields 
    from 
    {{ ref('src_listing') }}




{% endsnapshot %}