{{
  config(
    materialized = 'incremental',
    unique_key = 'listing_key',
    )
}}

{# uses snapshot table and source table to build a dimension table incremental model #}
{# Unique key - SK + Timestamp, #} 
{# SCD 2 fields - can change and versioned in snapshot  #}
{# SCD 1 fields - can change but updated with latest version of the record #}
{# Business keys  - used to compare with source #}

{% set t2_cols = ['LISTID','SELLERID' , 'EVENTID' , 'DATEID'] %}
{% set t1_cols = ['NUMTICKETS' , 'PRICEPERTICKET' , 'TOTALPRICE' , 'LISTTIME'] %}
{% set dim_name = 'listing' %}
{% set src_alias = 'src' %}
{% set snap_alias = 't2' %}
{% set inc_alias = 'inc' %}

with src_listing as (
    select 
    * ,
    {{ dbt_utils.generate_surrogate_key(t1_cols) }} as t1_key --Hash T1 Cols at source
    from  
        {{ ref('src_listing') }} 
)
  {% if is_incremental() %}
    , dim_listing_incr as (
        select 
            * ,
        {{ dbt_utils.generate_surrogate_key(t1_cols) }} as t1_key --Hash T1 Cols at dim for incremental builds
        from 
            {{ this }}
    )
  {% endif %}
    SELECT 
      t2.listing_key,
        {% for col in t2_cols -%}
             t2.{{col}} ,
        {%- endfor -%}
        {{ dim_type_1_cols( t1_cols , dim_name , src_alias , snap_alias , inc_alias) }} ,
        {{ update_dbt_timestamp( dim_name , src_alias , snap_alias , inc_alias) }} ,
         t2.dbt_valid_from ,
         t2.dbt_valid_to 
    FROM 
        {{ ref('listing_snapshot') }} t2 --Snapshot
            LEFT JOIN src_listing src 
                ON src.listid = t2.listid  --matches source on unique business keys
        {% if is_incremental() %}
             LEFT JOIN dim_listing_incr inc 
                ON t2.listing_key = inc.listing_key
        {% endif %}

        







  {# Compiled Code   #}
 {# {% if is_incremental() %}
                  case when  t2.dbt_valid_to IS NULL and t2.t1_key <> src.t1_key then src.NUMTICKETS  --if t1_key doesn't match then take from source
                   when inc.listing_key is not null then inc.NUMTICKETS --if t1 hash matches and record found in dim , then use dim val due to no change
                   else t2.NUMTICKETS END AS NUMTICKETS--default             
        {% else %} --initial load/full refresh
                case when t2.dbt_valid_to IS NULL and t2.t1_key <> src.t1_key  then src.NUMTICKETS 
                     else t2.NUMTICKETS END AS NUMTICKETS--if t1 key doesn't match then take source t1 val else retain snapshot t1 val
        {% endif %}
        ,
        {% if is_incremental() %}
                  case when  t2.dbt_valid_to IS NULL and t2.t1_key <> src.t1_key then convert_timezone('UTC',CURRENT_TIMESTAMP)
                   when inc.listing_key is not null then inc.DBT_UPDATED_AT
                   else t2.DBT_UPDATED_AT END AS DBT_UPDATED_AT--default             
        {% else %} 
                case when t2.dbt_valid_to IS NULL and t2.t1_key <> src.t1_key  then convert_timezone('UTC',CURRENT_TIMESTAMP)
                     else t2.DBT_UPDATED_AT END AS DBT_UPDATED_AT--if t1 key doesn't match then take source t1 val else retain snapshot t1 val
        {% endif %} #}