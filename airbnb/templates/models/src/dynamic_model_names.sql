{% set tblname = get_dyn_tbl_name("dyn_table") %}
{{
  config(
    materialized = 'table',
    transient = true,
    alias = tblname
    )
}}

select 1 as num
