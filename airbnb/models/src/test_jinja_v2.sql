
{% call set_sql_header(config) -%}
  CREATE OR REPLACE TRANSIENT TABLE AIRBNB.RAW.TRN_DUMMY
  COPY GRANTS
   AS
   SELECT * FROM (VALUES (1,4),(3,6)) a(col1 , col2);
{%- endcall %}

select col1,col2 from AIRBNB.RAW.TRN_DUMMY
UNION ALL 
select 0,0 from  AIRBNB.raw.raw_reviews WHERE False

-- {{target.role}}
-- {{target.user}}
-- {{target.warehouse}}