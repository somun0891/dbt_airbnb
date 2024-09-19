{#  
 
 {{ dbt_date.today("America/New_York") }}

{{ dbt_date.now("America/New_York") }}

{{ dbt_date.to_unixtimestamp( dbt_date.now("America/New_York")) }}

{{dbt_date.week_start(dbt_date.today("UTC") ) }}

get next month - sql code:
{{dbt_date.next_month(dbt_date.today("UTC") ) }}

Converted from epochs to timestamp - sql code:
{{
    dbt_date.from_unixtimestamp( dbt_date.to_unixtimestamp(dbt_date.now("UTC")))
}}


{{ run_started_at.strftime('%Y-%m-%d %H:%M:%S') }}


{% set schema_name = "Retail"   %}
{% set customdate = run_started_at.strftime("%Y%m%d")   %}
{% set custom_schema_name =  schema_name | upper  + '_'+ target.user | capitalize  + '_'+ customdate %}

{% do adapter.create_schema(api.Relation.create(database=target.database, schema=adapter.quote(custom_schema_name))) %}  

{% set columns= adapter.get_columns_in_relation("AIRBNB.raw.raw_listings") %}

{% for col in columns %}

   {{ print("Column: " ~ col.column ) }}
   {{ print("Column: " ~ col.is_string() ~ "\n") }} 
   

{% endfor %}


{% set relation = adapter.get_relation(database = "AIRBNB" ,schema = "RAW"  , identifier = "RAW_LISTING"       )  %}

{% if relation %}
    "Source relation exists! "
{% else %}
    "Source relation not found! "
{% endif %} 


{% set room_type =  dbt_utils.get_column_values(table= ref('src_listings'), column='ROOM_TYPE' ) %}
    SELECT 
 {%   for typ in room_type  -%}
        CASE WHEN ROOM_TYPE = '{{typ}}' THEN MAX(MINIMUM_NIGHTS) END AS {{ adapter.quote( typ | replace(" ","_") | replace("/","or")  ~ "_Stays" )  }}
          {%-if not loop.last %},{% endif %}
   {% endfor -%}
   FROM ref('src_listings')

{%  set  fruits =  "Oranges,guava,mango"   %}
{%  set  fruitlist =  fruits.split(",") %}   {# ['Oranges', 'guava', 'mango'] #}

{# 
{{ print(fruitlist) }}


{% set columns= adapter.get_columns_in_relation("AIRBNB.raw.raw_listings") %}
{% set coldef = [] %}
{% set coldict = {} %}
{% for col in columns %}
    {{ print(col) }} 
    {% set cols = ({col.column : col.dtype}) %}

 {%   do coldef.append(cols)  %}
  {%   do coldict.update(cols)  %}

    {{ print( {col.column : col.is_string()} ) }} 
{% endfor  %}

{{ print(coldict) }}


{% set fieldlist = ['LISTING_ID','REVIEW_DATE','REVIEWER_NAME'] %}
 SELECT  {{ dbt_utils.generate_surrogate_key(fieldlist) }}  FROM {{ ref('src_reviews') }} LIMIT 1;

 SELECT 
 LISTING_ID , REVIEW_DATE , COUNT(1) AS Total_Reviews_Per_Day 
 FROM {{ ref('src_reviews') }}
 {{ dbt_utils.group_by(n=2) }} ;
 


 {% set t1_cols = ['NUMTICKETS' , 'PRICEPERTICKET' , 'TOTALPRICE' , 'LISTTIME'] %}
{% set dim_name = 'listing' %}
{% set src_alias = 'src' %}
{% set snap_alias = 't2' %}
{% set inc_alias = 'inc' %}


  {{ dim_type_1_cols( t1_cols , dim_name , src_alias , snap_alias , inc_alias) }} 
 {{ update_dbt_timestamp( dim_name , src_alias , snap_alias , inc_alias) }} 



{% set Countries = ['India','Australia','USA']
%}

 {% set size = Countries|length
%}

{{ print('The size of the array is: ' ~ size) }} 

{% set states = dbt_utils.get_column_values(table = source('tickit','users'),column='STATE') %}


{% if states | list  %}

{{print('State is a list')}}

{% else %}

{{print('State is NOT a list')}}

{% endif %}

select 
array_size({{states}})
from 
    dual;


{% set columns = adapter.get_columns_in_relation(ref("src_listing"))
%}

SELECT ARRAY_SIZE(array_construct(
{%-  for col in columns -%}
    '{{col.column}}'{%- if not loop.last -%},{%- endif -%}
{%- endfor -%}
)
)

{% set dbname = 'testdb'|upper  %}
{% set tablelist = ['listing','users']  %}


with source as (
    select * from "{{dbname}}".information_schema.tables
),counts as (
    select count(1) as rowcount
     from source
    WHERE table_name in 
    (
       {%- for table_name in tablelist  -%}
            '{{table_name|upper}}' {%- if not loop.last -%},{%- endif -%}
        {%- endfor -%}
    )
)
select rowcount 
from counts where rowcount <= ARRAY_SIZE(
                                    ARRAY_CONSTRUCT ( 
                                         {%- for table_name in tablelist  -%}
                                              '{{table_name|upper}}' {%- if not loop.last -%},{%- endif -%}
                                          {%- endfor -%}
                                    )
                                )


{{ dbt_utils.slugify('hello sabyasachi! Do you like ice-cream?')}}



{% set exclude_columns_tableA = ['DBT_UPDATED_AT' , 'DBT_VALID_FROM' , 'DBT_VALID_TO'] |join(',') %}
{% set exclude_columns_tableB = ['LISTID']|join(',') %}
{% set tableA = 'TESTDB.PUBLIC.DIM_LISTING' %}
{% set tableB = 'TESTDB.PUBLIC.LISTING' %}

{{ print(exclude_columns_tableA) }}
{{ print(exclude_columns_tableB) }}

with CompareRelA_to_RelB AS (
  select *
  {%- if exclude_columns_tableA is string and exclude_columns_tableA is not none -%}
  EXCLUDE({{exclude_columns_tableA}})
  {%- endif %}
  from {{tableA}}
  except
  select *
  {%- if exclude_columns_tableB is string and exclude_columns_tableB is not none -%}
  EXCLUDE({{exclude_columns_tableB}})
  {%- endif %}
  from {{tableB}}
),Compare_RelB_to_RelA AS (
 select *
  {%- if exclude_columns_tableB is string and exclude_columns_tableB is not none -%}
  EXCLUDE({{exclude_columns_tableB}})
  {%- endif %}
  from {{tableB}}
  except
  select *
  {%- if exclude_columns_tableA is string and exclude_columns_tableA is not none -%}
  EXCLUDE({{exclude_columns_tableA}})
  {%- endif %}
  from {{tableA}}
)
SELECT * FROM CompareRelA_to_RelB
UNION ALL 
SELECT * FROM Compare_RelB_to_RelA;

#}

{# 
{{print('I am in airbnb...')}}
{% set group_by_columns = ['Id','Name']%}
{% set model =  'EmpA' %}
{% set compare_model =  'EmpB' %}

{% if group_by_columns|length() > 0 %}
  {% set select_gb_cols = group_by_columns|join(', ') + ', ' %}
  {% set join_gb_cols %}
    {% for c in group_by_columns %}
      and a.{{c}} = b.{{c}}
    {% endfor %}
  {% endset %}
  {% set groupby_gb_cols = 'group by ' + group_by_columns|join(',') %}
{% endif %}
#}

{#-- We must add a fake join key in case additional grouping variables are not provided --#}
{#-- Redshift does not allow for dynamically created join conditions (e.g. full join on 1 = 1 --#}
{#-- The same logic is used in fewer_rows_than. In case of changes, maintain consistent logic --#}

{#
{% set group_by_columns = ['id_dbtutils_test_equal_rowcount'] + group_by_columns %}
{% set groupby_gb_cols = 'group by ' + group_by_columns|join(',') %}

with a as (

    select 
      {{select_gb_cols}}
      1 as id_dbtutils_test_equal_rowcount,
      count(*) as count_a 
    from {{ model }}
    {{groupby_gb_cols}}


),
b as (

    select 
      {{select_gb_cols}}
      1 as id_dbtutils_test_equal_rowcount,
      count(*) as count_b 
    from {{ compare_model }}
    {{groupby_gb_cols}}

),
final as (

    select
    
        {% for c in group_by_columns -%}
          a.{{c}} as {{c}}_a,
          b.{{c}} as {{c}}_b,
        {% endfor %}

        count_a,
        count_b,
        abs(count_a - count_b) as diff_count

    from a
    full join b
    on
    a.id_dbtutils_test_equal_rowcount = b.id_dbtutils_test_equal_rowcount
    {{join_gb_cols}}


)

select * from final 
#}

{# Get columns in relation #}
 {% set identifier = "dim_listing" %}
 {% set schema = "AIRBNB.RAW" %}

 {% set relation1 =  schema + "." + identifier %}
{% set collist = [] %}
 {% set columnsObject = adapter.get_columns_in_relation( relation = relation1) %} {#  doesn't work , only takes ref  #} 
{% for cols in columnsObject %}
 {% do collist.append(cols.column)%}
{% endfor %}
{% set columnstring = '"'+collist |join('","')+'"'  %}
 {# {{ log("The list of columns from relation :  " ~ columnstring ,info=true)  }} #}
select 
  {{columnstring}}
FROM 
  {{ref('dim_listing')}}

{# Add column and their dtypes to a dictionary object #}
{% set meta  = {} %}
{% for cols in columnsObject  %}
 {% do meta.update({cols.column : cols.dtype}) %}
{% endfor %}
{{ log('The columns metadata ...' ~ meta , info= true) }}

{# Check relation existence #}
{% set relation_exists = load_relation(relation = ref('dim_listing')) is not none  %}
{% if relation_exists %}
{{ log('The relation is found...' , info= true) }}
{% else %}
{{ log('The relation is missing from the warehouse.It may have been dropped...', info= true)}}
{% endif %}

{% set filtered_cols = dbt_utils.get_filtered_columns_in_relation(from = ref("dim_listing") , except = ["DBT_UPDATED_AT","DBT_VALID_FROM","DBT_VALID_TO"]  )  %}

{{ log('The filtered columns are : ' ~ (filtered_cols | join(',') ) ,info = true ) }}

{# 
  this.database
  this.schema
  this.identifier ~ "_backup"
 #}
{# 
{% set identifier = "numbers" %}

{%- set rel = adapter.get_relation(
  database = "AIRBNB",
  schema = "RAW",
  identifier = identifier
) -%}


{%- set backup_rel = adapter.get_relation(
  database = "AIRBNB",
  schema = "RAW",
  identifier = identifier ~ "_backup"
) -%}

{%if rel is not none and backup_rel is not none %}
{% do adapter.rename_relation(rel , backup_rel) %}
{% else %}
{{ log("No such relation found." , info=true)}}
{% endif %} #}

 

{# {% set numbers_query %}
select distinct
num
from airbnb.raw.numbers
order by 1
{% endset %}

{% set results = run_query(numbers_query) %}

{% if execute %}
{% set numbers = results.columns[0].values()[0] %}
{% else %}
{% set numbers = [] %}
{% endif %}

{{ log(numbers , info=true)  }}

{# {{ dbt.date_trunc("week", "CAST('" ~ run_started_at.strftime("%Y-%m-%d")  ~ "' AS DATE)" ) }} #}
{# 
{{ dbt_date.today("America/Los_Angeles") }}
 

{{dbt_utils.pretty_time(format="%Y-%m-%d")}}  #}

{# {{ dbt_utils.current_timestamp() }} #}

{# WITH date_spine AS (

  {{ dbt_utils.date_spine(
      start_date="to_date('11/01/2009', 'mm/dd/yyyy')",
      datepart="day",
      end_date="dateadd(year, 40, current_date)"
     )
  }}

)
SELECT * FROM date_spine; #}


 {{ drop_audit_columns('testdb.public.dim_listing_clone') }}

   
 {% set str = 'dbt-updated timestamp'  %}
 {# --modules.re.sub(pattern , replacement , str) #}
 {% set string = modules.re.sub('[ -]+' , '_' ,str ) %}

 {{ log(string , info=True) }}

 {% set fieldlist = ['a','b'] %}
{{ dbt_utils.generate_surrogate_key(fields) }}  

{% set fields = audit_columns() | replace("\n", "") | replace(" ", "") %}
{{ fields.split(",") }}

{% set filtered_cols = dbt_utils.get_filtered_columns_in_relation(from = ref("dim_listing") , except =  audit_cols_list() )  %}
{{ filtered_cols }}

{% set modelstring = 'dim_listing' %}
select * from {{ ref(modelstring) }}

  {% set CATGROUP = dbt_utils.get_column_values(table= source('tickit','category') , column = 'CATGROUP') %}

  {{ CATGROUP }}

-- custom macro generate_surrogate_key
 select 
  {{ generate_surrogate_key(t1_cols = ["CATID","CATNAME","CATGROUP"]) }}
  from 
 tickit.raw.category;