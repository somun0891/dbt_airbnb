-- Use an admin role


USE ROLE SYSADMIN;
-- Create our database and schemas
CREATE DATABASE IF NOT EXISTS AIRBNB;
CREATE SCHEMA IF NOT EXISTS AIRBNB.RAW;
--CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH;


USE ROLE SECURITYADMIN;
-- Create the `transform` role
CREATE ROLE IF NOT EXISTS transform;
GRANT ROLE TRANSFORM TO ROLE SYSADMIN;

USE ROLE SECURITYADMIN;
GRANT USAGE,OPERATE,MODIFY ON WAREHOUSE COMPUTE_WH TO ROLE SYSADMIN WITH GRANT OPTION;
GRANT OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM;

USE ROLE SECURITYADMIN;
-- Create the `dbt` user and assign to role
CREATE USER IF NOT EXISTS dbt_user
  PASSWORD='QAWS123qaws'
  LOGIN_NAME='dbt_user'
  MUST_CHANGE_PASSWORD=FALSE
  DEFAULT_WAREHOUSE='COMPUTE_WH'
  DEFAULT_ROLE='transform'
  DEFAULT_NAMESPACE='AIRBNB.RAW'
  COMMENT='DBT user used for data transformation';
GRANT ROLE transform to USER dbt_user;


-- Set up permissions to role `transform`
USE ROLE SECURITYADMIN;
GRANT ALL ON WAREHOUSE COMPUTE_WH TO ROLE transform; 
GRANT ALL ON DATABASE AIRBNB to ROLE transform;
GRANT ALL ON ALL SCHEMAS IN DATABASE AIRBNB to ROLE transform;
--GRANT ALL ON SCHEMA AIRBNB.RAW to ROLE transform;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE AIRBNB to ROLE transform;
GRANT ALL ON ALL TABLES IN SCHEMA AIRBNB.RAW to ROLE transform;
GRANT ALL ON FUTURE TABLES IN SCHEMA AIRBNB.RAW to ROLE transform;




/* Data loading */

-- Set up the defaults
USE ROLE transform;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE airbnb;
USE SCHEMA RAW;

-- Create our three tables and import the data from S3
CREATE OR REPLACE TABLE raw_listings
                    (id integer,
                     listing_url string,
                     name string,
                     room_type string,
                     minimum_nights integer,
                     host_id integer,
                     price string,
                     created_at datetime,
                     updated_at datetime);

COPY INTO raw_listings (id,
                        listing_url,
                        name,
                        room_type,
                        minimum_nights,
                        host_id,
                        price,
                        created_at,
                        updated_at)
                   from 's3://dbtlearn/listings.csv'
                    FILE_FORMAT = (type = 'CSV' skip_header = 1
                    FIELD_OPTIONALLY_ENCLOSED_BY = '"');


CREATE OR REPLACE TABLE raw_reviews
                    (listing_id integer,
                     date datetime,
                     reviewer_name string,
                     comments string,
                     sentiment string);

COPY INTO raw_reviews (listing_id, date, reviewer_name, comments, sentiment)
                   from 's3://dbtlearn/reviews.csv'
                    FILE_FORMAT = (type = 'CSV' skip_header = 1
                    FIELD_OPTIONALLY_ENCLOSED_BY = '"');


CREATE OR REPLACE TABLE raw_hosts
                    (id integer,
                     name string,
                     is_superhost string,
                     created_at datetime,
                     updated_at datetime);

COPY INTO raw_hosts (id, name, is_superhost, created_at, updated_at)
                   from 's3://dbtlearn/hosts.csv'
                    FILE_FORMAT = (type = 'CSV' skip_header = 1
                    FIELD_OPTIONALLY_ENCLOSED_BY = '"');

--Adding a ingest timestamp columns
ALTER TABLE AIRBNB.raw.raw_reviews
ADD COLUMN  ingest_ts DATETIME  ;

UPDATE AIRBNB.raw.raw_reviews
SET ingest_ts = CURRENT_TIMESTAMP;


/*Query */

select * from raw_listings limit 10; 
select * from raw_hosts;
select * from raw_reviews;

select * from information_schema.columns where table_name='RAW_LISTINGS';

select * 
REPLACE('Host-' || HOST_ID AS HOST_ID)
 RENAME (NAME AS listing_name, PRICE AS PRICE_STR )
 from raw_listings limit 10;


select *
RENAME (DATE AS REVIEW_DATE , COMMENTS AS REVIEW_COMMENTS , SENTIMENT AS REVIEW_SENTIMENT)
FROM raw_reviews LIMIT 10;

--* exclude() replace() rename()
--* replace() rename()
--*exclude() replace()
--*exclude() rename()

select r.* 
  REPLACE('Host-'|| Id AS Id) 
RENAME (ID AS HOST_ID ,NAME AS HOST_NAME) 
from raw_hosts r limit 10;

--DBT Views
select * from raw.src_listings;
select * from raw.src_reviews;
select * from raw.src_hosts;
select * from RAW.dim_hosts_cleaned limit 10;
select * from RAW.dim_listing_cleansed limit 10;

show schemas in database airbnb;

select * from airbnb.raw.dim_listing_cleansed limit 10;



UPDATE AIRBNB.RAW.DIM_HOSTS_CLEANED SET HOST_NAME = null WHERE HOST_ID = 'Host-2164';



select 
IFF((SELECT COUNT(*) FROM airbnb.raw.DIM_HOSTS_CLEANED WHERE HOST_ID IS NULL ) >= 1, 1 , 0)
+ IFF((SELECT COUNT(*) FROM airbnb.raw.DIM_HOSTS_CLEANED WHERE HOST_NAME IS NULL ) >= 1, 1 , 0)  >= 1 as "check"
FROM DUAL;


create or replace table Teams AS 
SELECT 'Knight Riders' AS Team , 'Kolkata' AS State 
UNION ALL 
SELECT 'Sunrisers' AS team , 'Hyderabad' AS State 
UNION ALL 
SELECT 'Daredevils' AS Team , 'Delhi' AS State ;


SELECT DISTINCT Team, State FROM AIRBNB.RAW.Teams;

 WITH data AS (
 SELECT *
    FROM VALUES
           ('Greensboro-High Point-Winston-Salem;Norfolk-Portsmouth-Newport News Washington, D.C. Roanoke-Lynchburg Richmond-Petersburg')
              v( cities))
select  strtok_split_to_table(cities, ';-')
    from data;
    --order by seq, index;


set str = 'The side bar 2 by 4. includes a 54Cllheatsheet and also have 3 by 4. ';
select regexp_substr($str,  '(\\d) (\\w+) (\\d)');
select regexp_substr_all($str,  '(\\d) by (\\d)');
select regexp_substr($str,  '\\d'); --subject,pattern

select regexp_replace($str ,  '(\\d) (\\w+) (\\d)' , ''); --subject,pattern,replacement

select  VALUE from dual , lateral flatten (input => regexp_substr_all($str , '(\\d) (\\w+) (\\d)'));

select SPLIT(CONCAT('1',',','2') , ',');

select REGEXP_REPLACE(rstr , '[^a-zA-Z0-9 ]' ,''  )
from 
(
SELECT  '54Cllheatsheet $3% 2trefssd' as rstr from dual 
)X;

select POSITION('by' , $str, 1   );

set mystr = 'sabyasachinayak';

select 
  case when $mystr IS NULL THEN NULL
      when CONTAINS($mystr ,' ') THEN NULL 
  else $mystr 
end;

select 1/NULLIFZERO(0);

create or replace table AIRBNB.RAW.MARKETUNIT
AS 
SELECT 'RIA' AS MarketUnit
UNION ALL
 SELECT 'Bank' AS MarketUnit
UNION ALL
 SELECT 'Institution' AS MarketUnit;


select current_timestamp();

show tables in schema RAW;

select * from  AIRBNB.RAW.student_list ;

describe table AIRBNB.RAW.student_list;

select LISTAGG(STUDENT_NAME,',') from  AIRBNB.RAW.student_list;

SELECT REPLACE(ARRAY_TO_STRING(REGEXP_SUBSTR_ALL('HI sachi! Hi somun! How are you doing?', '(\\w+)!' ) , ',') , '!' , '');

select * from AIRBNB.raw.raw_reviews limit 10;



select top 10 * from  AIRBNB.snapshots.reviews_check_snapshot;
select * from raw.raw_reviews WHERE LISTING_ID='4666545' AND REVIEW_DATE = '2016-11-03 00:00:00.000';



drop table AIRBNB.snapshots.reviews_check_snapshot;

    create or replace transient table AIRBNB.snapshots.reviews_snapshot
         as
    select *,
        md5(coalesce(cast(LISTING_ID as varchar ), '')
         || '|' || coalesce(cast(ingest_ts as varchar ), '')
        ) as dbt_scd_id,
        ingest_ts as dbt_updated_at,
        ingest_ts as dbt_valid_from,
        nullif(ingest_ts, ingest_ts) as dbt_valid_to
    from (
         select * from raw.src_reviews
    ) sbq
    ;
      
select hash(COALESCE('Make sure to handle Nulls'  ,'') || '|' ||  COALESCE('ALso use Pipe delim to separate key cols' , '') );

SELECT * FROM raw.raw_reviews  WHERE LISTING_ID='35175941' AND DATE = '2019-08-07 00:00:00.000';
SELECT * FROM "AIRBNB"."SNAPSHOTS"."REVIEWS_SNAPSHOT"  WHERE LISTING_ID='35175941' AND REVIEW_DATE = '2019-08-07 00:00:00.000';

--AND INGEST_TS = CURRENT_TIMESTAMP(3)

UPDATE raw.raw_reviews
SET REVIEWER_NAME = 'Reiner P'--,COMMENTS = NULL,SENTIMENT = 'NA'
WHERE LISTING_ID = '35175941' AND DATE = '2019-08-07 00:00:00.000';


      begin;
    merge into "AIRBNB"."SNAPSHOTS"."REVIEWS_SNAPSHOT" as DBT_INTERNAL_DEST
    using "AIRBNB"."SNAPSHOTS"."REVIEWS_SNAPSHOT__dbt_tmp" as DBT_INTERNAL_SOURCE
    on DBT_INTERNAL_SOURCE.dbt_scd_id = DBT_INTERNAL_DEST.dbt_scd_id

    when matched
     and DBT_INTERNAL_DEST.dbt_valid_to is null
     and DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete')
        then update
        set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to

    when not matched
     and DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
        then insert ("LISTING_ID", "REVIEW_DATE", "REVIEWER_NAME", "REVIEW_COMMENTS", "REVIEW_SENTIMENT", "INGEST_TS", "DBT_UPDATED_AT", "DBT_VALID_FROM", "DBT_VALID_TO", "DBT_SCD_ID")
        values ("LISTING_ID", "REVIEW_DATE", "REVIEWER_NAME", "REVIEW_COMMENTS", "REVIEW_SENTIMENT", "INGEST_TS", "DBT_UPDATED_AT", "DBT_VALID_FROM", "DBT_VALID_TO", "DBT_SCD_ID")

;
    commit;
  

select dbt_scd_id , count(1)
from 
(
  select *,
        md5(coalesce(cast(LISTING_ID||REVIEW_DATE||REVIEWER_NAME as varchar ), '')
         || '|' || coalesce(cast(to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as varchar ), '')
        ) as dbt_scd_id,
        to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as dbt_updated_at,
        to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as dbt_valid_from,
        nullif(to_timestamp_ntz(convert_timezone('UTC', current_timestamp())), to_timestamp_ntz(convert_timezone('UTC', current_timestamp()))) as dbt_valid_to
    from (
select 
*
 from AIRBNB.RAW.src_reviews

    ) sbq
)f 
group by dbt_scd_id
having count(1) > 1;

select * from  AIRBNB.RAW.src_listings;

select * from AIRBNB.raw.raw_reviews limit 1;

select 
LISTING_ID ,REVIEW_DATE ,REVIEWER_NAME ,REVIEW_SENTIMENT
from AIRBNB.RAW.src_reviews
group by LISTING_ID ,REVIEW_DATE ,REVIEWER_NAME ,REVIEW_SENTIMENT
having count(1) > 1;

select * from  raw.raw_reviews where listing_id = 28535860
and DATE = '2020-09-27 00:00:00.000' and reviewer_name = 'Hannes';


select * from  AIRBNB.RAW.src_reviews where listing_id = 45475252
and REVIEW_DATE = '2020-12-18 00:00:00.000' and reviewer_name = 'Johannes';





 SELECT
md5(cast(coalesce(cast(LISTING_ID as TEXT), '_dbt_utils_surrogate_key_null_') 
||
 '-' || coalesce(cast(REVIEW_DATE as TEXT), '_dbt_utils_surrogate_key_null_') ||
 '-' || coalesce(cast(REVIEWER_NAME as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT))  FROM AIRBNB.RAW.src_reviews LIMIT 1;






        begin;
    merge into "AIRBNB"."SNAPSHOTS"."REVIEWS_CHECK_SNAPSHOT" as DBT_INTERNAL_DEST
    using "AIRBNB"."SNAPSHOTS"."REVIEWS_CHECK_SNAPSHOT__dbt_tmp" as DBT_INTERNAL_SOURCE
    on DBT_INTERNAL_SOURCE.dbt_scd_id = DBT_INTERNAL_DEST.dbt_scd_id

    when matched
     and DBT_INTERNAL_DEST.dbt_valid_to is null
     and DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete')
        then update
        set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to

    when not matched
     and DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
        then insert ("LISTING_ID", "REVIEW_DATE", "REVIEWER_NAME", "REVIEW_COMMENTS", "REVIEW_SENTIMENT", "INGEST_TS", "DBT_UPDATED_AT", "DBT_VALID_FROM", "DBT_VALID_TO", "DBT_SCD_ID")
        values ("LISTING_ID", "REVIEW_DATE", "REVIEWER_NAME", "REVIEW_COMMENTS", "REVIEW_SENTIMENT", "INGEST_TS", "DBT_UPDATED_AT", "DBT_VALID_FROM", "DBT_VALID_TO", "DBT_SCD_ID")

;
    commit;


    show schemas in  database AIRBNB;

    drop schema AIRBNB.RETAIL_DBT_USER_20240125;
    drop schema AIRBNB.CUSTOM_SCHEMA_NAME;

    
    SELECT  TO_NUMBER(TO_CHAR('2023-01-01'::DATE,'YYYYMMDD'),'99999999');


    SELECT  IFF(TRY_TO_NUMBER('2023-01-01','99999999') IS NULL ,1,0);
        SELECT  IFF( (SELECT 1=1) ,1,0);


        SELECT 
        'First' as Col1 ,
        'Second' as Col2 ,
        'Third' as Col3 ,
        ARRAY_CONSTRUCT(Col1 ,Col2,Col3) as ListCol
        FROM dual;


      SELECT PREVIOUS_DAY( DATEADD( 'day' , -1 ,DATE_TRUNC('MONTH', CURRENT_TIMESTAMP)  ) , 'Sunday');

      select dayname(CURRENT_TIMESTAMP);

    select '1.995'::NUMBER(10,2);
    select PARSE_JSON('{"name":"sachi"}'):name;

      select dayofweek($customdate::date);


        SET mindate = '2024-01-27';
        SET maxdate = '2024-04-30';  

        with recursive cte as (
        SELECT CASE WHEN dayofweek($mindate::date) = 0 THEN $mindate ELSE PREVIOUS_DAY( $mindate::date , 'Sunday') END as weekstart
          , CASE WHEN dayofweek($mindate::date) = 6 THEN  $mindate ELSE  NEXT_DAY( $mindate::date , 'Saturday')  END as weekend
        UNION ALL 
        SELECT weekstart + 7 , weekend + 7
        from CTE
         WHERE weekstart <= $maxdate
        )
        select * from cte  WHERE weekstart <= $maxdate;

    USE ROLE ACCOUNTADMIN;
      GRANT OWNERSHIP ON TABLE dim_listing_CLONE TO ROLE TRANSFORM REVOKE CURRENT GRANTS;
      select * from testdb.public.dim_listing limit 1;
        select * from testdb.public.dim_listing_CLONE limit 1;
        CREATE OR REPLACE TRANSIENT TABLE testdb.public.dim_listing_CLONE CLONE testdb.public.dim_listing;

        show schemas;
        show roles;
        use role transform;
        show tables like '%execution%';

        select * from RAW.fct_dbt__test_executions ;

    show tables in schema AIRBNB.RAW;

        select get_ddl('view','RAW.fct_dbt__test_executions');
        select * from RAW.fct_dbt__test_executions;
        SELECT COUNT(1) FROM  RAW.FCT_DBT__MODEL_EXECUTIONS;
       
        select 
        ARRAY_TO_STRING (DEPENDS_ON_NODES , ','),
        * from AIRBNB.raw.MODELS;

select * from airbnb.public.manual_book1;
        
        ALTER TABLE testdb.public.dim_listing_clone  DROP COLUMN IF EXISTS
  update_ts;





with all_values as (
  select
    distinct
      ACTION as value_col
  FROM
    AIRBNB.PUBLIC.MANUAL_BOOK1
), validation_errors as (
    select value_col
    FROM
      all_values
    WHERE
       value_col in ('BUY','SELL')
)
select  * from validation_errors;










with src_listing as (
    select
    * ,


md5(cast(coalesce(cast(NUMTICKETS as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(PRICEPERTICKET as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(TOTALPRICE as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(LISTTIME as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as t1_key --Hash T1 Cols at source
    from
        AIRBNB.RAW.src_listing
)

    , dim_listing_incr as (
        select
            * ,


md5(cast(coalesce(cast(NUMTICKETS as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(PRICEPERTICKET as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(TOTALPRICE as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(LISTTIME as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as t1_key --Hash T1 Cols at dim for incremental builds
        from
            TESTDB.public.dim_listing
    )


    SELECT
      t2.listing_key,
        t2.LISTID ,t2.SELLERID ,t2.EVENTID ,t2.DATEID ,
    case when t2.dbt_valid_to IS NULL and t2.t1_key <> src.t1_key then src.NUMTICKETS
                   when inc.listing_key is not null then inc.NUMTICKETS
                   else  t2.NUMTICKETS END AS NUMTICKETS,
    case when t2.dbt_valid_to IS NULL and t2.t1_key <> src.t1_key then src.PRICEPERTICKET
                   when inc.listing_key is not null then inc.PRICEPERTICKET
                   else  t2.PRICEPERTICKET END AS PRICEPERTICKET,
    case when t2.dbt_valid_to IS NULL and t2.t1_key <> src.t1_key then src.TOTALPRICE
                   when inc.listing_key is not null then inc.TOTALPRICE
                   else  t2.TOTALPRICE END AS TOTALPRICE,
    case when t2.dbt_valid_to IS NULL and t2.t1_key <> src.t1_key then src.LISTTIME
                   when inc.listing_key is not null then inc.LISTTIME
                   else  t2.LISTTIME END AS LISTTIME ,
        case when  t2.dbt_valid_to IS NULL and t2.t1_key <> src.t1_key then convert_timezone('UTC',CURRENT_TIMESTAMP)
                when inc.listing_key is not null then inc.DBT_UPDATED_AT
                else t2.DBT_UPDATED_AT END AS DBT_UPDATED_AT ,
         t2.dbt_valid_from ,
         t2.dbt_valid_to
    FROM
        TESTDB.SNAPSHOT.listing_snapshot t2 --Snapshot
            LEFT JOIN src_listing src
                ON src.listid = t2.listid  --matches source on unique business keys


             LEFT JOIN dim_listing_incr inc
                ON t2.listing_key = inc.listing_key;



   with Recency as  (
  select

    MAX(date)as Most_Recent_Date
  from
      AIRBNB.public.manual_book1

)
select
  Most_Recent_Date,
 cast(
    dateadd(
        day,
        -3,
        convert_timezone('UTC', current_timestamp())
        )
 as timestamp) as Threshold
FROM
  Recency
WHERE   Most_Recent_Date < cast(

    dateadd(
        day,
        -3,
        convert_timezone('UTC', current_timestamp())
        )

 as timestamp);             



 CREATE OR REPLACE TABLE EmpA 
 AS 
 SELECT 1 as id , 'Sachi'  as name , 900.00 as Salary
 UNION ALL 
  SELECT 2 , 'Nayak' , 1000.00
  UNION ALL 
  SELECT 4 , 'Tim' , 1000.00; 

 CREATE OR REPLACE TABLE EmpB 
 AS 
 SELECT 1 as id , 'Sachi'  as name , 900.00 as Salary
 UNION ALL 
SELECT 1 , 'Sachi' , 900.00
UNION ALL 
SELECT 3 , 'Arpit' , 1000.00
  UNION ALL 
  SELECT 4 , 'Tim' , 1000.00; 


 with EmpACTE AS (
  SELECT Id,Name, COUNT(*) AS CNT
   FROM EmpA
  group by Id,Name
 ),  EmpBCTE AS (
  SELECT Id,Name, COUNT(*) AS CNT
   FROM EmpB
  group by Id,Name
 )
 select 
 A.ID as ID_A ,
B.ID as ID_B ,
 A.NAME as NAME_A ,
B.NAME as NAME_B ,
A.CNT as cnt_a , 
B.CNT as cnt_B , 
ABS (COALESCE(A.CNT ,0) - COALESCE(B.CNT , 0)) AS CNT
 from 
 EmpACTE A FULL OUTER JOIN EmpBCTE B
 ON A.ID = B.ID
 AND A.NAME = B.NAME;




with a as (

    select
      Id, Name,
      1 as id_dbtutils_test_equal_rowcount,
      count(*) as count_a
    from EmpA
    group by id_dbtutils_test_equal_rowcount,Id,Name


),
b as (

    select
      Id, Name,
      1 as id_dbtutils_test_equal_rowcount,
      count(*) as count_b
    from EmpB
    group by id_dbtutils_test_equal_rowcount,Id,Name

),
final as (

    select

        a.id_dbtutils_test_equal_rowcount as id_dbtutils_test_equal_rowcount_a,
          b.id_dbtutils_test_equal_rowcount as id_dbtutils_test_equal_rowcount_b,
        a.Id as Id_a,
          b.Id as Id_b,
        a.Name as Name_a,
          b.Name as Name_b,


        count_a,
        count_b,
        abs(count_a - count_b) as diff_count

    from a
    full join b
    on
    a.id_dbtutils_test_equal_rowcount = b.id_dbtutils_test_equal_rowcount


      and a.Id = b.Id

      and a.Name = b.Name




)

select * from final;

show tables in database AIRBNB;


CREATE OR REPLACE TABLE airbnb.raw.numbers 
AS 
SELECT 1 AS NUM 
UNION ALL SELECT 2;

CREATE OR REPLACE TABLE airbnb.raw.numbers_backup
AS 
SELECT 3 AS NUM_bkp;


select * from airbnb.raw.numbers_backup;

set date_day = '2024-02-04'::date;

select count(1) from AIRBNB.RAW.student_list;
select count(1) from TESTDB.PUBLIC.student_list;
select * from TESTDB.PUBLIC.student_list;

desc table TESTDB.PUBLIC.student_list;
select * from information_schema.columns where table_name = UPPER('student_list');

SELECT 
  $date_day as date_Day,
  DAYNAME($date_day) AS day_name,
 DATE_PART('year',$date_day ) as year_actual ,
 DATE_PART('month',$date_day ) as month_actual ,
  DATE_PART('quarter',$date_day ) as quarter_actual ,
 DATE_PART('dayofweek', $date_day) + 1  as day_of_week,
CASE WHEN dayname($date_day) = 'Sun' then $date_day else  DATEADD( day , -1 , DATE_TRUNC('week', $date_day) ) end as first_day_of_week,
 CASE WHEN day_name = 'Sun' THEN week($date_day) + 1 ELSE DATE_PART(weekofyear , $date_day) end as week_of_year,
      CASE WHEN month_actual < 2
        THEN year_actual
        ELSE (year_actual+1) END   AS fiscal_year,
      CASE WHEN month_actual < 2 THEN '4'
        WHEN month_actual < 5 THEN '1'
        WHEN month_actual < 8 THEN '2'
        WHEN month_actual < 11 THEN '3'
        ELSE '4' END  as fiscal_quarter,
       ROW_NUMBER() OVER (PARTITION BY year_actual, quarter_actual ORDER BY date_Day)  AS day_of_quarter,  
        MONTHNAME($date_day) as Monthname,
        TO_CHAR($date_day,'MON') AS Monthname_Alt,
        TO_CHAR($date_day,'MMMM') AS MonthFullname,
        DATE_TRUNC('month', $date_day) as first_day_of_month,
        last_day($date_day , 'month') as last_day_of_month,
        DATE_TRUNC('quarter', $date_day) as first_day_of_quarter,
        last_day($date_day , 'quarter') as last_day_of_quarter
        
         ;      

--Extract last name
SET name = 'Dr Shri sabyasachi nayak';
SELECT SPLIT(TRIM($name) , ' ') 
,ARRAY_SLICE(SPLIT(TRIM($name), ' '), 1, 10) 
,ARRAY_TO_STRING(ARRAY_SLICE(SPLIT(TRIM($name), ' '), 1, 10), ' ') as All_Except_First
,SPLIT(TRIM($name), ' ') AS Name_array
,IFF(ARRAY_SIZE(Name_array) > 1 ,  Name_array[ARRAY_SIZE(Name_array) - 1] , NULL ) AS Last_Name;

--Check whether given array is present in a larger array
-- SET subset = (SELECT ARRAY_CONSTRUCT('Chips','Donut')); --Unsupported feature 'assignment from non-constant source expression'.
-- SET superset = ARRAY_CONSTRUCT('Soda','Chips','Burger','Donut','Nachos'); 

SET DT = CURRENT_DATE();

SELECT IFF(ARRAY_SIZE(ARRAY_INTERSECTION( ARRAY_CONSTRUCT('Chips','Donut') 
                                     ,ARRAY_CONSTRUCT('Soda','Chips','Burger','Donut','Nachos'))) > 0 , TRUE , FALSE) AS Subset_In_SuperSet ;


SET ADDRESS = '"546 Elmwood Drive ,~ BL Plaza %, New &Jersey , U.S.A';
SELECT REGEXP_REPLACE($ADDRESS ,'["?!~&%]' , '');
SELECT REGEXP_REPLACE($ADDRESS ,'[^a-zA-Z0-9, ]' , '');
SELECT REGEXP_SUBSTR($ADDRESS ,'.*(~ \\w).*')  ;

