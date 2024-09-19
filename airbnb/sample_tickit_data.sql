USE ROLE SYSADMIN;
CREATE DATABASE TICKIT;
CREATE DATABASE adventureworks;

USE ROLE SECURITYADMIN;

GRANT ALL ON DATABASE TESTDB TO ROLE TRANSFORM;
GRANT ALL ON DATABASE adventureworks TO ROLE TRANSFORM;

-- GRANT USAGE ON DATABASE TESTDB TO ROLE TRANSFORM;
-- GRANT USAGE ON SCHEMA TESTDB.PUBLIC TO ROLE TRANSFORM;
GRANT OWNERSHIP ON DATABASE TICKIT TO ROLE TRANSFORM;
--GRANT USAGE ON STAGE TESTDB.PUBLIC.EXT_STG_DATA TO ROLE TRANSFORM;

USE ROLE TRANSFORM; 
CREATE SCHEMA IF NOT EXISTS AIRBNB.RAW;
CREATE SCHEMA IF NOT EXISTS TESTDB.SNAPSHOT;
CREATE SCHEMA IF NOT EXISTS adventureworks.dbo;
list @TESTDB.PUBLIC.EXT_STG_DATA;

-- create table users_demo(
-- 	userid integer not null,
-- 	likemusicals boolean,
-- 	change_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP::TIMESTAMP_NTZ
-- );

-- insert into users_demo (userid,likemusicals)
-- select 1,true;

-- select * from users_demo;

USE DATABASE TICKIT;
USE SCHEMA RAW;

create or replace table users(
	userid integer not null,
	username varchar(8),
	firstname varchar(30),
	lastname varchar(30),
	city varchar(30),
	state varchar(2),
	email varchar(100),
	phone varchar(14),
	likesports boolean,
	liketheatre boolean,
	likeconcerts boolean,
	likejazz boolean,
	likeclassical boolean,
	likeopera boolean,
	likerock boolean,
	likevegas boolean,
	likebroadway boolean,
	likemusicals boolean,
	change_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP::TIMESTAMP_NTZ
	);





    create or replace  table venue(
	venueid smallint not null,
	venuename varchar(100),
	venuecity varchar(30),
	venuestate char(2),
	venueseats int,
	change_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP::TIMESTAMP_NTZ
	);


create or replace  table category(
	catid smallint not null,
	catgroup varchar(10),
	catname varchar(10),
	catdesc varchar(50),
	change_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP::TIMESTAMP_NTZ
);

   create or replace   table date(
	dateid smallint not null,
	caldate date not null,
	day character(3) not null,
	week smallint not null,
	month character(5) not null,
	qtr character(5) not null,
	year smallint not null,
	holiday boolean default('N'));


    
create or replace table event(
	eventid integer not null,
	venueid smallint not null,
	catid smallint not null,
	dateid smallint not null,
	eventname varchar(200),
	starttime timestamp_NTZ,
	change_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP::TIMESTAMP_NTZ
  );
 
 
--  describe table event;

 create  or replace table listing(
	listid integer not null,
	sellerid integer not null,
	eventid integer not null,
	dateid smallint not null ,
	numtickets smallint not null,
	priceperticket decimal(8,2),
	totalprice decimal(8,2),
	listtime timestamp_NTZ,
     change_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP::TIMESTAMP_NTZ
 );



--  describe table listing;

create or replace table sales(
	salesid integer not null,
	listid integer not null,
	sellerid integer not null,
	buyerid integer not null,
	eventid integer not null,
	dateid smallint not null,
	qtysold smallint not null,
	pricepaid numeric(8,2),
	commission decimal(8,2),
	saletime timestamp_NTZ(3)
	,change_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP::TIMESTAMP_NTZ
);

--     describe table sales;

-- select * from users limit 10;

copy INTO users 
from @TESTDB.PUBLIC.EXT_STG_DATA/tickit/allusers_pipe.txt
file_format = (type=csv field_delimiter = '|' error_on_column_count_mismatch=false );

UPDATE users
SET  change_timestamp = CURRENT_TIMESTAMP::TIMESTAMP_NTZ ;


copy INTO venue 
from @TESTDB.PUBLIC.EXT_STG_DATA/tickit/venue_pipe.txt
file_format = (type=csv field_delimiter = '|' error_on_column_count_mismatch=false );

UPDATE venue
SET  change_timestamp = CURRENT_TIMESTAMP::TIMESTAMP_NTZ ;

copy INTO category 
from @TESTDB.PUBLIC.EXT_STG_DATA/tickit/category_pipe.txt
file_format = (type=csv field_delimiter = '|' error_on_column_count_mismatch=false );

UPDATE category
SET  change_timestamp = CURRENT_TIMESTAMP::TIMESTAMP_NTZ ;



copy INTO date
from @TESTDB.PUBLIC.EXT_STG_DATA/tickit/date2008_pipe.txt
file_format = (type=csv field_delimiter = '|' error_on_column_count_mismatch=false );


copy INTO event
from @TESTDB.PUBLIC.EXT_STG_DATA/tickit/allevents_pipe.txt
file_format = (type=csv field_delimiter = '|' error_on_column_count_mismatch=false );

UPDATE event
SET  change_timestamp = CURRENT_TIMESTAMP::TIMESTAMP_NTZ ;

copy INTO listing
from @TESTDB.PUBLIC.EXT_STG_DATA/tickit/listings_pipe.txt
file_format = (type=csv field_delimiter = '|' error_on_column_count_mismatch=false );


UPDATE listing
SET  change_timestamp = CURRENT_TIMESTAMP::TIMESTAMP_NTZ ;


copy INTO sales
from @TESTDB.PUBLIC.EXT_STG_DATA/tickit/sales_tab.txt
file_format = (type=csv field_delimiter = '\t' error_on_column_count_mismatch=false );

UPDATE sales
SET  change_timestamp = CURRENT_TIMESTAMP::TIMESTAMP_NTZ ;

select * from TICKIT.RAW.sales limit 10;

-- GRANT ALL PRIVILEGES ON DATABASE TESTDB TO ROLE TRANSFORM;
-- GRANT ALL ON ALL SCHEMAS IN DATABASE TESTDB to ROLE transform;
-- --GRANT ALL ON SCHEMA AIRBNB.RAW to ROLE transform;
-- GRANT ALL ON FUTURE SCHEMAS IN DATABASE TESTDB to ROLE transform;
-- GRANT ALL ON ALL TABLES IN SCHEMA TESTDB.PUBLIC to ROLE transform;
-- GRANT ALL ON FUTURE TABLES IN SCHEMA TESTDB.PUBLIC to ROLE transform;


select TOP 10 * from AIRBNB.raw.src_listing; --src view
select TOP 10 * from TESTDB.SNAPSHOT.listing_snapshot; --snapshot
select TOP 10 * from TESTDB.public.dim_listing; --dim 

-- DROP TABLE TESTDB.SNAPSHOT.listing_snapshot;

select distinct STATE from TESTDB.PUBLIC.users;

  select * from "TESTDB".information_schema.tables;

--RAW TABLE EXISTENCE ,-VE CHECK
with source as (
    select * from "TESTDB".information_schema.tables
),counts as (
    select count(1) as rowcount
     from source
    WHERE table_name in
    ('LISTING','USERS')
)
select rowcount
from counts where rowcount < ARRAY_SIZE(
                                    ARRAY_CONSTRUCT ('LISTING','USERS')
                                );


{% set date = '';

SELECT  TO_NUMBER(TO_CHAR({{ date }}::DATE,'YYYYMMDD'),'99999999');    

-- SELECT COUNT(1) FROM TESTDB.PUBLIC.LISTING;
-- SELECT COUNT(1) FROM  TESTDB.PUBLIC.DIM_LISTING;

 SELECT * FROM TESTDB.PUBLIC.LISTING LIMIT 1;
 SELECT * FROM  TESTDB.PUBLIC.DIM_LISTING LIMIT 1;

with CompareRelA_to_RelB AS (
  select *EXCLUDE(DBT_UPDATED_AT,DBT_VALID_FROM,DBT_VALID_TO)
  from TESTDB.PUBLIC.DIM_LISTING
  except
  select *EXCLUDE(LISTID)
  from TESTDB.PUBLIC.LISTING
),Compare_RelB_to_RelA AS (
 select *EXCLUDE(LISTID)
  from TESTDB.PUBLIC.LISTING
  except
  select *EXCLUDE(DBT_UPDATED_AT,DBT_VALID_FROM,DBT_VALID_TO)
  from TESTDB.PUBLIC.DIM_LISTING
)
SELECT * FROM CompareRelA_to_RelB
UNION ALL
SELECT * FROM Compare_RelB_to_RelA;

use role accountadmin;
-- select * from "TESTDB".SNAPSHOT.listing_snapshot limit 1;

-- select * from TESTDB.PUBLIC.dim_listing limit 1;

-- select * from TICKIT.RAW.sales;

USE ROLE TRANSFORM;
select TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION, DATA_TYPE from TICKIT.information_schema.columns where 
TABLE_CATALOG='TICKIT' AND TABLE_SCHEMA='RAW'
order by TABLE_NAME, ORDINAL_POSITION;

 --AND TABLE_NAME=UPPER('sales');


select * from tickit.raw.sales limit 10;
select count( eventid) from tickit.raw.sales where eventid is null;
select TOP 10 * from tickit.raw.sales ;


-- SALESID
-- EVENTID
-- PRICEPAID
-- QTYSOLD
-- CHANGE_TIMESTAMP
-- DATEID
-- SALETIME
-- COMMISSION
-- BUYERID
-- SELLERID
-- LISTID
show grants to role transform;
use role accountadmin;
select * from AIRBNB.RAW.MODEL_EXECUTIONS;

GRANT OWNERSHIP ON SCHEMA AIRBNB.RAW TO ROLE TRANSFORM REVOKE CURRENT GRANTS;

use role transform;
 select min(QTYSOLD) ,max(QTYSOLD) from "TICKIT".raw.sales limit 1;



 select * from AIRBNB.RAW.base_tickit_sales limit 1;

 select
    1
from AIRBNB.RAW.base_tickit_sales

where not(COMMISSION > 100);



select
    count(1)
from AIRBNB.RAW.base_tickit_sales

where NOT(PRICEPAID  > COMMISSION);


select * from tickit.raw.users limit 10;
DESCRIBE TABLE tickit.raw.users;

DECLARE 
	sqlquery VARCHAR := 'select TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION, DATA_TYPE from TICKIT.information_schema.columns where TABLE_CATALOG=''TICKIT'' 
	AND TABLE_SCHEMA=''RAW'' AND TABLE_NAME = ''USERS'' order by TABLE_NAME, ORDINAL_POSITION';
	res resultset;
	t VARCHAR := '';
BEGIN 
 res := (EXECUTE IMMEDIATE sqlquery);

for r in res do 
	t  :=  t || '- name: ' || r.COLUMN_NAME || '\n' || '  description: description for ' || r.COLUMN_NAME || '\n' ;

end for;
RETURN t;

--RETURN TABLE(RES);
END;




with source as (
      select * from "TICKIT".raw.users
),
renamed as (
    select
        "USERID",
        "USERNAME",
        "FIRSTNAME",
        "LASTNAME",
        "CITY",
        "STATE",
        "EMAIL",
        "PHONE",
        "LIKESPORTS",
        "LIKETHEATRE",
        "LIKECONCERTS",
        "LIKEJAZZ",
        "LIKECLASSICAL",
        "LIKEOPERA",
        "LIKEROCK",
        "LIKEVEGAS",
        "LIKEBROADWAY",
        "LIKEMUSICALS",
        "CHANGE_TIMESTAMP"

    from source
)
select * from renamed;


select
    {# In TSQL, subquery aggregate columns need aliases #}
    {# thus: a filler col name, 'filler_column' #}
    {{select_gb_cols}}
    count(distinct {{ column_name }}) as filler_column

from {{ model }}

  {{groupby_gb_cols}}

having count(distinct {{ column_name }}) = 1;


select
   -- USERID, USERNAME,
    count(distinct CITY) as filler_column
from 
		"TICKIT".raw.users
--  {{groupby_gb_cols}}
having count(distinct CITY) > 1;

--CREATE OR REPLACE TRANSIENT TABLE TICKIT.RAW.users_clone CLONE TICKIT.RAW.users;

with a as 
(
	SELECT 
		USERID , USERNAME 
		,count(*) as count_a
	FROM 
		"TICKIT".raw.users
	group by 
			USERID , USERNAME 
), b as 
(
	SELECT 
		USERID , USERNAME 
		,count(*) as count_b
	FROM 
		"TICKIT".raw.users_clone
	group by 
			USERID , USERNAME 
)

SELECT 
a.UserID AS UserID_a,
a.USERNAME AS USERNAME_a,
b.UserID AS UserID_b,
b.USERNAME AS USERNAME_b,
count_a,
count_b, 
abs(COALESCE(count_a ,0) - COALESCE(count_b,0)) as diff_count
FROM a 
FULL JOIN b on 
	a.USERID = b.USERID AND
    a.USERNAME = b.USERNAME
HAVING diff_count > 0;



-- select *  from "TICKIT".raw.users_clone WHERE USERID < 100;




with a as
(
        SELECT
                USERID,USERNAME,
    1 as default_a
                ,count(*) as count_a
        FROM
                "TICKIT".raw.users
        group by USERID,USERNAME,
    1
), b as
(
        SELECT
                USERID,USERNAME,
    1 as default_b
                ,count(*) as count_b
        FROM
                "TICKIT".raw.users_clone
        group by USERID,USERNAME,
  1
)
SELECT

a.USERID  AS USERID_a,
b.USERID  AS USERID_b,
a.USERNAME  AS USERNAME_a,
b.USERNAME  AS USERNAME_b,
count_a,
count_b,
abs(COALESCE(count_a ,0) - COALESCE(count_b,0)) as diff_count
FROM a
FULL JOIN b on
a.default_a = b.default_b

     and a.USERID = b.USERID
     and a.USERNAME = b.USERNAME
HAVING diff_count > 0;


use role transform;
use schema AIRBNB.RAW;

select * from AIRBNB.RAW.BASE_TICKIT_SALES;
CREATE OR REPLACE TRANSIENT TABLE AIRBNB.RAW.sales_deprecated
CLONE AIRBNB.RAW.BASE_TICKIT_SALES;

select * from AIRBNB.RAW.BASE_TICKIT_USERS;

select * from AIRBNB.RAW.BASE_TICKIT_SALES limit 10;


SHOW TABLES LIKE '%BASE_TICKIT_SALES%' IN SCHEMA AIRBNB.RAW;
--SHOW VIEWS LIKE '%BASE_TICKIT_SALES%' IN SCHEMA AIRBNB.RAW;

--CHECK Constraints on table - 
DESCRIBE TABLE BASE_TICKIT_SALES;


UPDATE AIRBNB.RAW.sales_deprecated
SET Qtysold = 2 
where salesid in (5,6);



UPDATE AIRBNB.RAW.BASE_TICKIT_SALES
SET Qtysold = -2 
where salesid in (5,6);

 select * from AIRBNB.dbt_test__audit.MAX_QTY_ALWAYS_POSITIVE;

SHOW VIEWS IN SCHEMA AIRBNB.dbt_test__audit;

SELECT * FROM airbnb.raw.tests WHERE PACKAGE_NAME = 'airbnb';
SELECT * FROM airbnb.raw.TEST_EXECUTIONS WHERE status = 'fail'; -- FAILURES > 0

select * from  airbnb.RAW.test_results_central;
show tables;

select * from AIRBNB.dbt_test__audit.avg_sales_per_day_gt_one;

--compare values of a column across 2 tables
select 
coalesce(a.salesid , b.salesid) AS salesid ,
a.qtysold as qtysold_a,
b.qtysold as qtysold_b,
case 
when a.qtysold = b.qtysold then 'Perfect match' 
when a.qtysold is null and b.qtysold is null then 'both are null'
when a.salesid is null and b.salesid is not null then 'PK missing from a'  
when a.salesid is not null and b.salesid is null then 'PK missing from b' 
when a.qtysold is null then 'value is null in relation a only'
when b.qtysold is null then 'value is null in relation b only'
when a.qtysold <> B.qtysold then 'values are not a match'
end as match_status
from 
airbnb.raw.base_tickit_sales a
full outer join airbnb.raw.sales_deprecated b
on a.salesid = b.salesid;



with a as (

	select * from airbnb.raw.sales_deprecated
),

b as (

	select * from airbnb.raw.base_tickit_sales
	
),
a_intersect_b as (
	select * from a 
	intersect 
	select * from b 
),
a_except_b as (
	select * from a 
	except 
	select * from b 
),
b_except_a as (
	select * from b
	except 
	select * from a
),
all_records as (
select 
	* ,
	true as in_a,
	true as in_b
from 
	a_intersect_b
union all 
select 
	* ,
	true as in_a,
	false as in_b
from 
	a_except_b
union all 
select 
	* ,
	false as in_a,
	true as in_b
from 
	b_except_a
),

summary as (
select distinct
	in_a , 
	in_b ,
	count(*) over ( partition by in_a,in_b) as count
	,count(*) over () as total_count
FROM 
	all_records 
),
-- {% if summarize %}
final as (
select 
	in_a , 
	in_b ,
	count ,
    round( 100 * count/total_count ,9 ) as percent_of_total
--,round( 100 * count/sum(count) over () ,9 ) as percent_of_total
from 
summary
order by in_a , in_b
)

-- {% else %}

-- final as (
-- select 
-- * 
-- from all_records 
-- order by salesid, in_a desc, in_b desc
-- )

select * from final;

select date_trunc('wk',current_date());


With cte as (
	SELECT 'sabyasachi_nayak@gmail.com' AS email
	UNION ALL 
	SELECT 'sabya1991@spi.com'
)
SELECT *
FROM cte
WHERE REGEXP_INSTR(email , '[0-9#$%^&*()+=]+') > 0;


show schemas in database AIRBNB;

select 
*
 from AIRBNB.RAW.src_reviews LIMIT 1;

 select * from AIRBNB.RAW.dim_listing LIMIT 1;

 use role transform;
  select * from AIRBNB.RAW.dim_listing ;
  select * from tickit.raw.category;

 select
md5(
COALESCE(CAST( CATID as varchar) , '')|| '|' ||COALESCE(CAST( CATNAME as varchar) , '')|| '|' ||COALESCE(CAST( CATGROUP as varchar) , '')      
)
from 
 tickit.raw.category;


 select * from AIRBNB.person.address; --permanent table 
 select * from adventureworks.person.address; --transient table 
 select * from adventureworks.sales.salesorderheader limit 100;
 select * from adventureworks.sales.salesorderdetail LIMIT 10;

 select * from information_schema.tables;
 SHOW TABLES IN DATABASE adventureworks;





