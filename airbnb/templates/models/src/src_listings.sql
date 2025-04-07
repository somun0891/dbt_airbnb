with raw_listings as 
(
    select * from raw.raw_listings
)
select * 
REPLACE('Host-' || HOST_ID AS HOST_ID)
 RENAME (NAME AS listing_name, PRICE AS PRICE_STR )
 from raw_listings