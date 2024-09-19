with cte_listing AS (
    select * from {{ ref("src_listings") }}
)
SELECT 
    ID,
    LISTING_URL,
    LISTING_NAME,
    ROOM_TYPE,
    CASE WHEN MINIMUM_NIGHTS = 0 THEN 1 ELSE MINIMUM_NIGHTS END AS MINIMUM_NIGHTS,
    HOST_ID,
    REPLACE(PRICE_STR,'$','')::NUMBER(10,2) AS PRICE,
    CREATED_AT , 
    UPDATED_AT
FROM cte_listing