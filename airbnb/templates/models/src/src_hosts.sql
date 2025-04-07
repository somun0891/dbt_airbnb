with raw_hosts as (
    select * from raw.raw_hosts
)

select r.* 
REPLACE('Host-'|| Id AS Id) 
RENAME (ID AS HOST_ID ,NAME AS HOST_NAME) 
from raw_hosts r

