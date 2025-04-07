with source as (
      select * from {{ source('tickit', 'listing') }}
),
renamed as (
        select * from source
)
select * from renamed
  