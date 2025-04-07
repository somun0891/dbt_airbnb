with source as (
      select * from {{ source('tickit', 'users') }}
),
renamed as (
    select
        {{ adapter.quote("USERID") }},
        {{ adapter.quote("USERNAME") }},
        {{ adapter.quote("FIRSTNAME") }},
        {{ adapter.quote("LASTNAME") }},
        {{ adapter.quote("CITY") }},
        {{ adapter.quote("STATE") }},
        {{ adapter.quote("EMAIL") }},
        {{ adapter.quote("PHONE") }},
        {{ adapter.quote("LIKESPORTS") }},
        {{ adapter.quote("LIKETHEATRE") }},
        {{ adapter.quote("LIKECONCERTS") }},
        {{ adapter.quote("LIKEJAZZ") }},
        {{ adapter.quote("LIKECLASSICAL") }},
        {{ adapter.quote("LIKEOPERA") }},
        {{ adapter.quote("LIKEROCK") }},
        {{ adapter.quote("LIKEVEGAS") }},
        {{ adapter.quote("LIKEBROADWAY") }},
        {{ adapter.quote("LIKEMUSICALS") }},
        {{ adapter.quote("CHANGE_TIMESTAMP") }}

    from source
)
select * from renamed
  