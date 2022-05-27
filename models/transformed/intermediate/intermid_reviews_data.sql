with source as (
    select * from {{ ref('stg_airbnb_reviews_data') }}
)

select * from source