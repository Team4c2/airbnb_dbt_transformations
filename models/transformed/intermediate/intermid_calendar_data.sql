with source as (
 
    select * from {{ ref('stg_airbnb_calendar_data') }}
 
)

select * from source
