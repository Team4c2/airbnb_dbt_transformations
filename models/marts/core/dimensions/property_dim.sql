with source as (
 
    select * from {{ ref('stg_airbnb_property_data') }}
 
),

dimension as (
    select   {{ dbt_utils.surrogate_key(
                  'id',
                  'row_changed_on'
             ) }} as property_key,  
            id, 
            current_date() as row_created_on,
            LOCATION,
            NEIGHBOURHOOD, 
            longitude,
            property_type, 
            room_type, 
            bathrooms,
            bedrooms,
            beds,
            city,
            state,
            country
    from source
)
select * from dimension