{{
    config(
        materialized='incremental',
        unique_key='ID'
    )
}}
with source as (
 
    select * from {{ ref('stg_airbnb_property_data') }}
    {% if is_incremental() %}

  -- this filter will only be applied on an incremental run
     where row_changed_on > (select max(row_created_on) from {{ this }})

    {% endif %}
 
),

dimension as (
    select   {{ dbt_utils.surrogate_key(
                  'ID'
             ) }} as property_key,  
            id, 
            current_date() as row_created_on,
            LOCATION,
            NEIGHBOURHOOD, 
            latitude,
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