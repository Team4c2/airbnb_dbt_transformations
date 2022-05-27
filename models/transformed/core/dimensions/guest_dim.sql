{{
    config(
        materialized='incremental',
        unique_key='USER_ID'
    )
}}
with source as (
 
    select * from {{ ref('stg_airbnb_guest_data') }}
    {% if is_incremental() %}

  -- this filter will only be applied on an incremental run
     where row_changed_on > (select max(row_created_on) from {{ this }})

     {% endif %}
 
),

dimension as (
    select   {{ dbt_utils.surrogate_key(
                  'USER_ID'
             ) }} as GUEST_KEY,  
            user_id,
            current_date() as "ROW_CREATED_ON",
            FIRST_NAME,
            LAST_NAME,
            PHONE_NO,
            EMAIL_ID, 
            DATE_OF_BIRTH
    from source
)

select * from dimension