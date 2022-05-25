with source as (
 
    select * from {{ ref('stg_airbnb_reviews_data') }}
 
),

distinct_guests as (
    select distinct user_id,
                    row_changed_on,
                    REVIEWER_NAME
    from source
),

dimension as (
    select   {{ dbt_utils.surrogate_key(
                  'user_id',
                  'row_changed_on'
             ) }} as GUEST_KEY,  
            user_id,
            current_date() as "ROW_CREATED_ON",
            REVIEWER_NAME
    from distinct_guests
)

select * from dimension