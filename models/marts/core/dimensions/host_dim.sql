with source as (
 
    select * from {{ ref('stg_airbnb_host_data') }}
 
),

dimension as (
    select   {{ dbt_utils.surrogate_key(
                  'id',
             )}} as host_key,  
                id,
                current_date() as "ROW_CREATED_ON",
                name,
                since,
                location,
                about,
                response_time,
                verifications,
                has_profile_pic,
                is_identity_verified,
                response_rate,
                acceptance_rate,
                neighbourhood,
                listings_count,
                host_is_superhost
    from source
)
select * from dimension