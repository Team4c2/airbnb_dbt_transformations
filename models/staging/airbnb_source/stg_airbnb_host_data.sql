with source as (
 
    select * from {{source('airbnb_source_data','host') }}
 
), 

host_with_row_num as (
    select *,
    row_number() over (
        partition by id
        order by row_created_on desc
    ) as row_number 
    from source
),
deduplicated as (

    select 
        id,
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
    from   host_with_row_num
    where  row_number = 1
)

select * from deduplicated