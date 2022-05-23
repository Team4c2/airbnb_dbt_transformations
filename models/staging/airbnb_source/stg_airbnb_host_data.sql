with source as (
 
    select * from {{source('airbnb_source_data','host') }}
 
), 
deduplicated as (

    select 
        id,
        row_changed_on,
        name,
        since,
        case 
          when location = '' then NULL
          else location
        end as "location",
        about,
        response_time,
        verifications,
        has_profile_pic,
        is_identity_verified,
        response_rate,
        acceptance_rate,
        case 
           when neighbourhood = '' then NULL
           else neighbourhood 
        end as "neighbourhood",
        listings_count,
        host_is_superhost
    from   source
)

select * from deduplicated