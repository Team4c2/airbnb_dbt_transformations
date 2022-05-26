with source as (
 
    select * from {{source('airbnb_source_data','property')}} order by id 
 
), 

deduplicated as (
    select  id, 
            row_changed_on,
            name,
            description, 
            neighborhood_overview,
            host_id,
            case 
              when neighbourhood  = '' then NULL
              else neighbourhood
            end as "LOCATION",
            case 
               when neighbourhood_cleansed = '' then NULL 
               else neighbourhood_cleansed 
            end as "NEIGHBOURHOOD", 
            latitude,
            longitude,
            property_type, 
            room_type,
            accommodates, 
            case 
              when bathrooms is null and bathrooms_text is not null and regexp_like(bathrooms_text, '\\d.*')
                 then {{ dbt_utils.split_part(string_text='bathrooms_text', delimiter_text="' '", part_number=1) }}::integer
              else bathrooms
            end as bathrooms,
            bedrooms,
            beds,
            amenities, 
            case 
              when price is not null and regexp_like(price, '\\$.*')
                 then REPLACE(REPLACE(price, '$'), ',')::float 
              else NULL 
            end as price,
            minimum_nights, 
            maximum_nights,
            number_of_reviews, 
            review_scores_rating::float as review_scores_rating,
            review_scores_accuracy::float as review_scores_accuracy,
            review_scores_cleanliness::float as review_scores_cleanliness,
            review_scores_checkin::float as review_scores_checkin,
            review_scores_communication::float as review_scores_communication,
            review_scores_location::float as review_scores_location,
            is_instant_bookable,
            city, 
            state,
            country
    from source
    qualify row_number() over (
              partition by id
              order by ROW_CHANGED_ON desc
            ) = 1
    
)

select * from deduplicated