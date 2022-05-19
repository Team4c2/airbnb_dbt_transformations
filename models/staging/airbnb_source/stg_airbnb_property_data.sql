with source as (
 
    select * from {{source('airbnb_source_data','property') }}
 
), 

property_with_row_num as(
    select *,
    row_number() over (
        partition by id
        order by row_created_on desc
    ) as row_number 
    from source
),

deduplicated as (
    select id, 
            name,
            description, 
            neighborhood_overview,
            host_id,
            neighbourhood, 
            neighbourhood_cleansed, 
            longitude,
            property_type, 
            room_type,
            accommodates, 
            bathrooms,
            bathrooms_text,
            bedrooms,
            beds,
            amenities, 
            price,
            minimum_nights, 
            maximum_nights,
            number_of_reviews, 
            review_scores_rating, 
            review_scores_accuracy, 
            review_scores_cleanliness, 
            review_scores_checkin,
            review_scores_communication, 
            review_scores_location,
            is_instant_bookable
    from property_with_row_num
    where row_number = 1
)

select * from deduplicated