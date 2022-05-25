with source as (
    select * from {{ ref('stg_airbnb_property_data') }}
),

relevant as (
    select  id, 
            host_id,
            row_changed_on,
            name,
            description, 
            neighborhood_overview,
            accommodates, 
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
    from source
)

select * from relevant