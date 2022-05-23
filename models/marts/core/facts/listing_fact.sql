with property_dim_source as (
    select * from {{ ref('property_dim') }}
),

host_dim_source as (
    select * from {{ ref('host_dim') }}
),

property_staging_source as (
     select * from {{ ref('stg_airbnb_property_data') }}
),

combination as (
    select  pt_dim.property_key, 
            ho_dim.host_key,
            pt_st.name,
            pt_st.description, 
            pt_st.neighborhood_overview,
            pt_st.accommodates, 
            pt_st.amenities, 
            pt_st.price,
            pt_st.minimum_nights, 
            pt_st.maximum_nights,
            pt_st.number_of_reviews, 
            pt_st.review_scores_rating, 
            pt_st.review_scores_accuracy, 
            pt_st.review_scores_cleanliness, 
            pt_st.review_scores_checkin,
            pt_st.review_scores_communication, 
            pt_st.review_scores_location,
            pt_st.is_instant_bookable
    from property_dim_source as pt_dim 
    inner join property_staging_source as pt_st 
    on  pt_dim.id = pt_st.id
    inner join host_dim_source as ho_dim 
    on pt_st.host_id = ho_dim.id
    qualify row_number() over (
                partition by pt_dim.id, ho_dim.id
                order by pt_dim.row_created_on, ho_dim.row_created_on
               )  = 1
)

select * from combination