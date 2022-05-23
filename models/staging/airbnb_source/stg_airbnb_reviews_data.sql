with source as (
 
    select * from {{source('airbnb_source_data','reviews') }}
 
),

columns_reorganized as (
    select user_id,
           property_id,
           REVIEW_DATE,
           ROW_CHANGED_ON,
           REVIEWER_NAME,
           COMMENTS
    from source
)
select * from columns_reorganized