with source as (
 
    select * from {{source('airbnb_source_data','reviews') }} order by user_id,property_id,REVIEW_DATE
 
),

columns_reorganized as (
    select user_id,
           property_id,
           REVIEW_DATE,
           ROW_CHANGED_ON,
           REVIEWER_NAME,
           COMMENTS
    from source
    qualify row_number() over (
              partition by user_id,property_id,REVIEW_DATE 
              order by ROW_CHANGED_ON desc
            ) = 1
)
select * from columns_reorganized