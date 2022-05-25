with source as (
 
    select * from {{ ref('stg_airbnb_reviews_data') }}
 
),

distinct_dates as (
    select distinct REVIEW_DATE, row_changed_on
    from source
),

dimension as (
    select   {{ dbt_utils.surrogate_key(
                  'REVIEW_DATE',
                  'ROW_CHANGED_ON'
             ) }} as REVIEW_DATE_KEY,  
            current_date() as "ROW_CREATED_ON",
            REVIEW_DATE as "FORMATTED_DATE",
            day(REVIEW_DATE) as "DAY",
            month(REVIEW_DATE) as "MONTH", 
            year(REVIEW_DATE) as "YEAR"
    from distinct_dates
)

select * from dimension