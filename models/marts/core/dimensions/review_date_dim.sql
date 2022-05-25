{{
    config(
        materialized='incremental',
        unique_key='REVIEW_DATE'
    )
}}
with source as (
 
    select * from {{ ref('stg_airbnb_reviews_data') }}
    {% if is_incremental() %}

  -- this filter will only be applied on an incremental run
     where row_changed_on >= (select max(row_created_on) from {{ this }})

     {% endif %}
 
),

distinct_dates as (
    select distinct REVIEW_DATE
    from source
),

dimension as (
    select   {{ dbt_utils.surrogate_key(
                  'REVIEW_DATE'
             ) }} as REVIEW_DATE_KEY,  
            current_date() as "ROW_CREATED_ON",
            REVIEW_DATE as "FORMATTED_DATE",
            day(REVIEW_DATE) as "DAY",
            month(REVIEW_DATE) as "MONTH", 
            year(REVIEW_DATE) as "YEAR"
    from distinct_dates
)

select * from dimension