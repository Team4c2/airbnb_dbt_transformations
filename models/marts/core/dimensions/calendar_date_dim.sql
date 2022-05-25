{{
    config(
        materialized='incremental',
        unique_key='CALENDAR_DATE'
    )
}}

with source as (
 
    select * from {{ ref('stg_airbnb_calendar_data') }}
    {% if is_incremental() %}

  -- this filter will only be applied on an incremental run
    where row_changed_on >= (select max(row_created_on) from {{ this }})

     {% endif %}
),

distinct_dates as (
    select distinct CALENDAR_DATE
    from source
),

dimension as (
    select   {{ dbt_utils.surrogate_key(
                  'CALENDAR_DATE'
             ) }} as CALENDAR_DATE_KEY,
            current_date() as "ROW_CREATED_ON",
            CALENDAR_DATE as "FORMATTED_DATE",    
            day(CALENDAR_DATE) as "DAY",
            month(CALENDAR_DATE) as "MONTH", 
            year(CALENDAR_DATE) as "YEAR"
    from distinct_dates
)

select * from dimension