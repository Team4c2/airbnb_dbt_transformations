with source as (
 
    select * from {{ ref('stg_airbnb_calendar_data') }}
 
),

distinct_dates as (
    select distinct CALENDAR_DATE
    from source
),

dimension as (
    select   {{ dbt_utils.surrogate_key(
                  'CALENDAR_DATE',
             ) }} as CALENDAR_DATE_KEY,    
            day(CALENDAR_DATE) as "DAY",
            month(CALENDAR_DATE) as "MONTH", 
            year(CALENDAR_DATE) as "YEAR"
    from distinct_dates
)

select * from dimension