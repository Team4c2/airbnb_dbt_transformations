with source as (
 
    select * from {{ ref('stg_airbnb_calendar_data') }}
),

distinct_dates as (
    select distinct CALENDAR_DATE, row_changed_on
    from source
),

dimension as (
    select   {{ dbt_utils.surrogate_key(
                  'CALENDAR_DATE',
                  'ROW_CHANGED_ON'
             ) }} as CALENDAR_DATE_KEY,
            current_date() as "ROW_CREATED_ON",
            CALENDAR_DATE as "FORMATTED_DATE",    
            day(CALENDAR_DATE) as "DAY",
            month(CALENDAR_DATE) as "MONTH", 
            year(CALENDAR_DATE) as "YEAR"
    from distinct_dates
    LIMIT 10
)

select * from dimension