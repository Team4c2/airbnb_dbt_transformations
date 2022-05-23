with source as (
 
    select * from {{source('airbnb_source_data','calendar') }}
 
),

transformed as(
    select  PROPERTY_ID, 
            DATE as "CALENDAR_DATE", 
            ROW_CHANGED_ON,
            case 
              when AVAILABLE = 't' then True
              when AVAILABLE = 'f' then False
              else AVAILABLE
            end as "AVAILABLE",
            PRICE, 
            ADJUST_PRICE,
            MIN_NIGHTS,
            MAX_NIGHTS,
            row_number() over (
              partition by PROPERTY_ID, DATE
              order by ROW_CHANGED_ON desc
            ) as row_number
    from source 
    
),

deduplicated as (
    select * from transformed where row_number = 1
)

select * from deduplicated