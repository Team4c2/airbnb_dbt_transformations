with property_dim_source as (
    select * from {{ ref('property_dim') }}
),

calendar_date_dim_source as (
    select * from {{ ref('calendar_date_dim') }}
),

calendar_staging_source as (
     select * from {{ ref('intermid_calendar_data') }}
),

combination as (
 select     pt_dim.propert_key, 
            cal_dim.CALENDAR_DATE_KEY,
            current_date() as "ROW_CREATED_ON",
            st_cal.AVAILABLE,
            st_cal.PRICE, 
            st_cal.ADJUST_PRICE,
            st_cal.MIN_NIGHTS,
            st_cal.MAX_NIGHTS
    from property_dim_source as pt_dim 
    inner join  calendar_staging_source as st_cal 
    on pt_dim.id = st_cal.PROPERTY_ID
    inner join calendar_date_dim_source as cal_dim 
    on st_cal.CALENDAR_DATE = cal_dim.FORMATTED_DATE
    qualify row_number() over (
                partition by pt_dim.id, cal_dim.FORMATTED_DATE
                order by pt_dim.row_created_on, cal_dim.ROW_CREATED_ON
               )  = 1
)

select * from combination