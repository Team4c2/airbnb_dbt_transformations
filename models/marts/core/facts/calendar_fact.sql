{% set natural_key = dbt_utils.surrogate_key('PROPERTY_KEY', 'CALENDAR_DATE_KEY') %}
{{
    config(
        materialized='incremental',
        unique_key=natural_key 
    )
}}
with property_dim_source as (
    select * from {{ ref('property_dim') }}
),

calendar_date_dim_source as (
    select * from {{ ref('calendar_date_dim') }}
),

calendar_staging_source as (
     select * from {{ ref('intermid_calendar_data') }}
     {% if is_incremental() %}

  -- this filter will only be applied on an incremental run
     where row_changed_on >= (select max(row_created_on) from {{ this }})

     {% endif %}
),

combination as (
 select     pt_dim.property_key, 
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
)

select * from combination