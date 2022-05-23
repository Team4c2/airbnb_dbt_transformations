with property_dim_source as (
    select * from {{ ref('property_dim') }}
),

calendar_date_dim_source as (
    select * from {{ ref('calendar_date_dim') }}
),

calendar_staging_source as (
     select * from {{ ref('stg_airbnb_calendar_data') }}
),