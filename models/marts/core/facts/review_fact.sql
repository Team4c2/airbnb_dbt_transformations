{% set natural_key = dbt_utils.surrogate_key('GUEST_KEY', 'PROPERTY_KEY', 'REVIEW_DATE_KEY') %}
{{
    config(
        materialized='incremental',
        unique_key=natural_key 
    )
}}
with guest_dim_source as (
    select * from {{ ref('guest_dim') }}
),

review_date_dim_source as (
   select * from {{ ref('review_date_dim') }}
),

review_intermid_source as (
    select * from {{ ref('intermid_reviews_data') }}
    {% if is_incremental() %}

     -- this filter will only be applied on an incremental run
     where row_changed_on >= (select max(row_created_on) from {{ this }})

     {% endif %}
),

property_dim_source as (
    select * from {{ ref('property_dim') }}
),

combined as (
    select gu_dim.GUEST_KEY,
           pt_dim.property_key,
           rvdat_dim.REVIEW_DATE_KEY,
           current_date() as "ROW_CREATED_ON",
           rv_int.COMMENTS
    from guest_dim_source as gu_dim 
    inner join review_intermid_source as rv_int 
    on gu_dim.user_id = rv_int.user_id 
    inner join property_dim_source as pt_dim 
    on rv_int.property_id = pt_dim.id 
    inner join review_date_dim_source as rvdat_dim 
    on rv_int.REVIEW_DATE = rvdat_dim.FORMATTED_DATE
)

select * from combined