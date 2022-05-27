{{
    config(
        materialized='incremental',
        unique_key='ID'
    )
}}
with source as (
 
    select * from {{ ref('stg_airbnb_host_data') }}
    {% if is_incremental() %}

  -- this filter will only be applied on an incremental run
     where row_changed_on > (select max(row_created_on) from {{ this }})

     {% endif %}
 
),

dimension as (
    select   {{ dbt_utils.surrogate_key(
                  'ID'
             )}} as host_key,  
                id,
                current_date() as "ROW_CREATED_ON",
                name,
                since,
                location,
                about,
                response_time,
                verifications,
                has_profile_pic,
                is_identity_verified,
                response_rate,
                acceptance_rate,
                neighbourhood,
                listings_count,
                host_is_superhost
    from source
)
select * from dimension