with source as (
 
    select * from {{source('airbnb_source_data','user') }}
 
),
columns_reorganized as (
    select user_id,
           ROW_CHANGED_ON, 
	       F_NAME as FIRST_NAME,
	       L_NAME as LAST_NAME,
	       PHONE as PHONE_NO,
	       EMAIL as EMAIL_ID, 
	       DOB as DATE_OF_BIRTH
    from source
    qualify row_number() over (
              partition by user_id
              order by ROW_CHANGED_ON desc
            ) = 1
)
select * from columns_reorganized