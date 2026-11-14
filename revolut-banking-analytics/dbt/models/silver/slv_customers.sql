{{ config(materialized='table') }}
with typed as (
  select safe_cast(CustomerID as int64) customer_id,trim(FirstName) first_name,trim(LastName) last_name,nullif(lower(trim(Email)),'') email,nullif(trim(Phone),'') phone,safe_cast(DateOfBirth as date) date_of_birth,trim(City) city,trim(Country) country,upper(trim(RiskRating)) risk_rating,safe_cast(UpdatedAt as timestamp) updated_at,batch_id,business_date,source_file,source_row_number,ingestion_ts,count(*) over(partition by batch_id, CustomerID) as rows_in_batch
  from {{ source('bronze','CUSTOMERS_bronze') }}
), ranked as (
  select *, row_number() over(partition by customer_id order by updated_at desc, ingestion_ts desc, source_row_number desc) rn
  from typed
  where customer_id is not null and email is not null and phone is not null
)
select * except(rn, rows_in_batch) from ranked where rn=1
