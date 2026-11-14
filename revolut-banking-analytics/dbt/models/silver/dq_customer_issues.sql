{{ config(materialized='table') }}
with b as (
  select *,
    count(*) over(partition by batch_id,CustomerID) cnt
  from {{ source('bronze','CUSTOMERS_bronze') }}
)
select 'CUSTOMERS' entity_name,coalesce(CustomerID,'<null>') business_key,
  'duplicate_customer_identity' rule_name,'ERROR' severity,
  'Duplicate CustomerID within the same batch' failure_reason,
  batch_id,source_file,ingestion_ts detected_at
from b where cnt>1
union all
select 'CUSTOMERS',coalesce(CustomerID,'<null>'),
  'missing_critical_customer_fields','ERROR',
  concat(
    'Missing/invalid:',
    if(CustomerID is null or trim(CustomerID)='',' CustomerID',''),
    if(FirstName is null or trim(FirstName)='',' FirstName',''),
    if(LastName is null or trim(LastName)='',' LastName',''),
    if(Email is null or trim(Email)='',' Email',''),
    if(Phone is null or trim(Phone)='',' Phone',''),
    if(UpdatedAt is null or trim(UpdatedAt)='',' UpdatedAt','')
  ),
  batch_id,source_file,ingestion_ts
from {{ source('bronze','CUSTOMERS_bronze') }}
where CustomerID is null or trim(CustomerID)=''
   or FirstName is null or trim(FirstName)=''
   or LastName is null or trim(LastName)=''
   or Email is null or trim(Email)=''
   or Phone is null or trim(Phone)=''
   or UpdatedAt is null or trim(UpdatedAt)=''
