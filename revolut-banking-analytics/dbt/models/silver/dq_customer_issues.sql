{{ config(materialized='table') }}
with b as (
  select *, count(*) over(partition by batch_id, CustomerID) cnt
  from {{ source('bronze','CUSTOMERS_bronze') }}
)
select 'CUSTOMERS' entity_name, CustomerID business_key, 'duplicate_customer_identity' rule_name,
  'ERROR' severity, 'Duplicate CustomerID within the same batch' failure_reason,
  batch_id, source_file, ingestion_ts detected_at
from b where cnt > 1
union all
select 'CUSTOMERS', CustomerID, 'missing_critical_customer_fields', 'ERROR',
  concat('Missing/invalid: ', if(trim(Email)='','Email ',''), if(trim(Phone)='','Phone','')),
  batch_id, source_file, ingestion_ts
from {{ source('bronze','CUSTOMERS_bronze') }}
where trim(Email)='' or trim(Phone)=''
