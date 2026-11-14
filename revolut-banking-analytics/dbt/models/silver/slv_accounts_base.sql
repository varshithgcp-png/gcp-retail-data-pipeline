{{ config(materialized='table') }}
with typed as (
  select
    safe_cast(AccountID as int64) account_id,
    safe_cast(CustomerID as int64) customer_id,
    safe_cast(AccountTypeID as int64) account_type_id,
    safe_cast(BranchID as int64) branch_id,
    trim(SortCode) sort_code,
    trim(AccountNumber) account_number,
    upper(trim(Currency)) currency,
    safe_cast(Balance as numeric) balance,
    safe_cast(OpenDate as date) open_date,
    safe_cast(nullif(CloseDate,'') as date) close_date,
    upper(trim(Status)) status,
    safe_cast(UpdatedAt as timestamp) updated_at,
    load_type,
    batch_id,business_date,source_file,source_row_number,ingestion_ts
  from {{ source('bronze','ACCOUNTS_bronze') }}
),
ranked as (
  select *,
    row_number() over(
      partition by account_id
      order by updated_at desc, ingestion_ts desc, source_row_number desc
    ) rn
  from typed
  where account_id is not null
    and customer_id is not null
    and account_type_id is not null
    and branch_id is not null
    and updated_at is not null
)
select * except(rn)
from ranked
where rn=1
