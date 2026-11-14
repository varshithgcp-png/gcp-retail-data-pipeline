{{ config(materialized='table', cluster_by=['account_id','effective_from']) }}
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
  where safe_cast(AccountID as int64) is not null
    and safe_cast(UpdatedAt as timestamp) is not null
),
trusted as (
  select t.*
  from typed t
  join {{ ref('slv_customers') }} c using(customer_id)
  join {{ ref('slv_account_types') }} at using(account_type_id)
  join {{ ref('slv_branches') }} b using(branch_id)
),
deduped as (
  select * except(rn)
  from (
    select *,
      row_number() over(
        partition by account_id,updated_at
        order by ingestion_ts desc,source_row_number desc
      ) rn
    from trusted
  )
  where rn=1
),
versioned as (
  select *,
    row_number() over(partition by account_id order by updated_at) version_number,
    lead(updated_at) over(partition by account_id order by updated_at) next_updated_at
  from deduped
)
select
  * except(version_number,next_updated_at),
  case
    when version_number=1 and load_type='FULL' then timestamp('1900-01-01')
    else updated_at
  end effective_from,
  coalesce(next_updated_at,timestamp('9999-12-31 23:59:59+00')) effective_to
from versioned
