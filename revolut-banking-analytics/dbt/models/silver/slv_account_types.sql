{{ config(materialized='table') }}
select * except(rn) from (
  select safe_cast(AccountTypeID as int64) account_type_id, trim(AccountType) account_type, trim(ProductCategory) product_category, safe_cast(MonthlyFee as numeric) monthly_fee, safe_cast(InterestRate as numeric) interest_rate, safe_cast(OverdraftLimit as numeric) overdraft_limit, batch_id,business_date,source_file,ingestion_ts, row_number() over(partition by AccountTypeID order by business_date desc, ingestion_ts desc, source_row_number desc) rn
  from {{ source('bronze','ACCOUNTTYPES_bronze') }}
) where rn=1
