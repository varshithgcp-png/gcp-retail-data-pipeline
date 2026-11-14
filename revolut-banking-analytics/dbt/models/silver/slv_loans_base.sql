{{ config(materialized='table') }}
select * except(rn) from (
  select safe_cast(LoanID as int64) loan_id,safe_cast(CustomerID as int64) customer_id,safe_cast(BranchID as int64) branch_id,upper(trim(LoanType)) loan_type,safe_cast(PrincipalAmount as numeric) principal_amount,safe_cast(InterestRate as numeric) interest_rate,safe_cast(StartDate as date) start_date,safe_cast(EndDate as date) end_date,upper(trim(Status)) status,batch_id,business_date,source_file,ingestion_ts,row_number() over(partition by LoanID order by business_date desc, ingestion_ts desc, source_row_number desc) rn
  from {{ source('bronze','LOANS_bronze') }}
) where rn=1
