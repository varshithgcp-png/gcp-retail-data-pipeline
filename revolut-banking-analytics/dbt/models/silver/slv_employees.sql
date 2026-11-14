{{ config(materialized='table') }}
select * except(rn) from (
  select safe_cast(EmployeeID as int64) employee_id,safe_cast(BranchID as int64) branch_id,trim(FirstName) first_name,trim(LastName) last_name,trim(Role) role,safe_cast(HireDate as date) hire_date,upper(trim(Status)) status,batch_id,business_date,source_file,ingestion_ts,row_number() over(partition by EmployeeID order by business_date desc, ingestion_ts desc, source_row_number desc) rn
  from {{ source('bronze','EMPLOYEES_bronze') }}
) where rn=1
