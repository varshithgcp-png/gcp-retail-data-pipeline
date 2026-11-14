{{ config(materialized='table') }}
select * except(rn) from (
  select safe_cast(PaymentID as int64) payment_id,safe_cast(LoanID as int64) loan_id,safe_cast(PaymentDate as date) payment_date,safe_cast(DueDate as date) due_date,safe_cast(Amount as numeric) amount,upper(trim(PaymentStatus)) payment_status,batch_id,business_date,source_file,ingestion_ts,row_number() over(partition by PaymentID order by business_date desc, ingestion_ts desc, source_row_number desc) rn
  from {{ source('bronze','LOANPAYMENTS_bronze') }}
) where rn=1
