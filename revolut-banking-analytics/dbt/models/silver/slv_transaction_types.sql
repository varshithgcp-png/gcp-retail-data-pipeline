{{ config(materialized='table') }}
select * except(rn) from (
  select safe_cast(TransactionTypeID as int64) transaction_type_id,trim(TransactionType) transaction_type,trim(Category) category,upper(trim(Direction)) direction,upper(trim(IsCashProxy))='TRUE' is_cash_proxy,batch_id,business_date,source_file,ingestion_ts,row_number() over(partition by TransactionTypeID order by business_date desc, ingestion_ts desc, source_row_number desc) rn
  from {{ source('bronze','TRANSACTIONTYPES_bronze') }}
) where rn=1
