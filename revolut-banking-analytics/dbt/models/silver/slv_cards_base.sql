{{ config(materialized='table') }}
select * except(rn) from (
  select safe_cast(CardID as int64) card_id,safe_cast(AccountID as int64) account_id,upper(trim(CardType)) card_type,upper(trim(Network)) network,safe_cast(IssueDate as date) issue_date,safe_cast(ExpiryDate as date) expiry_date,upper(trim(Status)) status,upper(trim(ContactlessEnabled))='TRUE' contactless_enabled,batch_id,business_date,source_file,ingestion_ts,row_number() over(partition by CardID order by business_date desc, ingestion_ts desc, source_row_number desc) rn
  from {{ source('bronze','CARDS_bronze') }}
) where rn=1
