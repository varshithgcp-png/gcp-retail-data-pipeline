{{ config(materialized='table', partition_by={'field':'transaction_date','data_type':'date'}, cluster_by=['account_id','transaction_type_id']) }}
with raw_typed as (
  select
    safe_cast(TransactionID as int64) transaction_id,
    safe_cast(AccountID as int64) account_id,
    safe_cast(TransactionTypeID as int64) transaction_type_id,
    safe_cast(TransactionTs as timestamp) transaction_ts,
    date(safe_cast(TransactionTs as timestamp)) transaction_date,
    safe_cast(Amount as numeric) amount,
    upper(trim(Currency)) currency,
    upper(trim(Channel)) channel,
    upper(trim(MerchantCategory)) merchant_category,
    trim(Reference) reference,
    batch_id,business_date,source_file,source_row_number,ingestion_ts,
    count(*) over(partition by TransactionID) business_key_count,
    count(*) over(
      partition by TransactionID,AccountID,TransactionTypeID,TransactionTs,
                   Amount,Currency,Channel,MerchantCategory,Reference
    ) exact_duplicate_count,
    row_number() over(
      partition by TransactionID
      order by ingestion_ts desc,source_row_number desc
    ) rn
  from {{ source('bronze','TRANSACTIONS_bronze') }}
),
deduped as (
  select * except(rn)
  from raw_typed
  where rn=1
),
enriched as (
  select
    t.*,
    a.customer_id,a.account_type_id,a.branch_id,
    a.currency account_currency,a.open_date,a.close_date,a.status account_status,
    tt.category transaction_category,tt.direction,tt.is_cash_proxy,
    a.account_id is not null has_account_reference,
    tt.transaction_type is not null has_transaction_type_reference,
    t.currency != a.currency is_currency_mismatch,
    t.transaction_date < a.open_date
      or (a.close_date is not null and t.transaction_date > a.close_date)
      as is_invalid_transaction_date,
    abs(t.amount) > {{ var('suspicious_transaction_threshold',100000) }}
      as is_high_value
  from deduped t
  left join {{ ref('slv_account_versions') }} a
    on a.account_id=t.account_id
   and t.transaction_ts >= a.effective_from
   and t.transaction_ts < a.effective_to
  left join {{ ref('slv_transaction_types') }} tt using(transaction_type_id)
),
windowed as (
  select
    e.*,
    countif(
      is_cash_proxy
      and direction='CREDIT'
      and transaction_category='DEPOSIT'
      and channel in ('BRANCH','ATM')
      and abs(amount) between {{ var('structuring_lower_bound',9000) }}
                          and {{ var('structuring_upper_bound',10000) }}
    ) over(
      partition by account_id
      order by unix_seconds(transaction_ts)
      range between {{ var('structuring_window_seconds',3600) }} preceding and current row
    ) near_threshold_deposit_count,
    count(*) over(
      partition by account_id
      order by unix_seconds(transaction_ts)
      range between {{ var('structuring_window_seconds',3600) }} preceding and current row
    ) activity_burst_count,
    max(transaction_ts) over(
      partition by account_id
      order by unix_seconds(transaction_ts)
      range between unbounded preceding
        and {{ var('structuring_window_seconds',3600) + 1 }} preceding
    ) prior_activity_before_window
  from enriched e
),
flagged as (
  select
    *,
    near_threshold_deposit_count >= {{ var('structuring_min_count',3) }}
      as is_structuring_candidate,
    account_status='DORMANT'
      and activity_burst_count >= 3
      and (
        prior_activity_before_window is null
        or timestamp_diff(transaction_ts,prior_activity_before_window,day) >= 90
      ) as is_dormant_reactivation_burst
  from windowed
)
select
  *,
  not coalesce(is_invalid_transaction_date,true)
    and has_account_reference
    and has_transaction_type_reference
    and amount is not null
    and transaction_ts is not null
    as is_valid_record
from flagged
