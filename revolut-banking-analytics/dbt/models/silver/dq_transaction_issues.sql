{{ config(materialized='table') }}
select 'TRANSACTIONS' entity_name,cast(transaction_id as string) business_key,
  'exact_duplicate_transaction' rule_name,'ERROR' severity,
  concat('Exact transaction row appears ',cast(exact_duplicate_count as string),' times; one row retained') failure_reason,
  batch_id,source_file,ingestion_ts detected_at
from {{ ref('slv_transactions') }}
where exact_duplicate_count > 1
union all
select 'TRANSACTIONS',cast(transaction_id as string),'currency_mismatch','WARN',
  concat('transaction=',currency,', account=',coalesce(account_currency,'<missing>')),
  batch_id,source_file,ingestion_ts
from {{ ref('slv_transactions') }} where is_currency_mismatch
union all
select 'TRANSACTIONS',cast(transaction_id as string),'invalid_transaction_date','ERROR',
  'Transaction date is outside account open/close dates',
  batch_id,source_file,ingestion_ts
from {{ ref('slv_transactions') }} where is_invalid_transaction_date
union all
select 'TRANSACTIONS',cast(transaction_id as string),'structuring_candidate','WARN',
  '3+ near-threshold cash-proxy deposits via branch/ATM within one hour',
  batch_id,source_file,ingestion_ts
from {{ ref('slv_transactions') }} where is_structuring_candidate
union all
select 'TRANSACTIONS',cast(transaction_id as string),'dormant_reactivation_burst','WARN',
  'Dormant account has 90+ days/no prior observed activity followed by a 3+ transaction one-hour burst',
  batch_id,source_file,ingestion_ts
from {{ ref('slv_transactions') }} where is_dormant_reactivation_burst
union all
select 'TRANSACTIONS',cast(transaction_id as string),'referential_integrity','ERROR',
  'Missing account version or transaction-type reference',
  batch_id,source_file,ingestion_ts
from {{ ref('slv_transactions') }}
where not has_account_reference or not has_transaction_type_reference
