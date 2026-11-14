{{ config(materialized='table') }}
select 'TRANSACTIONS' entity_name, cast(transaction_id as string) business_key,
  'exact_duplicate_transaction' rule_name, 'ERROR' severity,
  concat('TransactionID appears ',cast(duplicate_count as string),' times; one row retained') failure_reason,
  batch_id,source_file,ingestion_ts detected_at
from {{ ref('slv_transactions') }} where duplicate_count > 1
union all
select 'TRANSACTIONS',cast(transaction_id as string),'currency_mismatch','WARN',
  concat('transaction=',currency,', account=',coalesce(account_currency,'<missing>')),batch_id,source_file,ingestion_ts
from {{ ref('slv_transactions') }} where is_currency_mismatch
union all
select 'TRANSACTIONS',cast(transaction_id as string),'invalid_transaction_date','ERROR',
  'Transaction date is outside account open/close dates',batch_id,source_file,ingestion_ts
from {{ ref('slv_transactions') }} where is_invalid_transaction_date
union all
select 'TRANSACTIONS',cast(transaction_id as string),'structuring_candidate','WARN',
  '3+ cash-proxy near-threshold deposits within the configured rolling window',batch_id,source_file,ingestion_ts
from {{ ref('slv_transactions') }} where is_structuring_candidate
union all
select 'TRANSACTIONS',cast(transaction_id as string),'dormant_reactivation_burst','WARN',
  'Dormant account shows clustered transaction activity',batch_id,source_file,ingestion_ts
from {{ ref('slv_transactions') }} where is_dormant_reactivation_burst
union all
select 'TRANSACTIONS',cast(transaction_id as string),'referential_integrity','ERROR',
  'Missing account or transaction-type reference',batch_id,source_file,ingestion_ts
from {{ ref('slv_transactions') }} where not has_account_reference or not has_transaction_type_reference
