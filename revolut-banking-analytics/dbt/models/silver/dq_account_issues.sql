{{ config(materialized='table') }}
select 'ACCOUNTS' entity_name, cast(a.account_id as string) business_key,
  'branch_sort_code_integrity' rule_name, 'WARN' severity,
  concat('Sort code ',a.sort_code,' does not start with branch prefix ',coalesce(b.sort_code_prefix,'<missing branch>')) failure_reason,
  a.batch_id,a.source_file,a.ingestion_ts detected_at
from {{ ref('slv_accounts_base') }} a
left join {{ ref('slv_branches') }} b using(branch_id)
where b.branch_id is null or not starts_with(a.sort_code,b.sort_code_prefix)
union all
select 'ACCOUNTS', cast(a.account_id as string), 'negative_balance_beyond_overdraft','WARN',
  concat('Balance=',cast(a.balance as string),', overdraft_limit=',cast(t.overdraft_limit as string)),
  a.batch_id,a.source_file,a.ingestion_ts
from {{ ref('slv_accounts_base') }} a
left join {{ ref('slv_account_types') }} t using(account_type_id)
where a.balance < -coalesce(t.overdraft_limit,0)
union all
select 'ACCOUNTS', cast(a.account_id as string), 'referential_integrity','ERROR',
  'Missing customer/account type/branch reference', a.batch_id,a.source_file,a.ingestion_ts
from {{ ref('slv_accounts_base') }} a
left join {{ ref('slv_customers') }} c using(customer_id)
left join {{ ref('slv_account_types') }} t using(account_type_id)
left join {{ ref('slv_branches') }} b using(branch_id)
where c.customer_id is null or t.account_type_id is null or b.branch_id is null
