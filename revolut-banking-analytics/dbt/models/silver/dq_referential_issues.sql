{{ config(materialized='table') }}
select 'EMPLOYEES' entity_name,cast(e.employee_id as string) business_key,
  'referential_integrity' rule_name,'ERROR' severity,
  'Employee references a branch that is not present in trusted Silver' failure_reason,
  e.batch_id,e.source_file,e.ingestion_ts detected_at
from {{ ref('slv_employees_base') }} e
left join {{ ref('slv_branches') }} b using(branch_id)
where b.branch_id is null
union all
select 'CARDS',cast(c.card_id as string),'referential_integrity','ERROR',
  'Card references an account that is not present in trusted Silver',
  c.batch_id,c.source_file,c.ingestion_ts
from {{ ref('slv_cards_base') }} c
left join {{ ref('slv_accounts') }} a using(account_id)
where a.account_id is null
union all
select 'LOANS',cast(l.loan_id as string),'referential_integrity','ERROR',
  'Loan references a missing customer or branch',
  l.batch_id,l.source_file,l.ingestion_ts
from {{ ref('slv_loans_base') }} l
left join {{ ref('slv_customers') }} c using(customer_id)
left join {{ ref('slv_branches') }} b using(branch_id)
where c.customer_id is null or b.branch_id is null
union all
select 'LOANPAYMENTS',cast(p.payment_id as string),'referential_integrity','ERROR',
  'Loan payment references a loan that is not present in trusted Silver',
  p.batch_id,p.source_file,p.ingestion_ts
from {{ ref('slv_loan_payments_base') }} p
left join {{ ref('slv_loans') }} l using(loan_id)
where l.loan_id is null
