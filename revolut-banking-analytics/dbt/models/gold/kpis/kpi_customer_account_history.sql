{{ config(materialized='view') }}
select
  c.customer_id,c.customer_sk,c.city,c.risk_rating,
  c.effective_from customer_effective_from,
  c.effective_to customer_effective_to,
  c.is_current customer_is_current,
  a.account_id,a.account_sk,a.account_type_id,a.branch_id,a.status,a.balance,
  a.effective_from account_effective_from,
  a.effective_to account_effective_to,
  a.is_current account_is_current,
  greatest(c.effective_from,a.effective_from) history_effective_from,
  least(c.effective_to,a.effective_to) history_effective_to
from {{ ref('dim_customer') }} c
left join {{ ref('dim_account') }} a
  on a.customer_id=c.customer_id
 and greatest(c.effective_from,a.effective_from)
     < least(c.effective_to,a.effective_to)
