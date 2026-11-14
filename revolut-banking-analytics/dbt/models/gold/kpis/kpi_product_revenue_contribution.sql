{{ config(materialized='table') }}
with x as (
  select at.account_type_id,at.account_type,at.product_category,sum(abs(f.amount)) transaction_value
  from {{ ref('fact_transaction') }} f join {{ ref('dim_account_type') }} at using(account_type_id)
  group by 1,2,3
)
select *, safe_divide(transaction_value,sum(transaction_value) over()) contribution_pct
from x
