{{ config(materialized='table') }}
select a.*
from {{ ref('slv_accounts_base') }} a
join {{ ref('slv_customers') }} c using(customer_id)
join {{ ref('slv_account_types') }} t using(account_type_id)
join {{ ref('slv_branches') }} b using(branch_id)
