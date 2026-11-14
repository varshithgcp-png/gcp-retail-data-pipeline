select
  customer_id
from {{ ref('stg_dim_customer') }}
where is_current = true
group by customer_id
having count(*) > 1
