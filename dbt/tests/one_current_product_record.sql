select
  product_id
from {{ ref('stg_dim_product') }}
where is_current = true
group by product_id
having count(*) > 1
