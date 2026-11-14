select p.purchase_id
from {{ ref('stg_fact_purchase') }} p
left join {{ ref('stg_dim_customer') }} c
  on p.customer_sk = c.customer_sk
left join {{ ref('stg_dim_product') }} pr
  on p.product_sk = pr.product_sk
where c.customer_sk is null
   or pr.product_sk is null
