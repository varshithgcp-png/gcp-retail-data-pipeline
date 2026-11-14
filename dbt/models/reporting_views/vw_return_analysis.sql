select
  r.return_date,
  r.return_id,
  r.purchase_id,
  r.customer_id,
  c.region,
  r.product_id,
  p.brand,
  p.category,
  p.fit_profile,
  r.return_reason,
  r.fit_feedback,
  r.is_fit_related_return,
  r.return_amount
from {{ ref('stg_fact_return') }} r
join {{ ref('stg_dim_customer') }} c
  on r.customer_sk = c.customer_sk
join {{ ref('stg_dim_product') }} p
  on r.product_sk = p.product_sk
