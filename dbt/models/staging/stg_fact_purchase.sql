select
  purchase_id,
  customer_sk,
  product_sk,
  customer_id,
  product_id,
  recommended_size,
  purchased_size,
  quantity,
  amount,
  purchase_date,
  record_hash,
  batch_id,
  created_at
from {{ source('curated', 'fact_purchase') }}
