select
  return_id,
  purchase_id,
  customer_sk,
  product_sk,
  customer_id,
  product_id,
  return_reason,
  fit_feedback,
  return_amount,
  return_date,
  {{ fit_return_flag('return_reason', 'fit_feedback') }} as is_fit_related_return,
  record_hash,
  batch_id,
  created_at
from {{ source('curated', 'fact_return') }}
