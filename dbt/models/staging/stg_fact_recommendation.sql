select
  recommendation_id,
  customer_sk,
  product_sk,
  customer_id,
  product_id,
  recommended_size,
  confidence_score,
  recommendation_ts,
  event_id,
  record_hash,
  batch_id,
  created_at
from {{ source('curated', 'fact_recommendation') }}
