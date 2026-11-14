select
  customer_sk,
  customer_id,
  region,
  height_cm,
  weight_kg,
  preferred_fit,
  effective_start_ts,
  effective_end_ts,
  is_current,
  record_hash,
  batch_id,
  created_at
from {{ source('curated', 'dim_customer') }}
