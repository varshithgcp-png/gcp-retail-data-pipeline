select
  product_sk,
  product_id,
  sku,
  brand,
  category,
  size_system,
  size_label,
  fit_profile,
  unit_price,
  effective_start_ts,
  effective_end_ts,
  is_current,
  record_hash,
  batch_id,
  created_at
from {{ source('curated', 'dim_product') }}
