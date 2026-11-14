select
  brand,
  category,
  purchase_count,
  fit_return_count,
  fit_related_return_rate,
  true_to_size_share,
  runs_small_share,
  runs_large_share
from {{ ref('mart_brand_size_consistency') }}
