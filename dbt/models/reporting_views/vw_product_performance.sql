select
  product_id,
  brand,
  category,
  fit_profile,
  sum(purchase_count) as purchase_count,
  sum(gross_purchase_value) as gross_purchase_value,
  sum(return_count) as return_count,
  sum(fit_return_count) as fit_return_count,
  sum(fit_return_value) as fit_return_value,
  {{ safe_rate('sum(fit_return_count)', 'sum(purchase_count)') }} as fit_related_return_rate,
  {{ safe_rate('sum(recommendation_match_count)', 'sum(purchase_count)') }}
    as recommendation_acceptance_rate
from {{ ref('mart_product_fit_performance') }}
group by 1,2,3,4
