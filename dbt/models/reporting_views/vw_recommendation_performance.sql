select
  recommendation_date,
  product_id,
  brand,
  category,
  recommendation_count,
  recommendations_with_purchase,
  correct_size_purchase_count,
  recommendation_accuracy,
  avg_confidence_score
from {{ ref('mart_recommendation_accuracy') }}
