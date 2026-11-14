select
  recommendation_date,
  product_id,
  brand,
  category,
  recommendation_count,
  streaming_recommendation_count,
  recommendations_with_purchase,
  recommendation_conversion_rate,
  correct_size_purchase_count,
  recommendation_accuracy,
  avg_confidence_score
from {{ ref('mart_recommendation_accuracy') }}
