select
  recommendation_id,
  event_id,
  recommendation_source,
  customer_id,
  product_id,
  recommended_size,
  confidence_score,
  recommendation_ts,
  customer_sk,
  product_sk
from {{ ref('stg_recommendation_unified') }}
where customer_sk is null
   or product_sk is null
