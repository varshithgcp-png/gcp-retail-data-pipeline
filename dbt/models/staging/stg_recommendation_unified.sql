with batch_recommendations as (
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
    'batch' as recommendation_source,
    1 as source_priority
  from {{ ref('stg_fact_recommendation') }}
),

streaming_recommendations as (
  select
    s.recommendation_id,
    c.customer_sk,
    p.product_sk,
    s.customer_id,
    s.product_id,
    s.recommended_size,
    s.confidence_score,
    s.event_ts as recommendation_ts,
    s.event_id,
    'streaming' as recommendation_source,
    2 as source_priority
  from {{ source('streaming', 'vw_recommendation_events_deduped') }} s
  left join {{ ref('stg_dim_customer') }} c
    on c.customer_id = s.customer_id
   and s.event_ts between c.effective_start_ts and c.effective_end_ts
  left join {{ ref('stg_dim_product') }} p
    on p.product_id = s.product_id
   and s.event_ts between p.effective_start_ts and p.effective_end_ts
),

combined as (
  select * from batch_recommendations
  union all
  select * from streaming_recommendations
),

event_deduped as (
  select *
  from combined
  qualify row_number() over (
    partition by event_id
    order by source_priority asc, recommendation_ts desc
  ) = 1
)

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
  recommendation_source
from event_deduped
qualify row_number() over (
  partition by recommendation_id
  order by source_priority asc, recommendation_ts desc, event_id desc
) = 1
