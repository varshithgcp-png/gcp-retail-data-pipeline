{{
  config(
    materialized='table',
    cluster_by=['brand', 'category', 'product_id']
  )
}}

with recommendations as (
  select *
  from {{ ref('stg_fact_recommendation') }}
),

purchases as (
  select *
  from {{ ref('stg_fact_purchase') }}
),

products as (
  select *
  from {{ ref('stg_dim_product') }}
  where is_current = true
),

candidate_matches as (
  select
    r.recommendation_id,
    r.event_id,
    r.customer_id,
    r.product_id,
    r.recommended_size,
    r.confidence_score,
    r.recommendation_ts,
    p.purchase_id,
    p.purchased_size,
    p.purchase_date,
    row_number() over (
      partition by r.recommendation_id
      order by timestamp(p.purchase_date) asc
    ) as rn
  from recommendations r
  left join purchases p
    on r.customer_id = p.customer_id
   and r.product_id = p.product_id
   and timestamp(p.purchase_date) >= r.recommendation_ts
   and timestamp(p.purchase_date) < timestamp_add(r.recommendation_ts, interval 30 day)
),

latest as (
  select *
  from candidate_matches
  where rn = 1
)

select
  date(r.recommendation_ts) as recommendation_date,
  r.product_id,
  dp.brand,
  dp.category,
  count(*) as recommendation_count,
  countif(r.purchase_id is not null) as recommendations_with_purchase,
  countif(r.purchase_id is not null and r.recommended_size = r.purchased_size) as correct_size_purchase_count,
  {{ safe_rate(
    "countif(r.purchase_id is not null and r.recommended_size = r.purchased_size)",
    "countif(r.purchase_id is not null)"
  ) }} as recommendation_accuracy,
  avg(r.confidence_score) as avg_confidence_score,
  current_timestamp() as dbt_updated_at
from latest r
join products dp
  on r.product_id = dp.product_id
group by 1,2,3,4
