with performance as (
  select
    product_id,
    sum(purchase_count) as purchase_count,
    sum(gross_purchase_value) as gross_purchase_value,
    sum(return_count) as return_count,
    sum(fit_return_count) as fit_return_count,
    sum(fit_return_value) as fit_return_value,
    sum(recommendation_exposed_count) as recommendation_exposed_count,
    sum(recommendation_match_count) as recommendation_match_count
  from {{ ref('mart_product_fit_performance') }}
  group by product_id
),

current_product as (
  select
    product_id,
    brand,
    category,
    fit_profile
  from {{ ref('stg_dim_product') }}
  where is_current = true
)

select
  p.product_id,
  d.brand,
  d.category,
  d.fit_profile,
  p.purchase_count,
  p.gross_purchase_value,
  p.return_count,
  p.fit_return_count,
  p.fit_return_value,
  {{ safe_rate('p.fit_return_count', 'p.purchase_count') }}
    as fit_related_return_rate,
  {{ safe_rate(
    'p.recommendation_match_count',
    'p.recommendation_exposed_count'
  ) }} as recommendation_acceptance_rate
from performance p
join current_product d
  on p.product_id = d.product_id
