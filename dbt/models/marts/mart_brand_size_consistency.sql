{{
  config(
    materialized='table',
    cluster_by=['brand', 'category']
  )
}}

with product_performance as (
  select *
  from {{ ref('mart_product_fit_performance') }}
),

brand_rollup as (
  select
    brand,
    category,
    sum(purchase_count) as purchase_count,
    sum(fit_return_count) as fit_return_count,
    sum(case when fit_profile = 'RUNS_SMALL' then purchase_count else 0 end) as runs_small_exposure,
    sum(case when fit_profile = 'RUNS_LARGE' then purchase_count else 0 end) as runs_large_exposure,
    sum(case when fit_profile = 'TRUE_TO_SIZE' then purchase_count else 0 end) as true_to_size_exposure
  from product_performance
  group by 1,2
)

select
  brand,
  category,
  purchase_count,
  fit_return_count,
  {{ safe_rate('fit_return_count', 'purchase_count') }} as fit_related_return_rate,
  {{ safe_rate('true_to_size_exposure', 'purchase_count') }} as true_to_size_share,
  {{ safe_rate('runs_small_exposure', 'purchase_count') }} as runs_small_share,
  {{ safe_rate('runs_large_exposure', 'purchase_count') }} as runs_large_share,
  current_timestamp() as dbt_updated_at
from brand_rollup
