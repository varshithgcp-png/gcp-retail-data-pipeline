{{
  config(
    materialized='table',
    cluster_by=['region', 'category']
  )
}}

with customers as (
  select *
  from {{ ref('stg_dim_customer') }}
),

purchases as (
  select *
  from {{ ref('stg_fact_purchase') }}
),

returns as (
  select *
  from {{ ref('stg_fact_return') }}
),

returns_by_purchase as (
  select
    purchase_id,
    count(distinct return_id) as return_count,
    count(distinct if(is_fit_related_return = 1, return_id, null))
      as fit_return_count,
    sum(
      if(is_fit_related_return = 1, coalesce(return_amount, 0), 0)
    ) as fit_return_value
  from returns
  group by purchase_id
),

products as (
  select *
  from {{ ref('stg_dim_product') }}
),

joined as (
  select
    c.region,
    dp.category,
    p.purchase_id,
    p.amount,
    p.recommended_size,
    p.purchased_size,
    coalesce(r.return_count, 0) as return_count,
    coalesce(r.fit_return_count, 0) as fit_return_count,
    coalesce(r.fit_return_value, 0) as fit_return_value
  from purchases p
  join customers c
    on p.customer_sk = c.customer_sk
  join products dp
    on p.product_sk = dp.product_sk
  left join returns_by_purchase r
    on p.purchase_id = r.purchase_id
)

select
  region,
  category,
  count(*) as purchase_count,
  sum(return_count) as return_count,
  sum(fit_return_count) as fit_return_count,
  sum(fit_return_value) as fit_return_value,
  {{ safe_rate('sum(fit_return_count)', 'count(*)') }}
    as fit_related_return_rate,
  {{ safe_rate(
    "countif(recommended_size = purchased_size)",
    "count(*)"
  ) }} as recommendation_acceptance_rate,
  current_timestamp() as dbt_updated_at
from joined
group by 1,2
