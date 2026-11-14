with customers as (
  select *
  from {{ ref('stg_dim_customer') }}
  where is_current = true
),

purchases as (
  select *
  from {{ ref('stg_fact_purchase') }}
),

returns as (
  select *
  from {{ ref('stg_fact_return') }}
)

select
  c.customer_id,
  c.region,
  c.height_cm,
  c.weight_kg,
  c.preferred_fit,
  count(distinct p.purchase_id) as purchase_count,
  count(distinct r.return_id) as return_count,
  count(distinct if(r.is_fit_related_return = 1, r.return_id, null)) as fit_return_count,
  {{ safe_rate(
    "count(distinct if(r.is_fit_related_return = 1, r.return_id, null))",
    "count(distinct p.purchase_id)"
  ) }} as fit_related_return_rate,
  {{ safe_rate(
    "count(distinct if(p.recommended_size is not null and p.recommended_size = p.purchased_size, p.purchase_id, null))",
    "count(distinct if(p.recommended_size is not null, p.purchase_id, null))"
  ) }} as recommendation_acceptance_rate
from customers c
left join purchases p
  on c.customer_id = p.customer_id
left join returns r
  on p.purchase_id = r.purchase_id
group by 1,2,3,4,5
