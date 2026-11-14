{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['metric_date', 'product_id'],
    partition_by={'field': 'metric_date', 'data_type': 'date'},
    cluster_by=['brand', 'category', 'product_id'],
    on_schema_change='sync_all_columns'
  )
}}

with returns as (
  select *
  from {{ ref('stg_fact_return') }}
),

recent_return_purchases as (
  select distinct purchase_id
  from returns
  {% if is_incremental() %}
    where created_at >= timestamp_sub(
      (
        select coalesce(
          max(dbt_updated_at),
          timestamp('1970-01-01')
        )
        from {{ this }}
      ),
      interval 1 day
    )
  {% endif %}
),

purchases as (
  select *
  from {{ ref('stg_fact_purchase') }}
  {% if is_incremental() %}
    where purchase_date >= date_sub(
      (
        select coalesce(
          max(metric_date),
          date('1900-01-01')
        )
        from {{ this }}
      ),
      interval {{ var('return_lookback_days', 30) }} day
    )
    or purchase_id in (
      select purchase_id from recent_return_purchases
    )
  {% endif %}
),

returns_by_purchase as (
  select
    r.purchase_id,
    count(distinct r.return_id) as return_count,
    max(if(r.is_fit_related_return = 1, 1, 0))
      as fit_return_count,
    sum(
      if(r.is_fit_related_return = 1, coalesce(r.return_amount, 0), 0)
    ) as fit_return_value
  from returns r
  join purchases p
    on r.purchase_id = p.purchase_id
  group by r.purchase_id
),

products as (
  select *
  from {{ ref('stg_dim_product') }}
),

purchase_return as (
  select
    p.purchase_date as metric_date,
    p.purchase_id,
    p.product_id,
    p.product_sk,
    p.amount,
    p.recommended_size,
    p.purchased_size,
    coalesce(r.return_count, 0) as return_count,
    coalesce(r.fit_return_count, 0) as fit_return_count,
    coalesce(r.fit_return_value, 0) as fit_return_value
  from purchases p
  left join returns_by_purchase r
    on p.purchase_id = r.purchase_id
),

aggregated as (
  select
    pr.metric_date,
    pr.product_id,
    dp.brand,
    dp.category,
    dp.fit_profile,
    count(*) as purchase_count,
    sum(pr.amount) as gross_purchase_value,
    sum(pr.return_count) as return_count,
    sum(pr.fit_return_count) as fit_return_count,
    sum(pr.fit_return_value) as fit_return_value,
    countif(pr.recommended_size is not null)
      as recommendation_exposed_count,
    countif(
      pr.recommended_size is not null
      and pr.recommended_size = pr.purchased_size
    ) as recommendation_match_count
  from purchase_return pr
  join products dp
    on pr.product_sk = dp.product_sk
  group by 1,2,3,4,5
)

select
  metric_date,
  product_id,
  brand,
  category,
  fit_profile,
  purchase_count,
  gross_purchase_value,
  return_count,
  fit_return_count,
  fit_return_value,
  recommendation_exposed_count,
  recommendation_match_count,
  {{ safe_rate('fit_return_count', 'purchase_count') }} as fit_related_return_rate,
  {{ safe_rate(
    'recommendation_match_count',
    'recommendation_exposed_count'
  ) }} as recommendation_acceptance_rate,
  current_timestamp() as dbt_updated_at
from aggregated
