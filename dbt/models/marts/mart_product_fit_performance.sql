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

with purchases as (
  select *
  from {{ ref('stg_fact_purchase') }}
  {% if is_incremental() %}
    where purchase_date >= date_sub(
      (select coalesce(max(metric_date), date('1900-01-01')) from {{ this }}),
      interval 7 day
    )
  {% endif %}
),

returns as (
  select *
  from {{ ref('stg_fact_return') }}
),

products as (
  select *
  from {{ ref('stg_dim_product') }}
  where is_current = true
),

purchase_return as (
  select
    p.purchase_date as metric_date,
    p.purchase_id,
    p.product_id,
    p.amount,
    p.recommended_size,
    p.purchased_size,
    r.return_id,
    r.return_amount,
    coalesce(r.is_fit_related_return, 0) as is_fit_related_return
  from purchases p
  left join returns r
    on p.purchase_id = r.purchase_id
),

aggregated as (
  select
    pr.metric_date,
    pr.product_id,
    dp.brand,
    dp.category,
    dp.fit_profile,
    count(distinct pr.purchase_id) as purchase_count,
    sum(pr.amount) as gross_purchase_value,
    count(distinct pr.return_id) as return_count,
    count(distinct if(pr.is_fit_related_return = 1, pr.return_id, null)) as fit_return_count,
    sum(if(pr.is_fit_related_return = 1, coalesce(pr.return_amount, 0), 0)) as fit_return_value,
    countif(pr.recommended_size = pr.purchased_size) as recommendation_match_count
  from purchase_return pr
  join products dp
    on pr.product_id = dp.product_id
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
  {{ safe_rate('fit_return_count', 'purchase_count') }} as fit_related_return_rate,
  {{ safe_rate('recommendation_match_count', 'purchase_count') }} as recommendation_acceptance_rate,
  current_timestamp() as dbt_updated_at
from aggregated
