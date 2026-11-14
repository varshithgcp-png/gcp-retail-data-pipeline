{{ config(materialized='table', cluster_by=['customer_id','is_current']) }}
with s as (
  select *,
    min(dbt_valid_from) over(partition by customer_id) first_valid_from,
    first_value(load_type) over(
      partition by customer_id order by dbt_valid_from
    ) first_load_type
  from {{ ref('customers_snapshot') }}
)
select
  to_hex(sha256(concat(cast(customer_id as string),'|',cast(dbt_valid_from as string)))) customer_sk,
  customer_id,first_name,last_name,email,phone,date_of_birth,city,country,risk_rating,
  case
    when dbt_valid_from=first_valid_from and first_load_type='FULL'
      then timestamp('1900-01-01')
    else dbt_valid_from
  end effective_from,
  coalesce(dbt_valid_to,timestamp('9999-12-31 23:59:59+00')) effective_to,
  dbt_valid_to is null is_current
from s
