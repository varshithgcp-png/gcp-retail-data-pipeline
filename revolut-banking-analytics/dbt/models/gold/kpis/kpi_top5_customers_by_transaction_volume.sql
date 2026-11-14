{{ config(materialized='table') }}
select customer_id, sum(abs(amount)) transaction_volume, count(*) transaction_count
from {{ ref('fact_transaction') }}
group by customer_id
order by transaction_volume desc
limit 5
