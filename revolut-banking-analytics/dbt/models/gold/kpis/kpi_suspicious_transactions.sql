{{ config(materialized='table') }}
select *
from {{ ref('fact_transaction') }}
where is_high_value
