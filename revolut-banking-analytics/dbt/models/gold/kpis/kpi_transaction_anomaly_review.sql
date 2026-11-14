{{ config(materialized='table') }}
select *
from {{ ref('fact_transaction') }}
where is_high_value
   or is_structuring_candidate
   or is_dormant_reactivation_burst
   or is_currency_mismatch
