{{ config(materialized='table') }}
select * from {{ ref('fact_transaction') }}
where abs(amount) > {{ var('suspicious_transaction_threshold',100000) }}
   or is_structuring_candidate or is_dormant_reactivation_burst or is_currency_mismatch
