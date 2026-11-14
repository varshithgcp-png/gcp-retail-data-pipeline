{{ config(materialized='table') }}
select p.* from {{ ref('slv_loan_payments_base') }} p join {{ ref('slv_loans') }} l using(loan_id)
