{{ config(materialized='table') }}
select l.* from {{ ref('slv_loans_base') }} l join {{ ref('slv_customers') }} c using(customer_id) join {{ ref('slv_branches') }} b using(branch_id)
