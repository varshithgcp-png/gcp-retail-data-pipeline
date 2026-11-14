{{ config(materialized='table') }}
select e.*
from {{ ref('slv_employees_base') }} e
join {{ ref('slv_branches') }} b using(branch_id)
