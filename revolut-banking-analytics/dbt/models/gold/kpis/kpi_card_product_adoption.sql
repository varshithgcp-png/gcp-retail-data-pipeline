{{ config(materialized='table') }}
with active_cards as (select distinct a.customer_id from {{ ref('dim_account') }} a join {{ ref('dim_card') }} c using(account_id) where a.is_current and c.status='ACTIVE')
select c.customer_id,c.first_name,c.last_name,c.risk_rating,ac.customer_id is not null has_active_card,ac.customer_id is null is_cross_sell_candidate from {{ ref('dim_customer') }} c left join active_cards ac using(customer_id) where c.is_current
