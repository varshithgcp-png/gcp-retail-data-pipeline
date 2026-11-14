{{ config(materialized='table') }}
select c.* from {{ ref('slv_cards_base') }} c join {{ ref('slv_accounts') }} a using(account_id)
