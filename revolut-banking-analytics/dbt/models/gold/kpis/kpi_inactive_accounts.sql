{{ config(materialized='table') }}
with anchor as (select max(transaction_date) as as_of_date from {{ ref('fact_transaction') }}), last_tx as (select account_id,max(transaction_date) last_transaction_date from {{ ref('fact_transaction') }} group by account_id)
select a.account_id,a.customer_id,a.status,l.last_transaction_date,anchor.as_of_date from {{ ref('dim_account') }} a cross join anchor left join last_tx l using(account_id) where a.is_current and (l.last_transaction_date is null or l.last_transaction_date < date_sub(anchor.as_of_date,interval {{ var('inactive_months',3) }} month))
