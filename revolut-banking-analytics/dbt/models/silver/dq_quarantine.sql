{{ config(materialized='table') }}
select * from {{ ref('dq_customer_issues') }}
union all select * from {{ ref('dq_account_issues') }}
union all select * from {{ ref('dq_transaction_issues') }}
union all select * from {{ ref('dq_referential_issues') }}
