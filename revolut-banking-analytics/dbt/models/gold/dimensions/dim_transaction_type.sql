{{ config(materialized='table') }}
select * from {{ ref('slv_transaction_types') }}
