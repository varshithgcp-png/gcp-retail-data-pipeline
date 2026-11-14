{{ config(materialized='table') }}
select * from {{ ref('slv_account_types') }}
