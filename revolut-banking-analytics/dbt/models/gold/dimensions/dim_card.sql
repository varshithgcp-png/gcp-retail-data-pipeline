{{ config(materialized='table') }}
select * from {{ ref('slv_cards') }}
