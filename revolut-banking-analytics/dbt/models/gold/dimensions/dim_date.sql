{{ config(materialized='table') }}
with bounds as (select min(transaction_date) min_date,max(transaction_date) max_date from {{ ref('slv_transactions') }}), dates as (select d from bounds, unnest(generate_date_array(min_date,max_date)) d)
select cast(format_date('%Y%m%d',d) as int64) date_sk,d date,extract(year from d) year,extract(quarter from d) quarter,extract(month from d) month,format_date('%B',d) month_name,extract(day from d) day,format_date('%A',d) day_name from dates
