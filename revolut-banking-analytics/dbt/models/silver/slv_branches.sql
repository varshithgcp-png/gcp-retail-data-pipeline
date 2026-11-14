{{ config(materialized='table') }}
select * except(rn) from (
  select safe_cast(BranchID as int64) branch_id,trim(BranchName) branch_name,trim(City) city,trim(Country) country,trim(SortCodePrefix) sort_code_prefix,trim(Region) region,batch_id,business_date,source_file,ingestion_ts,row_number() over(partition by BranchID order by business_date desc, ingestion_ts desc, source_row_number desc) rn
  from {{ source('bronze','BRANCHES_bronze') }}
) where rn=1
