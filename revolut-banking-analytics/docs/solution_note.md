# Short Solution Note

## Architecture
A single pipe-delimited interleaved file lands in GCS. Airflow waits for the object and orchestrates a Python ingestion command. Python detects `HDR|ENTITY` / `FTR|ENTITY` blocks dynamically, validates headers from metadata, calculates a file SHA-256, and appends raw rows plus batch metadata to the exact Bronze tables. BigQuery `CONTROL_TABLE_bronze` provides file/entity counts and replay evidence.

All business transformation is ELT in dbt. Silver performs typing, cleansing, deduplication, referential integrity, DQ/anomaly rules and traceable quarantine evidence. dbt snapshots preserve Customer and Account SCD Type 2 history. Gold builds conformed dimensions, transaction/loan-payment facts and the seven required KPI outputs.

## Incremental processing
- Day 0: full baseline, all ten entity blocks.
- Day 1: Customers + Accounts only, including 369/370 and 481/482.
- Day 2: Customers + Accounts only, including 371/372 and 483/484.
- A deterministic batch ID is derived from business date + file hash.
- A previously successful file hash is skipped in Bronze, so replay does not duplicate raw rows.
- Silver always resolves a single trusted current business key.
- dbt snapshots create new Customer/Account versions when `UpdatedAt` changes.

## DQ decisions
Hard failures such as missing critical customer data, invalid transaction dates and broken references are retained as rule-level evidence and excluded from trusted Gold where appropriate. Soft anomaly flags such as currency mismatch, overdraft breach, structuring candidates and dormant reactivation are retained for risk review.

## New-entity onboarding
Add the entity header/primary key/table mapping to `config/entities.yml`, add Bronze DDL, optionally add validation rules, then add a dbt Silver model and Gold object only when analytically required. The parser core does not change and does not depend on entity order.
