# Revolut-Style Banking Analytics Platform

Synthetic portfolio implementation of the **Analyze Customer Transactions** use case using only **Python, GCS, BigQuery, dbt, SQL and Airflow/Cloud Composer**. No Dataflow or Dataproc is used.

## Business goal
Turn one interleaved BankEvalDB-style file into governed Bronze, Silver and Gold analytics for customer transaction behaviour, anomaly review, SCD Type 2 history, loan health and card adoption.

## Day contract
- **Day 0:** full baseline with all ten entities.
- **Day 1:** Customers + Accounts only; new customers 369-370 and accounts 481-482.
- **Day 2:** Customers + Accounts only; new customers 371-372 and accounts 483-484.

## Bronze tables
Exact raw-table names:
- `BRANCHES_bronze`
- `CUSTOMERS_bronze`
- `EMPLOYEES_bronze`
- `ACCOUNTTYPES_bronze`
- `ACCOUNTS_bronze`
- `CARDS_bronze`
- `TRANSACTIONTYPES_bronze`
- `TRANSACTIONS_bronze`
- `LOANS_bronze`
- `LOANPAYMENTS_bronze`
- `CONTROL_TABLE_bronze`

Metadata tables: `ENTITY_CONFIG_bronze`, `VALIDATION_RULES_bronze`.

## Ownership by layer
```text
GCS interleaved file
  -> Python: dynamic HDR/FTR split + replay-safe BigQuery Bronze load
  -> dbt Silver: typing + cleansing + dedupe + RI + DQ/anomaly evidence
  -> dbt snapshots: Customer/Account SCD2
  -> dbt Gold: star schema + facts + KPI outputs
  -> Airflow: orchestration only
```

## Gold star schema
Dimensions:
`dim_customer`, `dim_account`, `dim_branch`, `dim_account_type`, `dim_transaction_type`, `dim_date`, `dim_card`, `dim_loan`.

Facts:
- `fact_transaction` — grain: **one valid unique banking transaction**.
- `fact_loan_payment` — grain: **one unique loan payment**.

## Required KPIs
1. `kpi_top5_customers_by_transaction_volume`
2. `kpi_product_revenue_contribution`
3. `kpi_inactive_accounts`
4. `kpi_suspicious_transactions`
5. `kpi_customer_account_history`
6. `kpi_loan_repayment_health`
7. `kpi_card_product_adoption`

## DQ / anomaly evidence
`revolut_silver.dq_quarantine` retains rule name, severity and failure reason for duplicate customers, missing customer fields, duplicate transactions, currency mismatches, invalid dates, sort-code issues, overdraft breaches, structuring candidates and dormant-reactivation bursts.

## Setup
1. Replace `PROJECT_ID` in `bigquery/create_bronze.sql` and run it.
2. Generate the deterministic 2,748-row Day 0 baseline:
   ```bash
   python data/generate_day0.py
   ```
3. Upload Day 0, Day 1 and Day 2 from `data/` to your GCS bucket.
4. Install `requirements.txt`.
5. Load metadata:
   ```bash
   python -m banking_pipeline.seed_metadata --project-id YOUR_PROJECT_ID
   ```
6. Run Day 0 Bronze locally or from Composer:
   ```bash
   python -m banking_pipeline.bronze_loader \
     --project-id YOUR_PROJECT_ID \
     --source gs://YOUR_BUCKET/revolut/source/day0/EvalDB_Day0_Interleaved.txt \
     --business-date 2026-11-01 \
     --load-type FULL
   ```
7. Run dbt Silver, snapshots and Gold:
   ```bash
   cd dbt
   dbt build --select tag:silver --profiles-dir /path/to/profile
   dbt snapshot --profiles-dir /path/to/profile
   dbt build --select tag:gold --profiles-dir /path/to/profile
   ```
8. Repeat Day 1 (`2026-11-02`, `INCREMENTAL`) and Day 2 (`2026-11-03`, `INCREMENTAL`) in sequence so snapshots capture the historical changes.

See `docs/` for architecture, profiling, assumptions, incremental evidence and onboarding.
