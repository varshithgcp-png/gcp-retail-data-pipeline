# Fit Analytics - GCP Data Engineering Pipeline

Portfolio project representing an apparel fit and size recommendation data platform built on Google Cloud.

> This repository uses fully synthetic data and a portfolio-safe architecture. It does not contain proprietary client data or source code.

## Business Problem

An apparel fit analytics platform helps customers select the right clothing size and helps brands understand fit-related returns, product sizing behaviour and recommendation accuracy.

The platform processes:

- customer fit-profile data
- product and sizing data
- purchases
- returns and fit feedback
- size recommendations
- batch historical files
- near-real-time recommendation events

## End-to-End Architecture

```text
Batch Sources: SFTP / NAS / API
             |
             v
        GCS Landing
             |
      File + Schema DQ
             |
             v
   Dataflow / Dataproc
             |
             v
   BigQuery Stage Layer
             |
      Dedupe / SCD2
             |
             v
 BigQuery History + Curated
             |
          dbt
             |
             v
 Reporting Views / KPIs

Streaming:
Recommendation Events
        |
        v
     Pub/Sub
        |
        v
     Dataflow
        |
  +-----+------+
  |            |
BigQuery      DLQ

Orchestration: Cloud Composer / Airflow
Audit: BigQuery Audit Tables
Security: IAM, Service Accounts, RLS/CLS
```

## GCP Technology Stack

- Google Cloud Storage
- Pub/Sub
- Dataflow / Apache Beam
- Dataproc / PySpark
- BigQuery
- Cloud Composer / Apache Airflow
- dbt
- Python
- SQL
- PySpark
- IAM / Service Accounts

## BigQuery Datasets

| Dataset | Purpose |
|---|---|
| `fitanalytics_stage` | Raw validated staging tables |
| `fitanalytics_history` | Historical change tracking |
| `fitanalytics_curated` | Dimensions and fact tables |
| `fitanalytics_audit` | Pipeline audit and quarantine records |
| `fitanalytics_reporting` | Analytics views and KPI outputs |

## Main Tables

### Stage
- `stg_customer`
- `stg_product`
- `stg_purchase`
- `stg_return`
- `stg_recommendation`

### Curated
- `dim_customer`
- `dim_product`
- `fact_purchase`
- `fact_return`
- `fact_recommendation`

## Engineering Scenarios Demonstrated

- file-level validation
- schema validation
- NULL and domain checks
- duplicate removal
- referential integrity
- SHA-256 change detection
- SCD Type 2
- incremental MERGE
- watermark-based processing
- idempotent reruns
- late-arriving records
- quarantine / dead-letter handling
- source-to-target reconciliation
- BigQuery partitioning and clustering

## Fit Analytics KPIs

- Fit-Related Return Rate
- Recommendation Accuracy
- Runs Small / Runs Large
- Brand Size Consistency
- Customer Fit Profile vs Purchase
- Category Fit Issues
- Region Fit Trends
- Financial Impact of Fit Issues

## Repository Structure

```text
gcp-retail-data-pipeline/
├── data/             # Synthetic customer/product/purchase/return/recommendation files
├── dataflow/         # Apache Beam ingestion pipelines
├── pyspark/          # Dedupe, SCD2 and transformation logic
├── airflow/          # Cloud Composer orchestration
├── bigquery/         # DDL, MERGE and KPI SQL
├── dbt/              # Models, tests, macros and snapshots
├── config/           # Metadata-driven pipeline configuration
├── tests/            # Unit tests
├── architecture/     # Architecture documentation
└── docs/             # Runbooks and project documentation
```

## Project Status

- Phase 1: Portfolio foundation and synthetic datasets
- Phase 2: Metadata-driven Dataflow ingestion and validation
- Phase 3: PySpark transformation + SCD Type 2
- Phase 4: BigQuery MERGE + dbt curated models
- Phase 5: Airflow orchestration
- Phase 6: Pub/Sub streaming pipeline
- Phase 7: Reporting KPIs, optimization and final architecture
