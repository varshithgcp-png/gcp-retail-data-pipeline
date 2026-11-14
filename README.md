# GCP Retail Data Engineering Pipeline

End-to-end portfolio project demonstrating a production-style batch retail data pipeline on Google Cloud.

## Architecture

```text
Retail Source Files
        |
        v
 Google Cloud Storage
        |
        v
 Validation & Data Quality
        |
        v
 Dataflow / Dataproc
        |
        v
 BigQuery Staging
        |
        v
 dbt Transformations
        |
        v
 BigQuery Curated
        |
        v
 Analytics / Reporting

Invalid Records ---> Quarantine
Pipeline Metrics --> Audit Tables
```

## Technology Stack

- Google Cloud Storage (GCS)
- Google Cloud Dataflow / Apache Beam
- Google Cloud Dataproc / PySpark
- BigQuery
- Cloud Composer / Apache Airflow
- dbt
- Python
- SQL
- PySpark

## Business Use Case

The pipeline processes retail customer, product, purchase and return data. It demonstrates ingestion, validation, deduplication, incremental processing, SCD Type 2, data-quality checks, reconciliation, audit logging and analytics-ready modelling.

## Repository Structure

```text
gcp-retail-data-pipeline/
├── data/             # Synthetic retail input datasets
├── bigquery/         # BigQuery DDL, MERGE and reporting SQL
├── dataflow/         # Apache Beam ingestion pipelines
├── pyspark/          # Spark transformations and SCD2
├── airflow/          # Composer/Airflow orchestration
├── dbt/              # Transformation models, tests and snapshots
├── architecture/     # Architecture diagrams
└── docs/             # Project documentation
```

## Data Quality Scenarios

The sample data intentionally contains records that allow the pipeline to demonstrate:

- duplicate detection
- missing critical fields
- referential-integrity validation
- updated customer records
- source-to-target reconciliation
- quarantine handling

## BigQuery Design

Staging and curated datasets are separated. Fact-style tables use date partitioning and customer/product clustering where appropriate to reduce scanned data and improve analytical query performance.

## Planned Pipeline Flow

1. Source CSV files land in GCS.
2. File and schema checks validate incoming files.
3. Dataflow loads valid data into BigQuery staging.
4. Invalid records move to quarantine.
5. PySpark/dbt applies deduplication and business transformations.
6. SCD Type 2 preserves customer history.
7. Curated dimensions and facts are published in BigQuery.
8. Audit tables capture counts, status and rejected records.
9. Airflow orchestrates dependencies, retries and monitoring.

## Current Status

**Phase 1 complete:** repository foundation, synthetic source datasets, initial BigQuery schemas and project documentation.

**Next:** Dataflow ingestion and reusable data-quality validation.

## Disclaimer

All datasets in this repository are synthetic and created only for learning and portfolio demonstration. No client or proprietary data is included.
