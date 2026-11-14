# Project Documentation

## Business Goal
Build a production-style retail analytics pipeline that ingests customer, product, purchase and return data, validates it, transforms it, and publishes analytics-ready BigQuery tables.

## Layers
1. **Landing** - CSV files arrive in Cloud Storage.
2. **Validation** - file, schema and row-level quality checks.
3. **Staging** - validated records are loaded to BigQuery staging tables.
4. **Curated** - deduplication, SCD Type 2 and business transformations create dimensions and facts.
5. **Reporting** - SQL/dbt models expose sales, return and customer metrics.
6. **Audit/Quarantine** - processing counts and rejected records are retained for traceability.

## Engineering Scenarios Included
- Duplicate purchase records
- Updated customer records
- Missing customer email
- Invalid customer reference in purchases
- Incremental processing
- Replay-safe design
- Partitioning and clustering
- Source-to-target reconciliation

## Planned Components
- Apache Beam/Dataflow batch ingestion
- PySpark transformations
- BigQuery SQL
- Cloud Composer/Airflow orchestration
- dbt models and tests
- Data-quality and audit framework
