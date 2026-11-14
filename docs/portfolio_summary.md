# Portfolio and Interview Summary

## LinkedIn Project Description

Built an end-to-end GCP data engineering portfolio solution for an apparel fit and size recommendation use case. The platform supports batch and real-time pipelines using GCS, Pub/Sub, Dataflow, Dataproc/PySpark, BigQuery, Cloud Composer/Airflow and dbt. Implemented file/schema/data-quality validation, quarantine and DLQ handling, deduplication, SHA-256 change detection, SCD Type 2, incremental MERGE, watermarks, replay-safe backfills, reconciliation, event-time windowing, late-data handling, partitioning/clustering and reporting KPIs.

## Resume Bullets

- Designed a production-style GCP batch architecture using GCS, Dataflow, Dataproc, BigQuery and Cloud Composer with validation, audit, quarantine and replay-safe processing.
- Implemented SCD Type 2 customer/product dimensions using PySpark SHA-256 change hashes and BigQuery affected-key history rebuild logic for controlled backfills.
- Built Pub/Sub -> Dataflow streaming recommendation ingestion with event-time processing, deduplication, allowed lateness, DLQ routing and BigQuery Storage Write API.
- Created dbt staging/reporting models, incremental KPI marts and automated data tests for fit-return and recommendation analytics.
- Added reconciliation, watermark controls, partitioning/clustering, monitoring queries, security examples and GitHub Actions CI/CD design.

## Two-Minute Interview Walkthrough

The platform has separate batch and streaming paths. Batch files arrive in GCS and Cloud Composer waits for all required entity files, validates file name/extension/size/header, and then starts Dataflow. Dataflow performs row/schema validation, valid rows go to BigQuery staging and invalid rows go to quarantine. Dataproc/PySpark deduplicates records, performs referential-integrity checks and creates SHA-256 hashes. Customer and product dimensions use SCD Type 2, while purchase, return and recommendation facts use idempotent MERGE logic. dbt builds KPI marts and reporting views.

For real-time recommendations, Pub/Sub sends events to Dataflow streaming. The pipeline uses event IDs for retry deduplication, validates the event timestamp and assigns it as Beam event time before windowing, uses fixed windows and allowed lateness, and routes bad payloads to an explicit DLQ. BigQuery tables are partitioned and clustered for cost/performance. dbt unifies batch and streaming recommendations for reporting. Audit, reconciliation, watermark/control state and Composer backfill parameters make reruns controlled and traceable.

## Portfolio Disclaimer

This repository is a synthetic portfolio implementation inspired by common apparel fit-analytics requirements. It contains no proprietary client data, credentials or client source code.
