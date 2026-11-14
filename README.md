# Fit Analytics - GCP Data Engineering Portfolio

End-to-end Google Cloud data engineering portfolio project for an **apparel fit and size recommendation** use case.

The solution demonstrates production-style **batch + streaming ingestion, data quality, SCD Type 2, BigQuery modelling, dbt analytics, Airflow orchestration, monitoring, security and CI/CD**.

> **Portfolio disclaimer:** all data, brand names and events in this repository are synthetic. This repository contains no proprietary client data, credentials or client source code.

## Architecture

```text
BATCH
SFTP / NAS / API
      |
      v
     GCS
      |
      v
Cloud Composer / Airflow
      |
      v
Dataflow Validation
      |
      v
BigQuery Stage
      |
      v
Dataproc / PySpark
      |
      v
SCD2 + Fact MERGE
      |
      v
BigQuery Curated
      |
      v
     dbt
      |
      v
Reporting / Fit KPIs


STREAMING
Recommendation Events
      |
      v
   Pub/Sub
      |
      v
Dataflow Streaming
   |       |       |
 valid   DLQ    windows
   |               |
   v               v
BigQuery       Streaming Metrics
```

See the full Mermaid architecture: [End-to-End Architecture](architecture/end_to_end_architecture.md).

## Technology Stack

| Area | Technologies |
|---|---|
| Storage / Ingestion | GCS, Pub/Sub |
| Processing | Dataflow / Apache Beam, Dataproc / PySpark |
| Warehouse | BigQuery |
| Orchestration | Cloud Composer / Apache Airflow |
| Transformation | dbt, SQL, PySpark |
| Languages | Python, SQL |
| Security | IAM, Service Accounts, RLS, CLS design |
| Monitoring | Cloud Monitoring, BigQuery audit queries |
| CI/CD | GitHub Actions, Workload Identity Federation |

## Business Entities

The platform processes five core entities:

- Customer fit profiles
- Products and sizing behaviour
- Purchases
- Returns and fit feedback
- Size recommendations

## BigQuery Data Layers

| Dataset | Purpose |
|---|---|
| `fitanalytics_stage` | validated ingestion/staging tables |
| `fitanalytics_history` | transformation work and history tables |
| `fitanalytics_curated` | SCD2 dimensions and fact tables |
| `fitanalytics_audit` | quarantine, rejects, audit, reconciliation and watermarks |
| `fitanalytics_reporting` | dbt KPI marts and reporting views |
| `fitanalytics_streaming` | real-time recommendation events and window metrics |

## Batch Pipeline

```text
Source Files
   |
   v
GCS Landing
   |
   v
File / Schema / Row DQ
   |
   +--> Invalid -> Quarantine
   |
   v
BigQuery Stage
   |
   v
PySpark Dedupe + RI + SHA-256
   |
   v
History Work Tables
   |
   v
SCD2 Dimensions + Fact MERGE
   |
   v
dbt KPI Marts / Reporting Views
```

The batch flow supports:

- business-date processing
- two daily arrival windows
- retries
- replay-safe `batch_id`
- controlled backfills
- deduplication
- quarantine
- source-to-target reconciliation
- watermark updates
- archive-on-success behaviour

See [Composer / Airflow Runbook](docs/run_composer.md).

## Streaming Pipeline

```text
Recommendation Event
      |
      v
    Pub/Sub
      |
      v
Dataflow Streaming
      |
 +----+----------------+
 |                     |
valid                 invalid
 |                     |
 v                     v
BigQuery             DLQ
 |
 +--> 5-minute event-time metrics
```

The streaming flow demonstrates:

- publisher-generated `event_id`
- Pub/Sub source deduplication
- Pub/Sub event-time timestamp attributes used for Dataflow watermarking
- watermarks
- fixed windows
- allowed lateness
- early/late panes for monitoring
- explicit DLQ routing
- BigQuery Storage Write API
- persistent BigQuery deduplicated consumer view

See [Streaming Runbook](docs/run_streaming.md).

## Curated Warehouse Model

### Dimensions

- `dim_customer` - SCD Type 2
- `dim_product` - SCD Type 2

### Facts

- `fact_purchase`
- `fact_return`
- `fact_recommendation`

SCD2 uses:

```text
effective_start_ts
effective_end_ts
is_current
record_hash
customer_sk / product_sk
```

SHA-256 hashes are generated from business attributes to detect changes.

## dbt Reporting Layer

### Unified Staging

- `stg_recommendation_unified` - batch + streaming recommendations deduplicated by `event_id`

### KPI Marts

- `mart_product_fit_performance`
- `mart_recommendation_accuracy`
- `mart_brand_size_consistency`
- `mart_region_fit_trends`

### Reporting Views

- `vw_customer_fit_profile`
- `vw_product_performance`
- `vw_return_analysis`
- `vw_recommendation_performance`
- `vw_brand_performance`

See [dbt Runbook](docs/run_dbt.md).

## Fit Analytics KPIs

The portfolio demonstrates calculations for:

- Fit-Related Return Rate
- Recommendation Accuracy
- Recommendation Acceptance Rate
- Runs Small / Runs Large
- True-to-Size Share
- Brand Size Consistency
- Customer Fit Profile vs Purchase
- Category Fit Issues
- Regional Fit Trends
- Financial Impact of Fit-Related Returns

## Data Quality and Reliability

The project includes:

- file existence and header checks
- schema validation
- required-field checks
- data-type validation
- domain validation
- duplicate removal
- customer/product and return-to-purchase referential-integrity checks
- quarantine and transformation rejects
- audit counts
- reconciliation
- watermark/control tracking
- idempotent MERGE logic
- replay-safe batch cleanup

Reconciliation tracks:

```text
stage_count
valid_count
rejected_count
duplicate_count
target_count
reconciliation_status
```

## BigQuery Optimization

The design uses:

- date/event-date partitioning
- clustering on common filter/join columns
- partition pruning
- incremental dbt MERGE models
- limited late-data lookback
- monitoring through `INFORMATION_SCHEMA.JOBS*`

See [Optimization Guide](docs/optimization.md).

## Security

The design includes:

- separate service identities
- least-privilege IAM
- row-level security examples
- column-level policy-tag design
- authorized-view pattern
- no committed service-account keys
- GitHub Workload Identity Federation for deployment

See [Security Design](docs/security.md).

## Monitoring

Operational monitoring covers:

- Dataflow job failures and system lag
- Pub/Sub backlog
- streaming event lag
- Composer task/DAG failures
- reconciliation mismatches
- BigQuery bytes/slot usage
- DLQ trends
- Dataproc job duration and Spark failures

See [Monitoring Guide](docs/monitoring.md) and [Monitoring SQL](bigquery/monitoring_queries.sql).

## CI/CD

### CI

`.github/workflows/ci.yml`

Validates:

- Python/Airflow syntax
- batch validation tests
- streaming validation tests
- synthetic use-case contract tests
- dbt dependency resolution and parse validation

### Deployment

`.github/workflows/deploy-gcp.yml`

Uses **GitHub Actions + Workload Identity Federation** and is intentionally manual until real GCP repository variables/secrets are configured.

The workflow can:

- authenticate without storing a service-account key
- package/upload Dataproc code
- sync supporting code to the Composer `/data` folder
- import the Composer DAG

## Repository Structure

```text
gcp-retail-data-pipeline/
├── .github/workflows/   # CI and GCP deployment workflows
├── airflow/             # Cloud Composer DAG and config
├── architecture/        # End-to-end architecture
├── bigquery/            # DDL, MERGE, security and monitoring SQL
├── config/              # Batch and streaming configuration
├── data/                # Synthetic sample datasets/events
├── dataflow/            # Batch Apache Beam ingestion
├── dbt/                 # Staging, KPI marts, tests and views
├── docs/                # Runbooks and design documentation
├── scripts/             # GCP resource setup helpers
├── spark_jobs/          # Dataproc/PySpark transformations
├── streaming/           # Pub/Sub -> Dataflow streaming pipeline
└── tests/               # Unit tests
```

## Runbooks

- [Batch Dataflow](docs/run_dataflow.md)
- [Dataproc / PySpark](docs/run_dataproc.md)
- [dbt](docs/run_dbt.md)
- [Cloud Composer / Airflow](docs/run_composer.md)
- [Pub/Sub / Dataflow Streaming](docs/run_streaming.md)
- [Deployment Checklist](docs/deployment_checklist.md)
- [Expected Synthetic Results](docs/expected_results.md)
- [Full Use-Case Review](docs/use_case_review.md)
- [Portfolio / Interview Summary](docs/portfolio_summary.md)

## Project Status

- Phase 1: ✅ Portfolio foundation and synthetic datasets
- Phase 2: ✅ Dataflow batch ingestion and validation
- Phase 3: ✅ PySpark transformation, SCD2 and reconciliation
- Phase 4: ✅ dbt KPI marts, tests and reporting views
- Phase 5: ✅ Cloud Composer / Airflow orchestration
- Phase 6: ✅ Pub/Sub + Dataflow real-time streaming
- Phase 7: ✅ Architecture, optimization, security, monitoring and CI/CD

## Execution Status

The repository implementation is complete as a portfolio codebase.

**It has not yet been executed end-to-end against this repository owner's live GCP environment.** Use the [Deployment Checklist](docs/deployment_checklist.md) before adding real execution screenshots or claiming a successful deployed run.

## Portfolio Summary

For resume bullets, a LinkedIn project description and a two-minute interview walkthrough, see:

[Portfolio and Interview Summary](docs/portfolio_summary.md)
