# Fit Analytics - End-to-End GCP Architecture

```mermaid
flowchart LR
  subgraph Batch["Batch Pipeline"]
    A1["SFTP / NAS / API"] --> A2["GCS Landing"]
    A2 --> A3["Cloud Composer / Airflow"]
    A3 --> A4["Dataflow Batch Validation"]
    A4 -->|Valid| A5["BigQuery Stage"]
    A4 -->|Invalid| A6["Quarantine"]
    A5 --> A7["Dataproc / PySpark"]
    A7 --> A8["History Work Tables"]
    A8 --> A9["SCD2 + Fact MERGE"]
    A9 --> A10["BigQuery Curated"]
    A10 --> A11["dbt"]
    A11 --> A12["Reporting Marts / Views"]
  end

  subgraph Stream["Real-Time Recommendation Pipeline"]
    B1["Fit Recommendation Events"] --> B2["Pub/Sub"]
    B2 --> B3["Dataflow Streaming"]
    B3 -->|Valid| B4["BigQuery Storage Write API"]
    B3 -->|Invalid| B5["Streaming DLQ"]
    B3 --> B6["5-min Event-Time Windows"]
    B6 --> B7["Streaming Metrics"]
    B4 --> B8["Deduplicated Consumer View"]
  end

  subgraph Control["Platform Controls"]
    C1["IAM / Service Accounts"]
    C2["RLS / CLS / Authorized Views"]
    C3["Cloud Monitoring / Logging"]
    C4["Audit + Reconciliation"]
    C5["Watermark / Batch Control"]
    C6["GitHub Actions CI/CD"]
  end

  A12 --> D1["Analytics / BI / Fit KPIs"]
  B8 --> D1
  C1 -.secures.-> A5
  C1 -.secures.-> B4
  C2 -.protects.-> D1
  C3 -.monitors.-> A3
  C3 -.monitors.-> B3
  C4 -.validates.-> A9
  C5 -.controls.-> A3
  C6 -.deploys/tests.-> A3
```

## Batch Path

**SFTP/NAS/API -> GCS -> Composer -> Dataflow -> BigQuery Stage -> Dataproc/PySpark -> SCD2/MERGE -> dbt -> Reporting**

## Streaming Path

**Recommendation Event -> Pub/Sub -> Dataflow Streaming -> BigQuery -> Reporting**, with explicit DLQ routing, event-time windows, allowed lateness and source-level deduplication.

## Data Layers

- `fitanalytics_stage`: validated ingestion layer
- `fitanalytics_history`: transformation work/history layer
- `fitanalytics_curated`: SCD2 dimensions and facts
- `fitanalytics_reporting`: dbt KPI marts and reporting views
- `fitanalytics_streaming`: real-time recommendation events and window metrics
- `fitanalytics_audit`: pipeline audit, rejects, reconciliation and watermarks
