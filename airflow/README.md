# Fit Analytics Cloud Composer Orchestration

The main DAG is:

`fitanalytics_batch_pipeline`

## Schedule

The DAG is scheduled for:

- 08:00 Asia/Kolkata
- 13:00 Asia/Kolkata

`catchup=False` prevents automatic historical runs. Manual backfills are supported by supplying a business date and batch ID.

## DAG Flow

```text
start
  |
  +--> wait customer file --> Dataflow --> stage check --+
  +--> wait product file  --> Dataflow --> stage check --+
  +--> wait purchase file --> Dataflow --> stage check --+--> stage gate
  +--> wait return file   --> Dataflow --> stage check --+
  +--> wait recommendation--> Dataflow --> stage check --+
                                                         |
                                                         v
                                               Dataproc / PySpark
                                                         |
                                                         v
                                               Reconciliation Gate
                                                         |
                                                         v
                                                    dbt build
                                                         |
                                                         v
                                                  Archive Files
                                                         |
                                                         v
                                                      success
```

## Manual Backfill / Rerun

Trigger the DAG with configuration such as:

```json
{
  "business_date": "2026-11-14",
  "batch_id": "20261114_backfill_01"
}
```

The batch ID is propagated into stage, history, curated and audit records.

## File Layout

```text
gs://YOUR_BUCKET/landing/
├── customer/business_date=YYYY-MM-DD/
├── product/business_date=YYYY-MM-DD/
├── purchase/business_date=YYYY-MM-DD/
├── return/business_date=YYYY-MM-DD/
└── recommendation/business_date=YYYY-MM-DD/
```

Processed files are moved to the matching `archive/` path only after Dataflow, Dataproc, reconciliation and dbt succeed.
