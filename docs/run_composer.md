# Phase 5 - Cloud Composer / Airflow Orchestration

## Goal

Phase 5 connects the batch components created in Phases 2-4 into one production-style workflow.

```text
GCS Sensors
    |
    v
Replay Cleanup
    |
    v
Dataflow Ingestion
    |
    v
BigQuery Stage Validation
    |
    v
Dataproc / PySpark
    |
    v
Reconciliation Gate
    |
    v
dbt Build
    |
    v
GCS Archive
```

## Operators and Sensors

The DAG uses:

- `GCSObjectsWithPrefixExistenceSensor`
- `BeamRunPythonPipelineOperator`
- `BigQueryInsertJobOperator` for replay cleanup
- `BigQueryCheckOperator`
- `DataprocSubmitJobOperator`
- `BashOperator` for dbt Core
- `GCSToGCSOperator`

The GCS, Dataflow/Beam, BigQuery and Dataproc tasks use deferrable execution where supported so long-running waits do not occupy normal worker slots.

## 1. Prepare Composer Environment

Install the packages in:

```text
airflow/composer_requirements.txt
```

Set the environment variables shown in:

```text
airflow/environment_variables.example
```

Do not put credentials or JSON service-account keys in GitHub.

## 2. Deploy Repository Code

Make the repository available under the Composer environment data path:

```text
/home/airflow/gcs/data/gcp-retail-data-pipeline
```

The exact path is configurable with `FITANALYTICS_REPO_ROOT`.

Copy the DAG file into the Composer DAGs folder:

```text
airflow/fitanalytics_batch_dag.py
```

## 3. Package Dataproc Support Code

The Dataproc driver imports the `spark_jobs` package, so package it before upload.

Example from the repository root:

```bash
zip -r spark_jobs.zip spark_jobs
```

Upload:

```bash
gcloud storage cp spark_jobs/run_transformations.py \
  gs://YOUR_BUCKET/code/spark_jobs/run_transformations.py

gcloud storage cp spark_jobs.zip \
  gs://YOUR_BUCKET/code/spark_jobs/spark_jobs.zip
```

## 4. dbt Profile

Create a Composer-side `profiles.yml` under the directory configured by:

```text
FITANALYTICS_DBT_PROFILES_DIR
```

Use the example in:

```text
dbt/profiles.yml.example
```

The credential file must be stored securely and must not be committed to the repository.

## 5. Landing Path

Each entity arrives under a business-date folder:

```text
gs://YOUR_BUCKET/landing/customer/business_date=2026-11-14/customer_*.csv
gs://YOUR_BUCKET/landing/product/business_date=2026-11-14/product_*.csv
gs://YOUR_BUCKET/landing/purchase/business_date=2026-11-14/purchase_*.csv
gs://YOUR_BUCKET/landing/return/business_date=2026-11-14/return_*.csv
gs://YOUR_BUCKET/landing/recommendation/business_date=2026-11-14/recommendation_*.csv
```

## 6. Scheduled Runs

The DAG runs at 08:00 and 13:00 Asia/Kolkata.

The default business date comes from the Airflow logical date and the default batch ID comes from the Airflow run timestamp.

## 7. Backfill

For an explicit replay, trigger the DAG with:

```json
{
  "business_date": "2026-11-10",
  "batch_id": "20261110_backfill_01"
}
```

Before reprocessing a batch, Composer removes prior stage/quarantine/audit rows for the same batch ID. Dataproc also clears prior work/reject rows for that batch. Curated dimensions/facts use SCD2/MERGE logic, so the workflow is designed for replay-safe processing.

## 8. Failure Behaviour

If a sensor times out, Dataflow fails, stage validation returns false, Dataproc fails, reconciliation reports a mismatch, or dbt tests fail, downstream archive tasks do not execute.

This leaves source files in the landing path for investigation and rerun.

## 9. Successful Run

Files move from `landing/` to `archive/` only after all transformation, reconciliation and dbt tasks succeed.

The next phase adds the independent Pub/Sub -> Dataflow streaming path for real-time recommendation events.
