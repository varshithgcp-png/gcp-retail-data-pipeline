# Phase 3 - Dataproc / PySpark Transformations

## Flow

```text
fitanalytics_stage
      |
      v
PySpark on Dataproc
      |
      +--> deduplication
      +--> referential-integrity checks
      +--> SHA-256 change detection
      |
      v
fitanalytics_history work tables
      |
      v
BigQuery SCD2 + MERGE
      |
      v
fitanalytics_curated
```

## Processing Order

1. Read customer/product stage rows for one batch.
2. Keep the latest row per business key.
3. Generate SHA-256 hashes from business attributes.
4. Write dimension work records to `fitanalytics_history`.
5. Run atomic SCD Type 2 logic for customer and product.
6. Validate purchase/return/recommendation foreign keys.
7. Send invalid references to `fitanalytics_audit.transformation_rejects`.
8. MERGE valid facts into curated tables.
9. Use batch IDs and natural keys to make reruns idempotent.

## SCD Type 2

Dimensions contain:

- `effective_start_ts`
- `effective_end_ts`
- `is_current`
- `record_hash`
- deterministic surrogate key

PySpark writes deduplicated, hashed dimension work rows. BigQuery then rebuilds only the affected customer/product histories from retained work records ordered by source `updated_at`. Consecutive identical hashes are collapsed, version end timestamps are derived with `LEAD`, and exactly one latest version is marked current.

## Expected Synthetic DQ Scenario

Purchase `PU1005` references customer `C999` and should be rejected. Return `R2004` references purchase `PU9999` and should also be rejected. See `docs/expected_results.md` for expected counts.

## Setup

Run:

```text
bigquery/create_tables.sql
bigquery/create_curated_tables.sql
bigquery/create_control_tables.sql
```

## Submit

Package `spark_jobs` as a zip or make it available to the cluster, then submit:

```bash
gcloud dataproc jobs submit pyspark spark_jobs/run_transformations.py \
  --cluster=YOUR_CLUSTER \
  --region=us-central1 \
  --py-files=YOUR_SUPPORTING_PYTHON_ZIP \
  -- \
  --project_id=YOUR_PROJECT_ID \
  --temporary_gcs_bucket=YOUR_TEMP_BUCKET \
  --batch_id=20261114_080000
```

The Spark BigQuery connector must be available to the Dataproc runtime for DataFrame reads/writes against BigQuery.

## Why PySpark + BigQuery MERGE

PySpark performs scalable deduplication, validation and hash calculation. BigQuery executes the affected-key SCD2 rebuild transaction and fact MERGE close to the warehouse. Deterministic surrogate keys keep existing versions stable. If an older dimension backfill changes historical version boundaries, rerun the impacted fact business dates to refresh surrogate-key assignments.
