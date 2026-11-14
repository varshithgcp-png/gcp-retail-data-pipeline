-- Reference SQL for Fit Analytics SCD2 and fact merges.
-- Replace PROJECT_ID and bind @batch_id.
--
-- The executable parameterized SQL lives in spark_jobs/bigquery_merges.py.
-- SCD2 is rebuilt only for business keys touched by the current batch,
-- using retained work history ordered by source updated_at. This supports
-- controlled out-of-order/backfill dimension arrivals better than simply
-- expiring the current row.

BEGIN TRANSACTION;

DELETE FROM `PROJECT_ID.fitanalytics_curated.dim_customer`
WHERE customer_id IN (
  SELECT DISTINCT customer_id
  FROM `PROJECT_ID.fitanalytics_history.wrk_customer`
  WHERE batch_id = @batch_id
);

INSERT INTO `PROJECT_ID.fitanalytics_curated.dim_customer`
(
  customer_sk,
  customer_id,
  region,
  height_cm,
  weight_kg,
  preferred_fit,
  effective_start_ts,
  effective_end_ts,
  is_current,
  record_hash,
  batch_id,
  created_at
)
WITH affected AS (
  SELECT DISTINCT customer_id
  FROM `PROJECT_ID.fitanalytics_history.wrk_customer`
  WHERE batch_id = @batch_id
),
ordered_history AS (
  SELECT
    w.*,
    LAG(record_hash) OVER (
      PARTITION BY customer_id
      ORDER BY updated_at, ingestion_ts, batch_id
    ) AS previous_hash
  FROM `PROJECT_ID.fitanalytics_history.wrk_customer` w
  JOIN affected a USING (customer_id)
),
changes AS (
  SELECT *
  FROM ordered_history
  WHERE previous_hash IS NULL OR previous_hash != record_hash
),
versioned AS (
  SELECT
    *,
    LEAD(updated_at) OVER (
      PARTITION BY customer_id
      ORDER BY updated_at, ingestion_ts, batch_id
    ) AS next_updated_at
  FROM changes
)
SELECT
  TO_HEX(SHA256(CONCAT(customer_id, '|', CAST(updated_at AS STRING)))),
  customer_id,
  region,
  height_cm,
  weight_kg,
  preferred_fit,
  updated_at,
  COALESCE(
    TIMESTAMP_SUB(next_updated_at, INTERVAL 1 MICROSECOND),
    TIMESTAMP('9999-12-31 23:59:59+00')
  ),
  next_updated_at IS NULL,
  record_hash,
  batch_id,
  CURRENT_TIMESTAMP()
FROM versioned;

COMMIT TRANSACTION;

-- Product SCD2 uses the same affected-key history rebuild pattern.
-- Facts use MERGE on purchase_id / return_id / recommendation_id.
-- If a historical dimension backfill changes version boundaries, rerun
-- impacted fact business dates so surrogate keys are refreshed.
