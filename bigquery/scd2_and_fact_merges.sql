-- Reference SQL for Fit Analytics SCD2 and fact merges.
-- Replace PROJECT_ID and bind @batch_id.

BEGIN TRANSACTION;

UPDATE `PROJECT_ID.fitanalytics_curated.dim_customer` d
SET
  effective_end_ts = TIMESTAMP_SUB(w.updated_at, INTERVAL 1 MICROSECOND),
  is_current = FALSE
FROM `PROJECT_ID.fitanalytics_history.wrk_customer` w
WHERE w.batch_id = @batch_id
  AND d.customer_id = w.customer_id
  AND d.is_current = TRUE
  AND d.record_hash != w.record_hash;

INSERT INTO `PROJECT_ID.fitanalytics_curated.dim_customer`
(
  customer_sk, customer_id, region, height_cm, weight_kg, preferred_fit,
  effective_start_ts, effective_end_ts, is_current,
  record_hash, batch_id, created_at
)
SELECT
  TO_HEX(SHA256(CONCAT(w.customer_id, '|', CAST(w.updated_at AS STRING)))),
  w.customer_id, w.region, w.height_cm, w.weight_kg, w.preferred_fit,
  w.updated_at,
  TIMESTAMP('9999-12-31 23:59:59+00'),
  TRUE,
  w.record_hash,
  w.batch_id,
  CURRENT_TIMESTAMP()
FROM `PROJECT_ID.fitanalytics_history.wrk_customer` w
LEFT JOIN `PROJECT_ID.fitanalytics_curated.dim_customer` d
  ON d.customer_id = w.customer_id
 AND d.is_current = TRUE
WHERE w.batch_id = @batch_id
  AND (d.customer_id IS NULL OR d.record_hash != w.record_hash);

COMMIT TRANSACTION;

-- Product SCD2 follows the same expire-and-insert pattern.
-- Facts are loaded with MERGE using their natural transaction key:
-- purchase_id, return_id and recommendation_id.
-- This makes reruns idempotent.
