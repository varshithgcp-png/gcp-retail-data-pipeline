"""Parameterized BigQuery SQL for Fit Analytics Phase 3."""

CUSTOMER_SCD2_SQL = """
BEGIN TRANSACTION;

DELETE FROM `{project}.fitanalytics_curated.dim_customer`
WHERE customer_id IN (
  SELECT DISTINCT customer_id
  FROM `{project}.fitanalytics_history.wrk_customer`
  WHERE batch_id = @batch_id
);

INSERT INTO `{project}.fitanalytics_curated.dim_customer`
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
  FROM `{project}.fitanalytics_history.wrk_customer`
  WHERE batch_id = @batch_id
),
timestamp_deduped AS (
  SELECT * EXCEPT(_rn)
  FROM (
    SELECT
      w.*,
      ROW_NUMBER() OVER (
        PARTITION BY customer_id, updated_at
        ORDER BY ingestion_ts DESC, batch_id DESC
      ) AS _rn
    FROM `{project}.fitanalytics_history.wrk_customer` w
    JOIN affected a USING (customer_id)
  )
  WHERE _rn = 1
),
ordered_history AS (
  SELECT
    w.*,
    LAG(record_hash) OVER (
      PARTITION BY customer_id
      ORDER BY updated_at, ingestion_ts, batch_id
    ) AS previous_hash
  FROM timestamp_deduped w
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
  ingestion_ts
FROM versioned;

COMMIT TRANSACTION;
"""

PRODUCT_SCD2_SQL = """
BEGIN TRANSACTION;

DELETE FROM `{project}.fitanalytics_curated.dim_product`
WHERE product_id IN (
  SELECT DISTINCT product_id
  FROM `{project}.fitanalytics_history.wrk_product`
  WHERE batch_id = @batch_id
);

INSERT INTO `{project}.fitanalytics_curated.dim_product`
(
  product_sk,
  product_id,
  sku,
  brand,
  category,
  size_system,
  size_label,
  fit_profile,
  unit_price,
  effective_start_ts,
  effective_end_ts,
  is_current,
  record_hash,
  batch_id,
  created_at
)
WITH affected AS (
  SELECT DISTINCT product_id
  FROM `{project}.fitanalytics_history.wrk_product`
  WHERE batch_id = @batch_id
),
timestamp_deduped AS (
  SELECT * EXCEPT(_rn)
  FROM (
    SELECT
      w.*,
      ROW_NUMBER() OVER (
        PARTITION BY product_id, updated_at
        ORDER BY ingestion_ts DESC, batch_id DESC
      ) AS _rn
    FROM `{project}.fitanalytics_history.wrk_product` w
    JOIN affected a USING (product_id)
  )
  WHERE _rn = 1
),
ordered_history AS (
  SELECT
    w.*,
    LAG(record_hash) OVER (
      PARTITION BY product_id
      ORDER BY updated_at, ingestion_ts, batch_id
    ) AS previous_hash
  FROM timestamp_deduped w
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
      PARTITION BY product_id
      ORDER BY updated_at, ingestion_ts, batch_id
    ) AS next_updated_at
  FROM changes
)
SELECT
  TO_HEX(SHA256(CONCAT(product_id, '|', CAST(updated_at AS STRING)))),
  product_id,
  sku,
  brand,
  category,
  size_system,
  size_label,
  fit_profile,
  unit_price,
  updated_at,
  COALESCE(
    TIMESTAMP_SUB(next_updated_at, INTERVAL 1 MICROSECOND),
    TIMESTAMP('9999-12-31 23:59:59+00')
  ),
  next_updated_at IS NULL,
  record_hash,
  batch_id,
  ingestion_ts
FROM versioned;

COMMIT TRANSACTION;
"""

PURCHASE_MERGE_SQL = """
MERGE `{project}.fitanalytics_curated.fact_purchase` t
USING (
  SELECT w.purchase_id, c.customer_sk, p.product_sk,
         w.customer_id, w.product_id, w.recommended_size, w.purchased_size,
         w.quantity, w.amount, w.purchase_date, w.record_hash, w.batch_id
  FROM `{project}.fitanalytics_history.wrk_purchase` w
  JOIN `{project}.fitanalytics_curated.dim_customer` c
    ON c.customer_id = w.customer_id
   AND DATE(c.effective_start_ts) <= w.purchase_date
  JOIN `{project}.fitanalytics_curated.dim_product` p
    ON p.product_id = w.product_id
   AND DATE(p.effective_start_ts) <= w.purchase_date
  WHERE w.batch_id = @batch_id
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY w.purchase_id
    ORDER BY c.effective_start_ts DESC, p.effective_start_ts DESC
  ) = 1
) s
ON t.purchase_id = s.purchase_id
WHEN MATCHED AND (
  t.record_hash IS DISTINCT FROM s.record_hash
  OR t.customer_sk IS DISTINCT FROM s.customer_sk
  OR t.product_sk IS DISTINCT FROM s.product_sk
) THEN UPDATE SET
  customer_sk=s.customer_sk, product_sk=s.product_sk,
  customer_id=s.customer_id, product_id=s.product_id,
  recommended_size=s.recommended_size, purchased_size=s.purchased_size,
  quantity=s.quantity, amount=s.amount, purchase_date=s.purchase_date,
  record_hash=s.record_hash, batch_id=s.batch_id, created_at=CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT
(purchase_id, customer_sk, product_sk, customer_id, product_id,
 recommended_size, purchased_size, quantity, amount, purchase_date,
 record_hash, batch_id, created_at)
VALUES
(s.purchase_id, s.customer_sk, s.product_sk, s.customer_id, s.product_id,
 s.recommended_size, s.purchased_size, s.quantity, s.amount, s.purchase_date,
 s.record_hash, s.batch_id, CURRENT_TIMESTAMP());
"""

RETURN_MERGE_SQL = """
MERGE `{project}.fitanalytics_curated.fact_return` t
USING (
  SELECT
    w.return_id,
    w.purchase_id,
    fp.customer_sk,
    fp.product_sk,
    w.customer_id,
    w.product_id,
    w.return_reason,
    w.fit_feedback,
    w.return_amount,
    w.return_date,
    w.record_hash,
    w.batch_id
  FROM `{project}.fitanalytics_history.wrk_return` w
  JOIN `{project}.fitanalytics_curated.fact_purchase` fp
    ON fp.purchase_id = w.purchase_id
  WHERE w.batch_id = @batch_id
) s
ON t.return_id = s.return_id
WHEN MATCHED AND (
  t.record_hash IS DISTINCT FROM s.record_hash
  OR t.customer_sk IS DISTINCT FROM s.customer_sk
  OR t.product_sk IS DISTINCT FROM s.product_sk
) THEN UPDATE SET
  purchase_id=s.purchase_id, customer_sk=s.customer_sk, product_sk=s.product_sk,
  customer_id=s.customer_id, product_id=s.product_id,
  return_reason=s.return_reason, fit_feedback=s.fit_feedback,
  return_amount=s.return_amount, return_date=s.return_date,
  record_hash=s.record_hash, batch_id=s.batch_id, created_at=CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT
(return_id, purchase_id, customer_sk, product_sk, customer_id, product_id,
 return_reason, fit_feedback, return_amount, return_date,
 record_hash, batch_id, created_at)
VALUES
(s.return_id, s.purchase_id, s.customer_sk, s.product_sk, s.customer_id, s.product_id,
 s.return_reason, s.fit_feedback, s.return_amount, s.return_date,
 s.record_hash, s.batch_id, CURRENT_TIMESTAMP());
"""

RECOMMENDATION_MERGE_SQL = """
MERGE `{project}.fitanalytics_curated.fact_recommendation` t
USING (
  SELECT w.recommendation_id, c.customer_sk, p.product_sk,
         w.customer_id, w.product_id, w.recommended_size,
         w.confidence_score, w.recommendation_ts, w.event_id,
         w.record_hash, w.batch_id
  FROM `{project}.fitanalytics_history.wrk_recommendation` w
  JOIN `{project}.fitanalytics_curated.dim_customer` c
    ON c.customer_id = w.customer_id
   AND w.recommendation_ts BETWEEN c.effective_start_ts AND c.effective_end_ts
  JOIN `{project}.fitanalytics_curated.dim_product` p
    ON p.product_id = w.product_id
   AND w.recommendation_ts BETWEEN p.effective_start_ts AND p.effective_end_ts
  WHERE w.batch_id = @batch_id
) s
ON t.recommendation_id = s.recommendation_id
WHEN MATCHED AND (
  t.record_hash IS DISTINCT FROM s.record_hash
  OR t.customer_sk IS DISTINCT FROM s.customer_sk
  OR t.product_sk IS DISTINCT FROM s.product_sk
) THEN UPDATE SET
  customer_sk=s.customer_sk, product_sk=s.product_sk,
  customer_id=s.customer_id, product_id=s.product_id,
  recommended_size=s.recommended_size, confidence_score=s.confidence_score,
  recommendation_ts=s.recommendation_ts, event_id=s.event_id,
  record_hash=s.record_hash, batch_id=s.batch_id, created_at=CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT
(recommendation_id, customer_sk, product_sk, customer_id, product_id,
 recommended_size, confidence_score, recommendation_ts, event_id,
 record_hash, batch_id, created_at)
VALUES
(s.recommendation_id, s.customer_sk, s.product_sk, s.customer_id, s.product_id,
 s.recommended_size, s.confidence_score, s.recommendation_ts, s.event_id,
 s.record_hash, s.batch_id, CURRENT_TIMESTAMP());
"""
