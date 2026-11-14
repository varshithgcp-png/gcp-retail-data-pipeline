-- Fit Analytics curated and transformation-work tables.
-- Replace PROJECT_ID before running.

CREATE TABLE IF NOT EXISTS `PROJECT_ID.fitanalytics_curated.dim_customer` (
  customer_sk STRING NOT NULL,
  customer_id STRING NOT NULL,
  region STRING,
  height_cm INT64,
  weight_kg NUMERIC,
  preferred_fit STRING,
  effective_start_ts TIMESTAMP NOT NULL,
  effective_end_ts TIMESTAMP NOT NULL,
  is_current BOOL NOT NULL,
  record_hash STRING NOT NULL,
  batch_id STRING,
  created_at TIMESTAMP
)
CLUSTER BY customer_id, is_current;

CREATE TABLE IF NOT EXISTS `PROJECT_ID.fitanalytics_curated.dim_product` (
  product_sk STRING NOT NULL,
  product_id STRING NOT NULL,
  sku STRING,
  brand STRING,
  category STRING,
  size_system STRING,
  size_label STRING,
  fit_profile STRING,
  unit_price NUMERIC,
  effective_start_ts TIMESTAMP NOT NULL,
  effective_end_ts TIMESTAMP NOT NULL,
  is_current BOOL NOT NULL,
  record_hash STRING NOT NULL,
  batch_id STRING,
  created_at TIMESTAMP
)
CLUSTER BY product_id, brand, category;

CREATE TABLE IF NOT EXISTS `PROJECT_ID.fitanalytics_curated.fact_purchase` (
  purchase_id STRING NOT NULL,
  customer_sk STRING,
  product_sk STRING,
  customer_id STRING,
  product_id STRING,
  recommended_size STRING,
  purchased_size STRING,
  quantity INT64,
  amount NUMERIC,
  purchase_date DATE,
  record_hash STRING,
  batch_id STRING,
  created_at TIMESTAMP
)
PARTITION BY purchase_date
CLUSTER BY customer_id, product_id;

CREATE TABLE IF NOT EXISTS `PROJECT_ID.fitanalytics_curated.fact_return` (
  return_id STRING NOT NULL,
  purchase_id STRING,
  customer_sk STRING,
  product_sk STRING,
  customer_id STRING,
  product_id STRING,
  return_reason STRING,
  fit_feedback STRING,
  return_amount NUMERIC,
  return_date DATE,
  record_hash STRING,
  batch_id STRING,
  created_at TIMESTAMP
)
PARTITION BY return_date
CLUSTER BY customer_id, product_id;

CREATE TABLE IF NOT EXISTS `PROJECT_ID.fitanalytics_curated.fact_recommendation` (
  recommendation_id STRING NOT NULL,
  customer_sk STRING,
  product_sk STRING,
  customer_id STRING,
  product_id STRING,
  recommended_size STRING,
  confidence_score FLOAT64,
  recommendation_ts TIMESTAMP,
  event_id STRING,
  record_hash STRING,
  batch_id STRING,
  created_at TIMESTAMP
)
PARTITION BY DATE(recommendation_ts)
CLUSTER BY customer_id, product_id;

CREATE TABLE IF NOT EXISTS `PROJECT_ID.fitanalytics_history.wrk_customer` (
  customer_id STRING, region STRING, height_cm INT64, weight_kg NUMERIC,
  preferred_fit STRING, updated_at TIMESTAMP, record_hash STRING,
  batch_id STRING, source_file STRING, ingestion_ts TIMESTAMP
)
CLUSTER BY customer_id, batch_id;

CREATE TABLE IF NOT EXISTS `PROJECT_ID.fitanalytics_history.wrk_product` (
  product_id STRING, sku STRING, brand STRING, category STRING,
  size_system STRING, size_label STRING, fit_profile STRING,
  unit_price NUMERIC, updated_at TIMESTAMP, record_hash STRING,
  batch_id STRING, source_file STRING, ingestion_ts TIMESTAMP
)
CLUSTER BY product_id, batch_id;

CREATE TABLE IF NOT EXISTS `PROJECT_ID.fitanalytics_history.wrk_purchase` (
  purchase_id STRING, customer_id STRING, product_id STRING,
  recommended_size STRING, purchased_size STRING, quantity INT64,
  amount NUMERIC, purchase_date DATE, updated_at TIMESTAMP,
  record_hash STRING, batch_id STRING, source_file STRING, ingestion_ts TIMESTAMP
)
PARTITION BY purchase_date
CLUSTER BY customer_id, product_id, batch_id;

CREATE TABLE IF NOT EXISTS `PROJECT_ID.fitanalytics_history.wrk_return` (
  return_id STRING, purchase_id STRING, customer_id STRING, product_id STRING,
  return_reason STRING, fit_feedback STRING, return_amount NUMERIC,
  return_date DATE, record_hash STRING, batch_id STRING,
  source_file STRING, ingestion_ts TIMESTAMP
)
PARTITION BY return_date
CLUSTER BY customer_id, product_id, batch_id;

CREATE TABLE IF NOT EXISTS `PROJECT_ID.fitanalytics_history.wrk_recommendation` (
  recommendation_id STRING, customer_id STRING, product_id STRING,
  recommended_size STRING, confidence_score FLOAT64,
  recommendation_ts TIMESTAMP, event_id STRING, record_hash STRING,
  batch_id STRING, source_file STRING, ingestion_ts TIMESTAMP
)
PARTITION BY DATE(recommendation_ts)
CLUSTER BY customer_id, product_id, batch_id;
