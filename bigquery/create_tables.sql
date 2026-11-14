-- Fit Analytics portfolio schemas.
-- Replace PROJECT_ID with your Google Cloud project ID.

CREATE SCHEMA IF NOT EXISTS `PROJECT_ID.fitanalytics_stage`;
CREATE SCHEMA IF NOT EXISTS `PROJECT_ID.fitanalytics_history`;
CREATE SCHEMA IF NOT EXISTS `PROJECT_ID.fitanalytics_curated`;
CREATE SCHEMA IF NOT EXISTS `PROJECT_ID.fitanalytics_audit`;
CREATE SCHEMA IF NOT EXISTS `PROJECT_ID.fitanalytics_reporting`;

CREATE TABLE IF NOT EXISTS `PROJECT_ID.fitanalytics_stage.stg_customer` (
  customer_id STRING,
  region STRING,
  height_cm INT64,
  weight_kg NUMERIC,
  preferred_fit STRING,
  updated_at TIMESTAMP,
  ingestion_ts TIMESTAMP,
  batch_id STRING,
  source_file STRING
);

CREATE TABLE IF NOT EXISTS `PROJECT_ID.fitanalytics_stage.stg_product` (
  product_id STRING,
  sku STRING,
  brand STRING,
  category STRING,
  size_system STRING,
  size_label STRING,
  fit_profile STRING,
  unit_price NUMERIC,
  updated_at TIMESTAMP,
  ingestion_ts TIMESTAMP,
  batch_id STRING,
  source_file STRING
);

CREATE TABLE IF NOT EXISTS `PROJECT_ID.fitanalytics_stage.stg_purchase` (
  purchase_id STRING,
  customer_id STRING,
  product_id STRING,
  recommended_size STRING,
  purchased_size STRING,
  quantity INT64,
  amount NUMERIC,
  purchase_date DATE,
  updated_at TIMESTAMP,
  ingestion_ts TIMESTAMP,
  batch_id STRING,
  source_file STRING
)
PARTITION BY purchase_date
CLUSTER BY customer_id, product_id;

CREATE TABLE IF NOT EXISTS `PROJECT_ID.fitanalytics_stage.stg_return` (
  return_id STRING,
  purchase_id STRING,
  customer_id STRING,
  product_id STRING,
  return_reason STRING,
  fit_feedback STRING,
  return_amount NUMERIC,
  return_date DATE,
  ingestion_ts TIMESTAMP,
  batch_id STRING,
  source_file STRING
)
PARTITION BY return_date
CLUSTER BY customer_id, product_id;

CREATE TABLE IF NOT EXISTS `PROJECT_ID.fitanalytics_stage.stg_recommendation` (
  recommendation_id STRING,
  customer_id STRING,
  product_id STRING,
  recommended_size STRING,
  confidence_score FLOAT64,
  recommendation_ts TIMESTAMP,
  event_id STRING,
  ingestion_ts TIMESTAMP,
  batch_id STRING,
  source_file STRING
)
PARTITION BY DATE(recommendation_ts)
CLUSTER BY customer_id, product_id;

CREATE TABLE IF NOT EXISTS `PROJECT_ID.fitanalytics_audit.pipeline_audit` (
  pipeline_name STRING,
  batch_id STRING,
  source_count INT64,
  stage_count INT64,
  target_count INT64,
  rejected_count INT64,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  status STRING,
  error_message STRING
)
PARTITION BY DATE(start_time);

CREATE TABLE IF NOT EXISTS `PROJECT_ID.fitanalytics_audit.quarantine_records` (
  entity_type STRING,
  raw_record STRING,
  error_message STRING,
  source_file STRING,
  batch_id STRING,
  ingestion_ts TIMESTAMP
)
PARTITION BY DATE(ingestion_ts)
CLUSTER BY entity_type;
