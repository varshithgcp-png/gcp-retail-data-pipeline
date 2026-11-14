-- Replace PROJECT_ID with your Google Cloud project ID.
CREATE SCHEMA IF NOT EXISTS `PROJECT_ID.retail_stage`;
CREATE SCHEMA IF NOT EXISTS `PROJECT_ID.retail_curated`;
CREATE SCHEMA IF NOT EXISTS `PROJECT_ID.retail_audit`;

CREATE TABLE IF NOT EXISTS `PROJECT_ID.retail_stage.stg_customers` (
  customer_id INT64, name STRING, email STRING, city STRING, state STRING,
  updated_at TIMESTAMP, ingestion_ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `PROJECT_ID.retail_stage.stg_products` (
  product_id INT64, product_name STRING, category STRING, brand STRING,
  unit_price NUMERIC, updated_at TIMESTAMP, ingestion_ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `PROJECT_ID.retail_stage.stg_purchases` (
  purchase_id INT64, customer_id INT64, product_id INT64, quantity INT64,
  amount NUMERIC, purchase_date DATE, updated_at TIMESTAMP, ingestion_ts TIMESTAMP
)
PARTITION BY purchase_date
CLUSTER BY customer_id, product_id;

CREATE TABLE IF NOT EXISTS `PROJECT_ID.retail_stage.stg_returns` (
  return_id INT64, purchase_id INT64, product_id INT64, return_reason STRING,
  return_amount NUMERIC, return_date DATE, ingestion_ts TIMESTAMP
)
PARTITION BY return_date
CLUSTER BY product_id;

CREATE TABLE IF NOT EXISTS `PROJECT_ID.retail_audit.pipeline_audit` (
  pipeline_name STRING, batch_id STRING, source_count INT64, valid_count INT64,
  rejected_count INT64, start_time TIMESTAMP, end_time TIMESTAMP,
  status STRING, error_message STRING
)
PARTITION BY DATE(start_time);
