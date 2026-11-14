-- Fit Analytics control, reconciliation and transformation-reject tables.
-- Replace PROJECT_ID before running.

CREATE TABLE IF NOT EXISTS `PROJECT_ID.fitanalytics_audit.watermark_control` (
  pipeline_name STRING NOT NULL,
  entity_type STRING NOT NULL,
  last_processed_ts TIMESTAMP,
  last_batch_id STRING,
  updated_at TIMESTAMP
)
CLUSTER BY pipeline_name, entity_type;

CREATE TABLE IF NOT EXISTS `PROJECT_ID.fitanalytics_audit.reconciliation_audit` (
  pipeline_name STRING,
  entity_type STRING,
  batch_id STRING,
  stage_count INT64,
  valid_count INT64,
  rejected_count INT64,
  duplicate_count INT64,
  target_count INT64,
  reconciliation_status STRING,
  audit_ts TIMESTAMP
)
PARTITION BY DATE(audit_ts)
CLUSTER BY entity_type, batch_id;

CREATE TABLE IF NOT EXISTS `PROJECT_ID.fitanalytics_audit.transformation_rejects` (
  entity_type STRING,
  raw_record STRING,
  error_message STRING,
  source_file STRING,
  batch_id STRING,
  ingestion_ts TIMESTAMP
)
PARTITION BY DATE(ingestion_ts)
CLUSTER BY entity_type, batch_id;
