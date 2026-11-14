-- Fit Analytics real-time recommendation streaming tables.
-- Replace PROJECT_ID before running.

CREATE SCHEMA IF NOT EXISTS `PROJECT_ID.fitanalytics_streaming`;

CREATE TABLE IF NOT EXISTS
`PROJECT_ID.fitanalytics_streaming.recommendation_events` (
  event_id STRING NOT NULL,
  recommendation_id STRING NOT NULL,
  customer_id STRING NOT NULL,
  product_id STRING NOT NULL,
  recommended_size STRING NOT NULL,
  confidence_score FLOAT64 NOT NULL,
  event_ts TIMESTAMP NOT NULL,
  pubsub_message_id STRING,
  publish_time TIMESTAMP,
  ingestion_ts TIMESTAMP NOT NULL,
  event_lag_seconds FLOAT64,
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  source STRING
)
PARTITION BY DATE(event_ts)
CLUSTER BY customer_id, product_id, event_id;

CREATE TABLE IF NOT EXISTS
`PROJECT_ID.fitanalytics_streaming.recommendation_window_metrics` (
  window_start TIMESTAMP NOT NULL,
  window_end TIMESTAMP NOT NULL,
  event_count INT64 NOT NULL,
  pane_index INT64,
  pane_timing STRING,
  emitted_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(window_start)
CLUSTER BY pane_timing;

CREATE TABLE IF NOT EXISTS
`PROJECT_ID.fitanalytics_audit.streaming_dlq` (
  event_id STRING,
  raw_payload STRING,
  attributes_json STRING,
  error_message STRING,
  pubsub_message_id STRING,
  publish_time TIMESTAMP,
  ingestion_ts TIMESTAMP NOT NULL
)
PARTITION BY DATE(ingestion_ts)
CLUSTER BY event_id;

CREATE OR REPLACE VIEW
`PROJECT_ID.fitanalytics_streaming.vw_recommendation_events_deduped` AS
WITH event_deduped AS (
  SELECT *
  FROM `PROJECT_ID.fitanalytics_streaming.recommendation_events`
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY event_id
    ORDER BY ingestion_ts DESC, pubsub_message_id DESC
  ) = 1
)
SELECT *
FROM event_deduped
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY recommendation_id
  ORDER BY ingestion_ts DESC, event_id DESC
) = 1;
