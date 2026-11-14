-- Fit Analytics operational monitoring queries.
-- Adjust region if your BigQuery jobs run outside US multi-region.

-- 1. Recent expensive / high-volume BigQuery jobs.
SELECT
  creation_time,
  user_email,
  job_id,
  statement_type,
  total_bytes_processed,
  total_slot_ms,
  error_result
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
  AND job_type = 'QUERY'
ORDER BY total_bytes_processed DESC
LIMIT 50;

-- 2. Recent reconciliation failures.
SELECT
  entity_type,
  batch_id,
  stage_count,
  valid_count,
  rejected_count,
  duplicate_count,
  target_count,
  reconciliation_status,
  audit_ts
FROM `PROJECT_ID.fitanalytics_audit.reconciliation_audit`
WHERE audit_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  AND reconciliation_status != 'MATCHED'
ORDER BY audit_ts DESC;

-- 3. Streaming DLQ trend.
SELECT
  TIMESTAMP_TRUNC(ingestion_ts, HOUR) AS event_hour,
  COUNT(*) AS dlq_count
FROM `PROJECT_ID.fitanalytics_audit.streaming_dlq`
WHERE ingestion_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
GROUP BY event_hour
ORDER BY event_hour DESC;

-- 4. Streaming ingestion lag.
SELECT
  TIMESTAMP_TRUNC(event_ts, HOUR) AS event_hour,
  COUNT(*) AS event_count,
  AVG(event_lag_seconds) AS avg_lag_seconds,
  APPROX_QUANTILES(event_lag_seconds, 100)[OFFSET(95)] AS p95_lag_seconds
FROM `PROJECT_ID.fitanalytics_streaming.vw_recommendation_events_deduped`
WHERE event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
GROUP BY event_hour
ORDER BY event_hour DESC;

-- 5. Latest batch audit status.
SELECT
  pipeline_name,
  batch_id,
  source_count,
  stage_count,
  rejected_count,
  status,
  start_time,
  end_time
FROM `PROJECT_ID.fitanalytics_audit.pipeline_audit`
WHERE start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)
ORDER BY start_time DESC;
