"""BigQuery audit helpers for Fit Analytics transformations."""

from google.cloud import bigquery


def record_fact_reconciliation(
    client,
    project_id,
    entity_type,
    batch_id,
    stage_table,
    work_table,
    target_table,
):
    sql = f"""
    INSERT INTO `{project_id}.fitanalytics_audit.reconciliation_audit`
    (
      pipeline_name, entity_type, batch_id,
      stage_count, valid_count, rejected_count, target_count,
      reconciliation_status, audit_ts
    )
    WITH counts AS (
      SELECT
        (SELECT COUNT(*) FROM `{stage_table}` WHERE batch_id = @batch_id) AS stage_count,
        (SELECT COUNT(*) FROM `{work_table}` WHERE batch_id = @batch_id) AS valid_count,
        (
          SELECT COUNT(*)
          FROM `{project_id}.fitanalytics_audit.transformation_rejects`
          WHERE batch_id = @batch_id AND entity_type = @entity_type
        ) AS rejected_count,
        (SELECT COUNT(*) FROM `{target_table}` WHERE batch_id = @batch_id) AS target_count
    )
    SELECT
      'fitanalytics_dataproc_transform',
      @entity_type,
      @batch_id,
      stage_count,
      valid_count,
      rejected_count,
      target_count,
      CASE
        WHEN stage_count = valid_count + rejected_count
         AND valid_count = target_count
        THEN 'MATCHED'
        ELSE 'MISMATCH'
      END,
      CURRENT_TIMESTAMP()
    FROM counts
    """

    config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "entity_type", "STRING", entity_type
            ),
            bigquery.ScalarQueryParameter(
                "batch_id", "STRING", batch_id
            ),
        ]
    )
    client.query(sql, job_config=config).result()


def update_watermark(
    client,
    project_id,
    entity_type,
    batch_id,
    source_table,
    timestamp_column,
):
    sql = f"""
    MERGE `{project_id}.fitanalytics_audit.watermark_control` t
    USING (
      SELECT
        'fitanalytics_dataproc_transform' AS pipeline_name,
        @entity_type AS entity_type,
        MAX({timestamp_column}) AS last_processed_ts,
        @batch_id AS last_batch_id
      FROM `{source_table}`
      WHERE batch_id = @batch_id
    ) s
    ON t.pipeline_name = s.pipeline_name
       AND t.entity_type = s.entity_type
    WHEN MATCHED AND s.last_processed_ts IS NOT NULL THEN
      UPDATE SET
        last_processed_ts = GREATEST(
          COALESCE(t.last_processed_ts, TIMESTAMP('1970-01-01')),
          s.last_processed_ts
        ),
        last_batch_id = s.last_batch_id,
        updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED AND s.last_processed_ts IS NOT NULL THEN
      INSERT (
        pipeline_name, entity_type, last_processed_ts,
        last_batch_id, updated_at
      )
      VALUES (
        s.pipeline_name, s.entity_type, s.last_processed_ts,
        s.last_batch_id, CURRENT_TIMESTAMP()
      )
    """

    config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "entity_type", "STRING", entity_type
            ),
            bigquery.ScalarQueryParameter(
                "batch_id", "STRING", batch_id
            ),
        ]
    )
    client.query(sql, job_config=config).result()
